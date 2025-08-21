import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote, unquote
import re
import os
import traceback
import numpy as np
import time
from datetime import datetime
import argparse
import sys
import logging
import random

# Configure constants
DELAY_SECONDS = 120  # Increased delay to avoid blocking
LOG_FILE = "comic_processor.log"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
]
MIN_COVER_LENGTH = 15  # Minimum length to consider a cover URL valid

# Column indices (0-based) - Adjusted based on your file structure
TITLE_COL = 6    # Column G (Title)
LINK_COL = 10    # Column K (URL)
COVER_COL = 24   # Column Y (Cover)

def setup_logging():
    """Configure logging to both file and console"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

def log_to_file(message):
    """Append a message to the log file"""
    with open(LOG_FILE, 'a') as f:
        f.write(message + "\n")

def is_valid_cover(cover):
    """Check if cover field contains a valid URL"""
    if pd.isna(cover):
        return False
    cover_str = str(cover).strip()
    return len(cover_str) >= MIN_COVER_LENGTH and cover_str.startswith('http')

def manual_search_fallback(comic_name):
    """Manual fallback for when automated search fails"""
    print(f"\n--- MANUAL SEARCH REQUIRED ---")
    print(f"Comic: {comic_name}")
    print("Please search manually on https://www.bedetheque.com/")
    print("Enter the URL below or press Enter to skip:")
    
    url = input("URL: ").strip()
    if url and url.startswith('http'):
        return url
    return None

def get_cover_url(serie_url, interactive_mode):
    """Extract cover URL from a serie page"""
    try:
        # Use a random user agent
        user_agent = random.choice(USER_AGENTS)
        
        response = requests.get(
            serie_url,
            headers={
                'User-Agent': user_agent,
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://www.google.com/'
            },
            timeout=30
        )
        
        # Check if we're blocked
        if response.status_code == 403 or "access denied" in response.text.lower():
            logging.error("IP appears to be blocked. Waiting longer before continuing.")
            time.sleep(300)  # Wait 5 minutes if blocked
            return None
            
        response.raise_for_status()
        
        # Delay after the request
        if not interactive_mode:
            logging.info(f"Waiting {DELAY_SECONDS} seconds after cover request...")
            time.sleep(DELAY_SECONDS)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try multiple methods to find the cover image
        # 1. Look for og:image meta tag
        meta_image = soup.find('meta', {'property': 'og:image'})
        if meta_image and meta_image.get('content'):
            return meta_image.get('content')
        
        # 2. Look for image with class 'cover'
        cover_img = soup.find('img', class_='cover')
        if cover_img and cover_img.get('src'):
            src = cover_img.get('src')
            if src.startswith('//'):
                return f"https:{src}"
            elif src.startswith('/'):
                return f"https://www.bedetheque.com{src}"
            else:
                return src
        
        return None
        
    except requests.RequestException as e:
        logging.error(f"Cover fetch error for '{serie_url}': {str(e)}")
        if not interactive_mode:
            time.sleep(DELAY_SECONDS)
        return None

def process_row(index, row, df, interactive_mode):
    """Process a single row of the dataframe"""
    # Safely get values with proper empty checks
    comic_name = str(row[TITLE_COL]) if not pd.isna(row[TITLE_COL]) else ""
    current_link = str(row[LINK_COL]) if not pd.isna(row[LINK_COL]) else ""
    current_cover = str(row[COVER_COL]) if not pd.isna(row[COVER_COL]) else ""
    
    # Clean strings
    comic_name = comic_name.strip()
    current_link = current_link.strip()
    current_cover = current_cover.strip()
    
    # Initialize variables
    terminal_status = ""
    file_status = ""
    search_url = ""
    cover_url = ""
    updated = False
    
    # Debug print to verify values
    print(f"\nDEBUG - Row {index}:")
    print(f"Title: '{comic_name}'")
    print(f"Link: '{current_link}'")
    print(f"Cover: '{current_cover}'")
    print(f"Is valid cover: {is_valid_cover(current_cover)}")
    
    # Case 1: Both link and valid cover exist - skip (no delay)
    if current_link and is_valid_cover(current_cover):
        terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: filled - Result: Skipping - Cover: exists"
        file_status = "Skipping (both exist)"
    
    # Case 2: Link exists but cover is invalid - fetch cover
    elif current_link and not is_valid_cover(current_cover):
        cover_url = get_cover_url(current_link, interactive_mode)
        if cover_url:
            df.at[index, COVER_COL] = cover_url
            updated = True
            terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: filled - Result: Found - Cover: found"
            file_status = "Found (cover)"
        else:
            terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: filled - Result: Found - Cover: not found"
            file_status = "Found (no cover)"
    
    # Case 3: Link is empty - use manual fallback
    elif not current_link:
        if interactive_mode:
            new_link = manual_search_fallback(comic_name)
            if new_link:
                df.at[index, LINK_COL] = new_link
                cover_url = get_cover_url(new_link, interactive_mode)
                if cover_url:
                    df.at[index, COVER_COL] = cover_url
                updated = True
                terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: empty - Result: Found (manual) - Cover: {'found' if cover_url else 'not found'}"
                file_status = "Found (manual)"
            else:
                terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: empty - Result: Skipped (manual) - Cover: n/a"
                file_status = "Skipped (manual)"
        else:
            terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: empty - Result: not Found - Cover: n/a"
            file_status = "not Found"
    
    # Log to terminal
    print(terminal_status)
    
    # Log to file
    log_entry = (
        f"{datetime.now().strftime('%Y%m%d')},"
        f"{datetime.now().strftime('%H%M%S')},"
        f"Row,{index},{comic_name},"
        f"{current_link if current_link else 'empty'},"
        f"{file_status},"
        f"{search_url if search_url else 'empty'},"
        f"{cover_url if cover_url else 'empty'}"
    )
    log_to_file(log_entry)
    
    # Interactive mode handling
    if interactive_mode and updated:
        while True:
            user_input = input("Press ENTER to continue or type 'go' for non-interactive mode: ").strip().lower()
            if user_input == 'go':
                return False
            elif user_input == '':
                return True
    
    return interactive_mode

def process_excel_file(input_file, output_file, interactive_mode):
    """Process the Excel file"""
    try:
        # Read the Excel file
        _, ext = os.path.splitext(input_file)
        engine = 'xlrd' if ext.lower() == '.xls' else 'openpyxl'
        df = pd.read_excel(input_file, sheet_name='bd', engine=engine, header=None)
        
        # Print column information for debugging
        print("\nFile structure:")
        print(f"Total columns: {len(df.columns)}")
        print("First row sample:", df.iloc[2].tolist())  # Print header row
        
        # Verify column structure
        if len(df.columns) <= COVER_COL:
            raise ValueError(f"Input file has only {len(df.columns)} columns, but we need at least {COVER_COL+1} columns")
        
        # Process each row starting from row 4 (index 3)
        for index, row in df.iterrows():
            # Skip first 3 header rows and empty title rows
            if index < 3 or pd.isna(row[TITLE_COL]):
                continue
                
            # Process the row
            interactive_mode = process_row(index, row, df, interactive_mode)
            
            # Save progress after each update
            df.to_excel(output_file, sheet_name='bd', index=False, header=False, engine='openpyxl')
        
        logging.info("Processing complete")
        
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description='Process comic book data from Excel file')
    parser.add_argument('input_file', help='Input Excel file path')
    parser.add_argument('output_file', help='Output Excel file path')
    parser.add_argument('-i', '--interactive', action='store_true', help='Enable interactive mode')
    args = parser.parse_args()
    
    setup_logging()
    logging.info(f"Starting processing with {'interactive' if args.interactive else 'non-interactive'} mode")
    
    # Warning about potential IP blocking
    if not args.interactive:
        print("\nWARNING: Automated requests may result in IP blocking.")
        print("Consider using interactive mode (-i) for manual URL entry.")
        print("Waiting 10 seconds before starting...")
        time.sleep(10)
    
    process_excel_file(args.input_file, args.output_file, args.interactive)

if __name__ == "__main__":
    main()