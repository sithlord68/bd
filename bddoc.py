import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import re
import os
import traceback
import numpy as np
import time
from datetime import datetime
import argparse
import sys
import logging

# Configure constants
DELAY_SECONDS = 60  # Fixed 60-second delay between requests
LOG_FILE = "comic_processor.log"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

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

def is_empty_cell(cell):
    """Check if a cell is truly empty"""
    if pd.isna(cell):
        return True
    if isinstance(cell, str) and cell.strip() in ('', 'nan', 'None'):
        return True
    return False

def search_bedetheque(comic_name, interactive_mode):
    """Search for a comic on bedetheque.com and return the exact match URL if found"""
    search_url = f"https://www.bedetheque.com/search/albums/?keywords={quote(comic_name)}"
    
    try:
        logging.info(f"Waiting {DELAY_SECONDS} seconds before search...")
        time.sleep(DELAY_SECONDS)
        
        response = requests.get(
            search_url,
            headers={
                'User-Agent': USER_AGENT,
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.google.com/'
            },
            timeout=30
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        results = soup.find_all('div', class_='liste-series')
        
        if not results:
            return None, search_url
        
        for result in results:
            for link in result.find_all('a'):
                link_text = link.text.strip()
                if link_text.lower() == comic_name.strip().lower():
                    href = link.get('href')
                    if href:
                        url = f"https://www.bedetheque.com{href}" if not href.startswith('http') else href
                        return url, search_url
        
        return None, search_url
        
    except requests.RequestException as e:
        logging.error(f"Search error for '{comic_name}': {str(e)}")
        return None, search_url

def get_cover_url(serie_url, interactive_mode):
    """Extract cover URL from a serie page"""
    try:
        logging.info(f"Waiting {DELAY_SECONDS} seconds before cover request...")
        time.sleep(DELAY_SECONDS)
        
        response = requests.get(
            serie_url,
            headers={
                'User-Agent': USER_AGENT,
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.google.com/'
            },
            timeout=30
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_image = soup.find('meta', {'property': 'og:image'})
        
        return meta_image.get('content') if meta_image else None
        
    except requests.RequestException as e:
        logging.error(f"Cover fetch error for '{serie_url}': {str(e)}")
        return None

def process_row(index, row, df, interactive_mode):
    """Process a single row of the dataframe"""
    # Column indices (0-based)
    TITLE_COL = 6   # Column G (Title)
    LINK_COL = 10   # Column K (URL)
    COVER_COL = 24  # Column Y (Cover URL)
    
    # Safely get values with proper empty checks
    comic_name = str(row[TITLE_COL]) if not is_empty_cell(row[TITLE_COL]) else ""
    current_link = str(row[LINK_COL]) if not is_empty_cell(row[LINK_COL]) else ""
    current_cover = str(row[COVER_COL]) if not is_empty_cell(row[COVER_COL]) else ""
    
    # Additional cleaning
    current_link = current_link.strip()
    current_cover = current_cover.strip()
    
    # Initialize variables for logging
    terminal_status = ""
    file_status = ""
    search_url = ""
    cover_url = ""
    updated = False
    
    # Case 1: Both link and cover exist - skip (no delay)
    if current_link and current_cover:
        terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: filled - Result: Skipping - Cover: exists"
        file_status = "Skipping (both exist)"
    
    # Case 2: Link exists but cover is empty - fetch cover
    elif current_link and not current_cover:
        cover_url = get_cover_url(current_link, interactive_mode)
        if cover_url:
            df.at[index, COVER_COL] = cover_url
            updated = True
            terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: filled - Result: Found - Cover: found"
            file_status = "Found (cover)"
        else:
            terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: filled - Result: Found - Cover: not found"
            file_status = "Found (no cover)"
    
    # Case 3: Link is empty - search for comic
    elif not current_link:
        new_link, search_url = search_bedetheque(comic_name, interactive_mode)
        if new_link:
            df.at[index, LINK_COL] = new_link
            cover_url = get_cover_url(new_link, interactive_mode)
            if cover_url:
                df.at[index, COVER_COL] = cover_url
            updated = True
            terminal_status = f"[{datetime.now().strftime('%m%d %H%M')}] - Row: {index} - {comic_name} - link: empty - Result: Found - Cover: {'found' if cover_url else 'not found'}"
            file_status = "Found (new)"
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
                return False  # Signal to switch to non-interactive
            elif user_input == '':
                return True  # Continue in interactive mode
    
    return interactive_mode  # Maintain current mode

def process_excel_file(input_file, output_file, interactive_mode):
    """Process the Excel file"""
    try:
        # Read the Excel file
        _, ext = os.path.splitext(input_file)
        engine = 'xlrd' if ext.lower() == '.xls' else 'openpyxl'
        df = pd.read_excel(input_file, sheet_name='bd', engine=engine, header=None)
        
        # Verify column structure
        if len(df.columns) <= COVER_COL:
            raise ValueError(f"Input file has only {len(df.columns)} columns, but we need at least {COVER_COL+1} columns")
        
        # Process each row starting from row 4 (index 3)
        for index, row in df.iterrows():
            # Skip first 3 header rows and empty title rows
            if index < 3 or is_empty_cell(row[TITLE_COL]):
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
    
    process_excel_file(args.input_file, args.output_file, args.interactive)

if __name__ == "__main__":
    main()