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
DELAY_SECONDS = 30  # Reduced delay to avoid being flagged
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

def create_session():
    """Create a requests session with proper headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'Referer': 'https://www.bedetheque.com/'
    })
    return session

def search_bedetheque_direct(comic_name, interactive_mode):
    """Search for a comic directly on bedetheque.com"""
    search_url = f"https://www.bedetheque.com/search/albums?keywords={quote(comic_name)}"
    
    try:
        session = create_session()
        
        # First, get the main page to establish a session
        main_page = session.get("https://www.bedetheque.com/", timeout=10)
        time.sleep(2)  # Small delay between requests
        
        # Now try the search
        response = session.get(search_url, timeout=30)
        
        # Check if we got a valid response
        logging.info(f"Bedetheque response status: {response.status_code}")
        logging.info(f"Response length: {len(response.text)}")
        
        if response.status_code != 200:
            logging.error(f"Bedetheque returned status code: {response.status_code}")
            return None, search_url
        
        # Check if we're being blocked (very short response)
        if len(response.text) < 1000:
            logging.warning("Possible blocking detected (short response)")
            # Try an alternative approach - use a different URL pattern
            return search_bedetheque_alternative(comic_name, interactive_mode)
        
        response.raise_for_status()
        
        # Delay after the request
        if not interactive_mode:
            logging.info(f"Waiting {DELAY_SECONDS} seconds after search...")
            time.sleep(DELAY_SECONDS)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for search results
        results = soup.find_all('div', class_='liste-series')
        
        if not results:
            logging.warning("No search results found on the page")
            # Try to find any links that might be results
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link['href']
                if ('/serie-' in href or '/BD-' in href) and 'recherche' not in href:
                    full_url = f"https://www.bedetheque.com{href}" if href.startswith('/') else href
                    logging.info(f"Found series link: {full_url}")
                    return full_url, search_url
            return None, search_url
        
        # Extract links from search results
        for result in results:
            links = result.find_all('a', href=True)
            for link in links:
                href = link['href']
                if '/serie-' in href or '/BD-' in href:
                    full_url = f"https://www.bedetheque.com{href}" if href.startswith('/') else href
                    logging.info(f"Found series link in results: {full_url}")
                    return full_url, search_url
        
        logging.warning(f"No valid bedetheque.com links found for '{comic_name}'")
        return None, search_url
        
    except requests.RequestException as e:
        logging.error(f"Bedetheque search error for '{comic_name}': {str(e)}")
        if not interactive_mode:
            time.sleep(DELAY_SECONDS)
        return None, search_url

def search_bedetheque_alternative(comic_name, interactive_mode):
    """Alternative search method using different approach"""
    try:
        # Try a more direct approach - construct URL manually
        # Format: https://www.bedetheque.com/BD-[Comic-Name].html
        formatted_name = comic_name.lower().replace(' ', '-').replace("'", '').replace('é', 'e').replace('è', 'e')
        potential_url = f"https://www.bedetheque.com/BD-{formatted_name}.html"
        
        # Test if this URL exists
        session = create_session()
        response = session.get(potential_url, timeout=15, allow_redirects=False)
        
        if response.status_code == 200:
            logging.info(f"Found direct URL: {potential_url}")
            return potential_url, f"Direct URL test for {comic_name}"
        
        # If not found, try series format
        series_url = f"https://www.bedetheque.com/serie-{formatted_name}.html"
        response = session.get(series_url, timeout=15, allow_redirects=False)
        
        if response.status_code == 200:
            logging.info(f"Found series URL: {series_url}")
            return series_url, f"Series URL test for {comic_name}"
        
        return None, f"Alternative search for {comic_name}"
        
    except requests.RequestException as e:
        logging.error(f"Alternative search error for '{comic_name}': {str(e)}")
        return None, f"Alternative search for {comic_name}"

def get_cover_url(serie_url, interactive_mode):
    """Extract cover URL from a serie page"""
    try:
        session = create_session()
        response = session.get(serie_url, timeout=30)
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
        
        # 3. Look for any image in the content area
        content_div = soup.find('div', class_='content') or soup.find('div', class_='album-detail')
        if content_div:
            content_img = content_div.find('img')
            if content_img and content_img.get('src'):
                src = content_img.get('src')
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
    
    # Case 3: Link is empty - search for comic
    elif not current_link:
        new_link, search_url = search_bedetheque_direct(comic_name, interactive_mode)
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
    
    process_excel_file(args.input_file, args.output_file, args.interactive)

if __name__ == "__main__":
    main()