import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote, unquote, urlencode, urljoin
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
import getpass

# Configure constants
DELAY_SECONDS = 5  # Reduced delay for authenticated requests
LOG_FILE = "comic_processor.log"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.15 Safari/605.1.15"
]
MIN_COVER_LENGTH = 15  # Minimum length to consider a cover URL valid

# Column indices (0-based) - Adjusted based on your file structure
TITLE_COL = 6    # Column G (Title)
LINK_COL = 10    # Column K (URL)
COVER_COL = 24   # Column Y (Cover)

# Online BDGest URLs
BASE_URL = "https://online.bdgest.com"
LOGIN_URL = f"{BASE_URL}/login"
IMPORT_URL = f"{BASE_URL}/albums/import"
SEARCH_URL = f"{BASE_URL}/albums/import"

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

def wait_for_user(interactive_mode, message="Press ENTER to continue or type 'go' for non-interactive mode: "):
    """Wait for user input in interactive mode"""
    if interactive_mode:
        user_input = input(message).strip().lower()
        if user_input == 'go':
            return False
    return interactive_mode

def create_session(username, password, interactive_mode):
    """Create an authenticated session with online.bdgest.com"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'Referer': BASE_URL
    })
    
    try:
        # First, get the login page to extract CSRF token
        logging.info("Fetching login page to get CSRF token...")
        login_page = session.get(LOGIN_URL, timeout=10)
        login_page.raise_for_status()
        
        interactive_mode = wait_for_user(interactive_mode, "Login page fetched. Press ENTER to continue: ")
        
        soup = BeautifulSoup(login_page.text, 'html.parser')
        
        # Look for the specific CSRF token used by BDGest
        csrf_token = None
        csrf_input = soup.find('input', {'name': 'csrf_token_bdg'})
        if csrf_input:
            csrf_token = csrf_input.get('value')
            logging.info(f"Found CSRF token: {csrf_token[:10]}...")
        
        if not csrf_token:
            logging.error("Could not find CSRF token (csrf_token_bdg) on login page")
            return None, interactive_mode
        
        # Prepare login data - using the exact field names from the form
        login_data = {
            'csrf_token_bdg': csrf_token,
            'li1': 'username',
            'li2': 'password', 
            'source': '',
            'username': username,
            'password': password,
            'auto_connect': 'on'  # Remember me checkbox
        }
        
        logging.info("Attempting login...")
        
        # Perform login with proper headers
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': BASE_URL,
            'Referer': LOGIN_URL
        }
        
        response = session.post(LOGIN_URL, data=login_data, headers=headers, timeout=15, allow_redirects=True)
        
        interactive_mode = wait_for_user(interactive_mode, "Login attempt completed. Press ENTER to continue: ")
        
        # Check if login was successful by examining response
        success_indicators = [
            'mon-compte', 'déconnexion', 'logout', 'Déconnexion', 'Mon compte',
            'mes-albums', 'collection', 'mon profil', 'mon espace'
        ]
        
        # Check response text for success indicators
        response_lower = response.text.lower()
        if any(indicator in response_lower for indicator in success_indicators):
            logging.info("Login successful! Found account indicators in response.")
            return session, interactive_mode
        
        # Check if we were redirected away from login page
        if response.url != LOGIN_URL and 'login' not in response.url:
            logging.info(f"Login likely successful - redirected to: {response.url}")
            return session, interactive_mode
        
        # Check for error messages
        error_soup = BeautifulSoup(response.text, 'html.parser')
        error_messages = error_soup.find_all(class_=re.compile(r'error|alert|message|danger'))
        for error in error_messages:
            error_text = error.get_text().strip()
            if error_text:
                logging.error(f"Login error: {error_text}")
        
        # Save response for debugging
        with open("login_debug.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        
        logging.error("Login failed. Could not find success indicators in response.")
        logging.error(f"Response URL: {response.url}")
        logging.error("Saved response to login_debug.html for inspection")
        return None, interactive_mode
        
    except Exception as e:
        logging.error(f"Login error: {str(e)}")
        traceback.print_exc()
        return None, interactive_mode

def search_online_bdgest(session, comic_name, interactive_mode):
    """Search for a comic on online.bdgest.com using the import module"""
    try:
        # First, get the import page to set up the session
        logging.info(f"Searching for: {comic_name}")
        response = session.get(IMPORT_URL, timeout=15)
        response.raise_for_status()
        
        interactive_mode = wait_for_user(interactive_mode, "Import page fetched. Press ENTER to continue: ")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Prepare search parameters - using the simple search field
        search_params = {
            's': comic_name,  # Series name (main search field)
        }
        
        # Perform the search
        search_response = session.get(SEARCH_URL, params=search_params, timeout=15)
        search_response.raise_for_status()
        
        interactive_mode = wait_for_user(interactive_mode, "Search completed. Press ENTER to continue: ")
        
        if not interactive_mode:
            time.sleep(DELAY_SECONDS)
        
        search_soup = BeautifulSoup(search_response.text, 'html.parser')
        
        # Look for search results - try multiple selectors
        result_selectors = [
            '.album-item', '.result-item', '.item-album', 
            '.list-albums li', '.search-result', '.album',
            'div[class*="album"]', 'div[class*="result"]'
        ]
        
        results = None
        for selector in result_selectors:
            results = search_soup.select(selector)
            if results:
                logging.info(f"Found {len(results)} results using selector: {selector}")
                break
        
        if not results:
            logging.warning(f"No results found for '{comic_name}'")
            # Save the search results page for debugging
            with open(f"search_debug_{comic_name}.html", "w", encoding="utf-8") as f:
                f.write(search_response.text)
            return None, search_response.url, interactive_mode
        
        # Try to find the first valid result with a link to bedetheque
        for result in results[:3]:  # Check first 3 results
            # Look for links that might go to bedetheque
            links = result.find_all('a', href=True)
            for link in links:
                href = link['href']
                if 'bedetheque.com' in href:
                    logging.info(f"Found bedetheque link: {href}")
                    return href, search_response.url, interactive_mode
                
                # Also check for internal links that might lead to bedetheque info
                if '/album/' in href:
                    album_url = urljoin(BASE_URL, href)
                    # Visit the album page to find bedetheque link
                    bedetheque_url = get_bedetheque_link_from_album(session, album_url, interactive_mode)
                    if bedetheque_url:
                        return bedetheque_url, search_response.url, interactive_mode
        
        logging.warning(f"No bedetheque links found in results for '{comic_name}'")
        # Save the search results page for debugging
        with open(f"search_debug_{comic_name}.html", "w", encoding="utf-8") as f:
            f.write(search_response.text)
        return None, search_response.url, interactive_mode
        
    except Exception as e:
        logging.error(f"Online BDGest search error for '{comic_name}': {str(e)}")
        traceback.print_exc()
        return None, IMPORT_URL, interactive_mode

def get_bedetheque_link_from_album(session, album_url, interactive_mode):
    """Extract bedetheque link from an album detail page"""
    try:
        response = session.get(album_url, timeout=15)
        response.raise_for_status()
        
        interactive_mode = wait_for_user(interactive_mode, "Album page fetched. Press ENTER to continue: ")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for bedetheque links
        bedetheque_links = soup.find_all('a', href=re.compile(r'bedetheque\.com'))
        
        for link in bedetheque_links:
            href = link['href']
            logging.info(f"Found bedetheque link on album page: {href}")
            return href
        
        return None
        
    except Exception as e:
        logging.error(f"Error getting bedetheque link from album page: {str(e)}")
        return None

def get_cover_url(session, bedetheque_url, interactive_mode):
    """Extract cover URL from a bedetheque page"""
    try:
        # We need to access bedetheque.com directly for the cover
        # Create a separate session for bedetheque to avoid authentication issues
        bedetheque_session = requests.Session()
        bedetheque_session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        })
        
        response = bedetheque_session.get(bedetheque_url, timeout=30)
        response.raise_for_status()
        
        interactive_mode = wait_for_user(interactive_mode, "Bedetheque page fetched. Press ENTER to continue: ")
        
        if not interactive_mode:
            time.sleep(DELAY_SECONDS)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try multiple methods to find the cover image
        # 1. Look for og:image meta tag
        meta_image = soup.find('meta', {'property': 'og:image'})
        if meta_image and meta_image.get('content'):
            cover_url = meta_image.get('content')
            logging.info(f"Found cover via og:image: {cover_url}")
            return cover_url
        
        # 2. Look for image with class 'cover'
        cover_img = soup.find('img', class_='cover')
        if cover_img and cover_img.get('src'):
            src = cover_img.get('src')
            if src.startswith('//'):
                cover_url = f"https:{src}"
            elif src.startswith('/'):
                cover_url = f"https://www.bedetheque.com{src}"
            else:
                cover_url = src
            logging.info(f"Found cover via img.cover: {cover_url}")
            return cover_url
        
        # 3. Look for any image in the content area
        content_div = soup.find('div', class_='content') or soup.find('div', class_='album-detail')
        if content_div:
            content_img = content_div.find('img')
            if content_img and content_img.get('src'):
                src = content_img.get('src')
                if src.startswith('//'):
                    cover_url = f"https:{src}"
                elif src.startswith('/'):
                    cover_url = f"https://www.bedetheque.com{src}"
                else:
                    cover_url = src
                logging.info(f"Found cover via content image: {cover_url}")
                return cover_url
        
        logging.warning(f"No cover image found on bedetheque page: {bedetheque_url}")
        return None
        
    except Exception as e:
        logging.error(f"Cover fetch error for '{bedetheque_url}': {str(e)}")
        return None

def process_row(index, row, df, session, interactive_mode):
    """Process a single row of the dataframe"""
    # Safely get values with proper empty checks
    comic_name = str(row[TITLE_COL]) if not pd.isna(row[TITLE_COL]) else ""
    current_link = str(row[LINK_COL]) if not pd.isna(row[LINK_COL]) else ""
    current_cover = str(row[COVER_COL]) if not pd.isna(row[COVER_COL]) else ""
    
    # Clean strings
    comic_name = comic_name.strip()
    current_link = current_link.strip()
    current_cover = current_cover.strip()
    
    # Skip if comic name is empty
    if not comic_name:
        return interactive_mode
    
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
        cover_url = get_cover_url(session, current_link, interactive_mode)
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
        new_link, search_url, interactive_mode = search_online_bdgest(session, comic_name, interactive_mode)
        if new_link:
            df.at[index, LINK_COL] = new_link
            cover_url = get_cover_url(session, new_link, interactive_mode)
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
        interactive_mode = wait_for_user(interactive_mode)
    
    return interactive_mode

def process_excel_file(input_file, output_file, session, interactive_mode):
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
            interactive_mode = process_row(index, row, df, session, interactive_mode)
            
            # Save progress after each update
            df.to_excel(output_file, sheet_name='bd', index=False, header=False, engine='openpyxl')
        
        logging.info("Processing complete")
        
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description='Process comic book data from Excel file using online.bdgest.com')
    parser.add_argument('input_file', help='Input Excel file path')
    parser.add_argument('output_file', help='Output Excel file path')
    parser.add_argument('-i', '--interactive', action='store_true', help='Enable interactive mode')
    args = parser.parse_args()
    
    setup_logging()
    
    # Get credentials from user
    username = input("Enter your online.bdgest.com username: ")
    password = getpass.getpass("Enter your password: ")
    
    # Create authenticated session
    session, interactive_mode = create_session(username, password, args.interactive)
    if not session:
        logging.error("Failed to create authenticated session. Exiting.")
        logging.error("Please check the login_debug.html file for details.")
        return
    
    logging.info(f"Starting processing with {'interactive' if interactive_mode else 'non-interactive'} mode")
    
    process_excel_file(args.input_file, args.output_file, session, interactive_mode)

if __name__ == "__main__":
    main()