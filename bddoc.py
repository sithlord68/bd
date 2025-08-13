import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import re
import os
import traceback
import numpy as np

def search_bedetheque(comic_name):
    """Search for a comic on bedetheque.com and return the exact match URL if found"""
    search_url = f"https://www.bedetheque.com/search/albums/?keywords={quote(comic_name)}"
    
    print(f"  Searching: {search_url}")
    
    try:
        response = requests.get(search_url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for exact match in search results
        results = soup.find_all('div', class_='liste-series')
        print(f"  Found {len(results)} result sections")
        
        if not results:
            return None
        
        for i, result in enumerate(results):
            links = result.find_all('a')
            print(f"  Section {i+1}: Found {len(links)} links")
            
            for j, link in enumerate(links):
                link_text = link.text.strip()
                print(f"    Link {j+1}: '{link_text}'")
                
                # Compare normalized names
                if link_text.lower() == comic_name.strip().lower():
                    href = link.get('href')
                    if href:
                        full_url = f"https://www.bedetheque.com{href}" if not href.startswith('http') else href
                        print(f"    Exact match found! URL: {full_url}")
                        return full_url
        print("  No exact matches found")
        return None
        
    except requests.RequestException as e:
        print(f"  Error searching for {comic_name}: {e}")
        return None

def get_serie_info(serie_url):
    """Extract genre information from a serie page"""
    print(f"  Fetching serie page: {serie_url}")
    
    try:
        response = requests.get(serie_url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the genre information
        genre_label = soup.find('li', string=re.compile(r'Genre\s*:'))
        if not genre_label:
            print("  Genre label not found")
            return None, None
        
        genre_span = genre_label.find('span', class_='style-serie')
        if not genre_span:
            print("  Genre span not found")
            return None, None
        
        genres = [g.strip() for g in genre_span.text.split(',')]
        print(f"  Found genres: {genres}")
        
        primary = genres[0] if genres else None
        secondary = genres[1] if len(genres) > 1 else None
        
        # If there are more than 2 genres, join them in primary
        if len(genres) > 2:
            primary = ", ".join(genres)
            secondary = None
        
        return primary, secondary
        
    except requests.RequestException as e:
        print(f"  Error fetching serie page {serie_url}: {e}")
        return None, None

def is_empty_cell(value):
    """Check if a cell is empty or contains an error"""
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, str) and value.startswith("Err:"):
        return True
    return False

def process_excel_file(input_file, output_file):
    """Process the Excel file using column indices instead of names"""
    try:
        print(f"Reading input file: {input_file}")
        
        # Determine file format and use appropriate engine
        _, ext = os.path.splitext(input_file)
        engine = 'xlrd' if ext.lower() == '.xls' else 'openpyxl'
        
        # Read the Excel file
        df = pd.read_excel(input_file, sheet_name='bd', engine=engine, header=None)
        print(f"Read {len(df)} rows from sheet 'bd'")
        
        # Define column indices based on your example
        # Column indices (0-indexed)
        TITLE_COL = 6    # Column G (7th column) - Comic title
        GENRE_COL = 7    # Column H (8th column) - Primary genre
        LINK_COL = 10    # Column K (11th column) - URL
        SECOND_GENRE_COL = 11  # Column L (12th column) - Secondary genre
        
        print("\nColumn indices used:")
        print(f"  Title Column: {TITLE_COL} (Column {chr(65+TITLE_COL)})")
        print(f"  Genre Column: {GENRE_COL} (Column {chr(65+GENRE_COL)})")
        print(f"  Link Column: {LINK_COL} (Column {chr(65+LINK_COL)})")
        print(f"  Secondary Genre Column: {SECOND_GENRE_COL} (Column {chr(65+SECOND_GENRE_COL)})\n")
        
        # Find the starting row of data (skip header rows)
        start_row = 0
        for i in range(len(df)):
            # Skip empty title cells
            if pd.isna(df.iloc[i, TITLE_COL]):
                continue
                
            # Check if this looks like a header row
            title_value = str(df.iloc[i, TITLE_COL])
            if "titre" in title_value.lower():
                print(f"Found header row at index {i}: '{title_value}'")
                start_row = i + 1
                break
            else:
                start_row = i
                break
                
        print(f"Data starts at row: {start_row + 1} (index {start_row})")
        print(f"Processing {len(df) - start_row} potential comic rows\n")
        
        # Process each row
        processed_count = 0
        for index in range(start_row, len(df)):
            # Skip rows without a comic title
            if pd.isna(df.iloc[index, TITLE_COL]):
                print(f"\nRow {index + 1}: Skipping - No title")
                continue
                
            # Get comic name
            comic_name = str(df.iloc[index, TITLE_COL])
            
            # Check if link column is empty
            link_value = df.iloc[index, LINK_COL]
            if not is_empty_cell(link_value):
                print(f"\nRow {index + 1}:")
                print(f"  Comic Name: '{comic_name}'")
                print(f"  Link Column has value: '{link_value}' - Skipping")
                continue
                
            print(f"\nRow {index + 1}:")
            print(f"  Comic Name: '{comic_name}'")
            print(f"  Link Column is empty - Processing")
            
            # Search for the comic
            serie_url = search_bedetheque(comic_name)
            if not serie_url:
                print("  No match found - skipping")
                continue
            
            # Get genre information
            primary_genre, secondary_genre = get_serie_info(serie_url)
            
            # Update the DataFrame
            print(f"  Updating row:")
            print(f"    Link Column: {serie_url}")
            df.iloc[index, LINK_COL] = serie_url
            
            if primary_genre:
                print(f"    Primary Genre: {primary_genre}")
                df.iloc[index, GENRE_COL] = primary_genre
            if secondary_genre:
                print(f"    Secondary Genre: {secondary_genre}")
                df.iloc[index, SECOND_GENRE_COL] = secondary_genre
            
            processed_count += 1
        
        # Save the updated DataFrame
        print(f"\nSaving results to {output_file}")
        df.to_excel(output_file, sheet_name='bd', index=False, header=False, engine='openpyxl')
        print(f"Processing complete. Updated {processed_count} comics.")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        traceback.print_exc()




if __name__ == "__main__":
    input_filename = "bd-db.xls"  # Change to your input file
    output_filename = "comics_output.xls"  # Change to your output file
    
    print("Starting comic book processor...")
    process_excel_file(input_filename, output_filename)
    print("Done!")
    traceback.print_exc()

