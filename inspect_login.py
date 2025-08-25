import requests
from bs4 import BeautifulSoup
import logging

def inspect_login_form():
    """Inspect the login form to understand its structure"""
    logging.basicConfig(level=logging.INFO)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    # Get the login page
    login_url = "https://online.bdgest.com/login"
    response = session.get(login_url)
    
    if response.status_code != 200:
        print(f"Failed to get login page. Status: {response.status_code}")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all forms on the page
    forms = soup.find_all('form')
    print(f"Found {len(forms)} form(s) on the login page")
    
    for i, form in enumerate(forms):
        print(f"\n=== Form {i+1} ===")
        print(f"Action: {form.get('action', 'N/A')}")
        print(f"Method: {form.get('method', 'N/A')}")
        print(f"ID: {form.get('id', 'N/A')}")
        print(f"Class: {form.get('class', 'N/A')}")
        
        # Find all input fields
        inputs = form.find_all('input')
        print(f"Input fields ({len(inputs)}):")
        for input_field in inputs:
            name = input_field.get('name', 'No name')
            input_type = input_field.get('type', 'text')
            value = input_field.get('value', '')
            print(f"  - Name: '{name}', Type: '{input_type}', Value: '{value}'")
        
        # Find all buttons
        buttons = form.find_all('button')
        print(f"Buttons ({len(buttons)}):")
        for button in buttons:
            button_type = button.get('type', 'button')
            button_text = button.get_text().strip()
            print(f"  - Type: '{button_type}', Text: '{button_text}'")

if __name__ == "__main__":
    inspect_login_form()