# discover_sites.py

import os
import json
import logging
import gspread
import requests
import time                   # <-- Added
import random                 # <-- Added
from urllib.parse import unquote  # <-- Added
from google.oauth2.service_account import Credentials

# --- 1. Basic Setup ---
# Setup logging to file, which will be uploaded if the workflow fails
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("lead_discovery.log"),
        logging.StreamHandler()
    ]
)

# --- 2. Load Configuration & Secrets ---
try:
    SERPER_API_KEY = os.environ['SERPER_API_KEY']
    GSHEET_NAME = os.environ['GSHEET_NAME']
    USED_PHRASES_LOG_FILE = 'used_phrases_log.json'
    GOOGLE_CREDS_FILE = 'credentials.json'
    SEARCH_PHRASES_FILE = 'search_phrases.json'
except KeyError as e:
    logging.error(f"Missing environment variable: {e}. Make sure secrets are set in GitHub.")
    exit(1)

# --- 3. Define Core Functions ---

def load_json_file(file_path):
    """Loads a generic JSON file and returns its content."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"'{file_path}' not found. Returning empty list.")
        return []

def save_used_phrases(phrases_list):
    """Saves the updated list of searched phrases."""
    with open(USED_PHRASES_LOG_FILE, 'w') as f:
        json.dump(phrases_list, f, indent=2)
    logging.info(f"Updated '{USED_PHRASES_LOG_FILE}'.")

def get_search_results(phrase):
    """Searches for a phrase using the Serper API."""
    logging.info(f"Searching for phrase: '{phrase}'")
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": phrase})
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=15)
        response.raise_for_status()
        return response.json().get('organic', [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Serper API request failed for phrase '{phrase}': {e}")
        return []

def scrape_site_details(url):
    """
    Scrapes a single website with delays, a user-agent, and error handling.
    """
    logging.info(f"Analyzing {url}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # Add a random delay before making the request to be polite
        time.sleep(random.uniform(2, 5))

        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        # TODO: Add your logic here to parse the page with a library like BeautifulSoup
        # For now, we'll just return a success message.
        scraped_data = "Successfully Scraped"
        return scraped_data

    except requests.exceptions.HTTPError as e:
        logging.error(f"[FAIL] Could not access the site: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[FAIL] A network error occurred for {url}: {e}")
    
    return None  # Return None if accessing the site failed

def update_google_sheet(data_to_add):
    """Authenticates with Google Sheets and adds new data."""
    if not data_to_add:
        logging.info("No new data to add to Google Sheet.")
        return
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open(GSHEET_NAME).sheet1
        sheet.append_rows(data_to_add, value_input_option='USER_ENTERED')
        logging.info(f"Successfully added {len(data_to_add)} rows to '{GSHEET_NAME}'.")
    except Exception as e:
        logging.error(f"Failed to update Google Sheet: {e}")

# --- 4. Main Execution Logic ---
if __name__ == "__main__":
    logging.info("Starting lead scraper script.")

    phrases_to_search = load_json_file(SEARCH_PHRASES_FILE)
    used_phrases = load_json_file(USED_PHRASES_LOG_FILE)
    
    if not phrases_to_search:
        logging.warning("No search phrases found in 'search_phrases.json'. Exiting.")
        exit(0)

    all_new_leads = []

    for phrase in phrases_to_search:
        if phrase in used_phrases:
            logging.info(f"Skipping already used phrase: '{phrase}'")
            continue

        search_results = get_search_results(phrase)
        
        for result in search_results:
            link = result.get('link')
            if not link:
                continue
            
            clean_link = unquote(link)  # Use unquote to clean the URL

            # Scrape details from the individual site link
            scraped_info = scrape_site_details(clean_link)
            
            # Customize the data you want to save to the sheet
            lead_data = [
                result.get('title', 'N/A'),
                clean_link,
                result.get('snippet', 'N/A'),
                phrase,  # The search phrase that found this lead
                scraped_info if scraped_info else "Scrape Failed"
            ]
            all_new_leads.append(lead_data)
        
        # Mark phrase as used after processing all its results
        used_phrases.append(phrase)

    # Update the Google Sheet once with all new leads found
    if all_new_leads:
        update_google_sheet(all_new_leads)
    
    # Save the updated log file for the next run
    save_used_phrases(used_phrases)

    logging.info("Script finished successfully.")
