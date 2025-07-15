import gspread
import os
import time
from urllib.parse import urlparse, urlunparse

# --- CONFIGURATION ---
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
# Copied from our main script to ensure filtering is consistent
BLACKLISTED_DOMAINS = [
    'amazon.com', 'amazon.in', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com',
    'nykaa.com', 'snapdeal.com', 'tatacliq.com', 'jiomart.com', 'pepperfry.com',
    'limeroad.com', 'walmart.com', 'ebay.com', 'etsy.com', 'pinterest.com',
    'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com'
]

def clean_and_validate_url(url):
    """
    Cleans URL to its base domain and checks against the blacklist.
    """
    try:
        parsed = urlparse(url)
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        domain = parsed.netloc.replace('www.', '')
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS):
            return None
        return cleaned_url
    except Exception:
        return None

def correct_google_sheet():
    """
    Reads the Google Sheet, cleans all URLs, removes duplicates, sorts the data,
    and writes the corrected data back.
    """
    # 1. Connect to Google Sheets
    try:
        print("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        print("Connection successful.")
    except Exception as e:
        print(f"❌ Could not connect to Google Sheets. Error: {e}")
        return

    # 2. Read all data
    print("Reading all data from the sheet...")
    try:
        all_data = worksheet.get_all_records()
        if not all_data:
            print("Sheet is empty. Nothing to do.")
            return
        print(f"Found {len(all_data)} total rows to process.")
    except Exception as e:
        print(f"Could not read data. Is there a header row? Error: {e}")
        return

    # 3. Clean URLs and remove duplicates
    unique_leads = {}
    print("Cleaning URLs and removing duplicates...")
    for row in all_data:
        raw_url = row.get("URL")
        if raw_url:
            # THIS IS THE NEW STEP: Clean the URL first
            cleaned_url = clean_and_validate_url(raw_url)
            if cleaned_url:
                # Update the row's URL to the cleaned version
                row['URL'] = cleaned_url
                # Use the cleaned URL as the key to handle duplicates
                unique_leads[cleaned_url] = row

    print(f"Found {len(unique_leads)} unique, valid rows after cleaning.")

    # 4. Sort the unique data alphabetically by URL
    sorted_leads = sorted(unique_leads.values(), key=lambda lead: lead['URL'])
    print("Sorting data alphabetically by URL...")

    # 5. Clear the sheet and write the corrected data back
    print("Clearing the original sheet...")
    worksheet.clear()
    time.sleep(5)

    header = list(sorted_leads[0].keys())
    print("Writing new header...")
    worksheet.append_row(header)

    rows_to_write = [list(row.values()) for row in sorted_leads]

    print(f"Writing {len(sorted_leads)} corrected rows back to the sheet...")
    worksheet.append_rows(rows_to_write, value_input_option='USER_ENTERED')

    print("\n✅ Sheet correction complete!")

if __name__ == "__main__":
    correct_google_sheet()
