"""
This script discovers e-commerce websites, analyzes them for contact info,
and saves the results to a Google Sheet, using the sheet to prevent duplicates.
"""
# --- PART 1: IMPORTING OUR TOOLS ---
import json
import os
import random
import re
import time
from urllib.parse import urljoin, urlparse, urlunparse

import gspread
import requests
from bs4 import BeautifulSoup


# --- PART 2: CONFIGURATION ---
SEARCH_CONFIG_FILE = "search_phrases.json"
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
SERPER_API_KEY = os.getenv("SERPER_API_KEY")

# (All keyword lists are unchanged)
BLACKLISTED_DOMAINS = [
    'amazon.com', 'amazon.in', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com',
    'nykaa.com', 'snapdeal.com', 'tatacliq.com', 'jiomart.com', 'pepperfry.com',
    'limeroad.com', 'walmart.com', 'ebay.com', 'etsy.com', 'pinterest.com',
    'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com'
]
NEGATIVE_KEYWORDS = ['/blog/', '/news/', '/docs/', '/forum/', '/support/', 'whiskey', 'whisky', 'liquor', 'wine', 'beer', 'alcohol']
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart']
POLICY_PAGE_HINTS = ['shipping', 'policy', 'terms', 'about', 'legal', 'story']
INDIA_LOCATION_KEYWORDS = {
    'strong': ['made in india', 'cash on delivery', 'cod', 'shipping in india', 'pan india', 'mumbai', 'delhi', 'bangalore', 'bengaluru', 'chennai', 'kolkata', 'hyderabad', 'pune'],
    'weak': ['india']
}
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']


# --- PART 3: SETUP AND UTILITY FUNCTIONS ---

def setup_google_sheet():
    """Connects to Google Sheets and sets up the header row."""
    try:
        print("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        if not worksheet.get_all_values():
            worksheet.append_row([
                "URL", "Email", "Phone Number", "Facebook", "Instagram",
                "Twitter", "LinkedIn", "Scraped Timestamp"
            ])
        print("Google Sheets connection successful.")
        return worksheet
    except Exception as e:
        print(f"❌ Google Sheets Error: {e}")
        return None

def get_existing_urls_from_sheet(worksheet):
    """NEW: Reads all URLs from column A of the sheet to use for duplicate checking."""
    try:
        print("Fetching existing URLs from Google Sheet to prevent duplicates...")
        urls = worksheet.col_values(1)
        # Return a set for fast lookups, skipping the header
        return set(urls[1:])
    except Exception as e:
        print(f"❌ Could not fetch existing URLs from sheet. Error: {e}")
        return set()

def clean_and_validate_url(url):
    # (This function is unchanged)
    try:
        parsed = urlparse(url)
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        domain = parsed.netloc.replace('www.', '')
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS) or \
           any(keyword in url for keyword in NEGATIVE_KEYWORDS):
            return None
        return cleaned_url
    except Exception:
        return None


# --- PART 4: SAVING FUNCTIONS ---

def save_to_gsheet(worksheet, url, lead_data):
    """Saves a new row, including social links, to the connected Google Sheet."""
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        worksheet.append_row([
            url,
            lead_data['email'],
            lead_data['phone'],
            lead_data['social_links'].get('facebook', 'Not Found'),
            lead_data['social_links'].get('instagram', 'Not Found'),
            lead_data['social_links'].get('twitter', 'Not Found'),
            lead_data['social_links'].get('linkedin', 'Not Found'),
            timestamp
        ])
        print(f"✅  Saved to Google Sheet: {url}")
    except gspread.exceptions.APIError as e:
        print(f"❌ Could not write to Google Sheet. API Error: {e}")


# --- PART 5: THE WEBSITE ANALYZER ---
# (analyze_site and its helper functions are unchanged)
def analyze_site(url):
    print(f"   Analyzing {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html_text = response.text.lower()
        soup = BeautifulSoup(response.text, 'html.parser')
        if not ('cart' in html_text and ('shop' in html_text or 'checkout' in html_text)): return None
        is_confirmed_indian = False
        if any(keyword in html_text for keyword in INDIAN_TECH_KEYWORDS): is_confirmed_indian = True
        if not is_confirmed_indian:
            for hint in POLICY_PAGE_HINTS:
                page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
                if page_link:
                    page_url = urljoin(url, page_link['href'])
                    page_response = requests.get(page_url, headers=headers, timeout=10)
                    page_html = page_response.text.lower()
                    if any(keyword in page_html for keyword in INDIA_LOCATION_KEYWORDS['strong']):
                        is_confirmed_indian = True
                        break
        if not is_confirmed_indian: return None
        print("   [Success! It's a confirmed Indian e-commerce site.]")
        lead_data = {"email": _extract_email(html_text), "phone": _extract_phone_number(response.text, soup), "social_links": _extract_social_links(soup)}
        return lead_data
    except requests.exceptions.RequestException as e:
        print(f"   [Could not access the site. Error: {e}]")
        return None

def _extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if match and not match.group(0).endswith(('.png', '.jpg', '.gif')): return match.group(0)
    return None

def _extract_phone_number(html, soup):
    tel_link = soup.find('a', href=re.compile(r'tel:'))
    if tel_link:
        phone = re.sub(r'[^0-9+]', '', tel_link.get('href'))
        if 8 <= len(phone) <= 15: return phone
    patterns = [r'(?:(?:\+91|0)[\s-]?)?[6-9]\d{9}', r'(?:(?:\+91|0)[\s-]?)?\d{2,4}[\s-]?\d{6,8}']
    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            cleaned_match = re.sub(r'[^0-9]', '', match)
            if 10 <= len(cleaned_match) <= 12: return cleaned_match
    return None

def _extract_social_links(soup):
    social_links = {}
    for link in soup.find_all('a', href=True):
        href = link['href']
        for domain in SOCIAL_MEDIA_DOMAINS:
            domain_name = domain.split('.')[0]
            if domain_name == 'googleusercontent': domain_name = 'youtube'
            if domain in href and domain_name not in social_links:
                social_links[domain_name] = href
    return social_links

# --- PART 6: THE MAIN SCRIPT ---
def main():
    """Main function to run the discovery tool."""
    if not SERPER_API_KEY:
        print("❌ ERROR: SERPER_API_KEY not found. Please set the secret.")
        return

    print("🚀 Starting E-commerce Site Discovery Tool...")
    worksheet = setup_google_sheet()
    if not worksheet:
        print("Aborting script as Google Sheet could not be accessed.")
        return

    # NEW: Get existing URLs from the sheet to prevent duplicates
    existing_urls = get_existing_urls_from_sheet(worksheet)
    print(f"Found {len(existing_urls)} existing URLs in the sheet.")

    with open(SEARCH_CONFIG_FILE, 'r', encoding='utf-8') as f:
        search_categories = json.load(f)

    for category, phrases in search_categories.items():
        print(f"\n--- Searching in category: {category.upper()} ---")
        for phrase in phrases:
            print(f"\n🔍 Searching API with phrase: \"{phrase}\"")
            try:
                headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
                payload = json.dumps({'q': phrase, 'num': 100})
                response = requests.post("https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                response.raise_for_status()
                search_results = response.json().get('organic', [])

                for result in search_results:
                    raw_url = result.get('link')
                    if not raw_url: continue

                    base_url = clean_and_validate_url(raw_url)
                    if not base_url: continue

                    # NEW: Check against the set of URLs from the sheet
                    if base_url in existing_urls:
                        continue

                    if any(keyword in raw_url for keyword in NEGATIVE_KEYWORDS):
                        print(f"   [Found blog link, analyzing main site: {base_url}]")

                    analysis_result = analyze_site(base_url)
                    if analysis_result:
                        # Add to sheet and also to our in-memory set
                        save_to_gsheet(worksheet, base_url, analysis_result)
                        existing_urls.add(base_url)

                    time.sleep(random.randint(2, 5))
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Waiting...")
                time.sleep(30)
    print("\n🎉 Discovery complete!")


if __name__ == "__main__":
    main()
