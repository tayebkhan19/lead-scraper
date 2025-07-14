"""
This script discovers e-commerce websites by searching on Google,
analyzes them with multi-layer verification, and saves the results to a
local database and a Google Sheet.
"""
# --- PART 1: IMPORTING OUR TOOLS ---
import json
import os
import random
import re
import sqlite3
import time
from urllib.parse import urljoin, urlparse, urlunparse

import gspread
import requests
from bs4 import BeautifulSoup


# --- PART 2: CONFIGURATION ---
DATABASE_FILE = "ecommerce_sites.db"
SEARCH_CONFIG_FILE = "search_phrases.json"
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
SERPER_API_KEY = os.getenv("SERPER_API_KEY")

# --- REFINEMENT KEYWORDS ---

# List of domains to completely ignore
BLACKLISTED_DOMAINS = [
    'amazon.com', 'amazon.in', 'flipkart.com', 'walmart.com', 'ebay.com',
    'etsy.com', 'youtube.com', 'pinterest.com', 'facebook.com',
    'instagram.com', 'linkedin.com', 'twitter.com', 'help.ecomposer.io',
    'the-macallan.com', 'johnniewalker.com', 'jackdaniels.com'
]

# Keywords that indicate a page is NOT a store
NEGATIVE_KEYWORDS = [
    '/blog/', '/news/', '/docs/', '/forum/', '/support/',
    'whiskey', 'whisky', 'liquor', 'wine', 'beer', 'alcohol'
]

# Keywords for Indian Payment Gateways and Shipping Partners (High Confidence)
INDIAN_TECH_KEYWORDS = [
    'razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart'
]

# Keywords to find policy, about, or shipping pages
POLICY_PAGE_HINTS = ['shipping', 'policy', 'terms', 'about', 'legal']

# Keywords to verify the store is based in India
INDIA_LOCATION_KEYWORDS = {
    'strong': [
        'made in india', 'cash on delivery', 'cod', 'shipping in india',
        'pan india', 'mumbai', 'delhi', 'bangalore', 'bengaluru', 'chennai',
        'kolkata', 'hyderabad', 'pune'
    ],
    'weak': ['india']
}

# --- PART 3: SETUP AND UTILITY FUNCTIONS ---

def setup_database():
    """Initializes the SQLite database."""
    print("Setting up the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE,
            email TEXT, phone_number TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("Database setup complete.")


def setup_google_sheet():
    """Connects to Google Sheets and sets up the header row."""
    try:
        print("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        if not worksheet.get_all_values():
            worksheet.append_row(["URL", "Email", "Phone Number", "Scraped Timestamp"])
        print("Google Sheets connection successful.")
        return worksheet
    except Exception as e:
        print(f"❌ Google Sheets Error: {e}")
        return None


def clean_and_validate_url(url):
    """
    Cleans URL to its base domain and checks against blacklists.
    Returns cleaned URL if valid, otherwise None.
    """
    try:
        parsed = urlparse(url)
        # REBUILD THE URL WITH ONLY THE SCHEME AND DOMAIN
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))

        # Check against domain blacklist
        domain = parsed.netloc.replace('www.', '')
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS):
            return None

        # The negative keyword check is now less critical but can stay
        if any(keyword in url for keyword in NEGATIVE_KEYWORDS):
            return None

        return cleaned_url
    except Exception:
        return None # Ignore malformed URLs


# --- PART 4: SAVING FUNCTIONS ---

def is_url_in_db(url):
    """Checks if a given URL already exists in the local database."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM sites WHERE url = ?", (url,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def save_site_to_db(url, email, phone):
    """Saves a new lead to the SQLite database."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO sites (url, email, phone_number) VALUES (?, ?, ?)", (url, email, phone))
    conn.commit()
    conn.close()
    print(f"✅  Saved to DB: {url}")


def save_to_gsheet(worksheet, url, email, phone):
    """Saves a new row to the connected Google Sheet."""
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        worksheet.append_row([url, email, phone, timestamp])
        print(f"✅  Saved to Google Sheet: {url}")
    except gspread.exceptions.APIError as e:
        print(f"❌ Could not write to Google Sheet. API Error: {e}")


# --- PART 5: THE WEBSITE ANALYZER ---

def analyze_site(url):
    """Analyzes a URL with multiple layers of verification."""
    print(f"   Analyzing {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html_text = response.text.lower()
        soup = BeautifulSoup(response.text, 'html.parser')

        if not ('cart' in html_text and ('shop' in html_text or 'checkout' in html_text)):
            print("   [Verification failed: Lacks key e-commerce terms.]")
            return {"is_ecommerce": False}

        is_confirmed_indian = False
        if any(keyword in html_text for keyword in INDIAN_TECH_KEYWORDS):
            print("   [Verification success: Found Indian tech partner.]")
            is_confirmed_indian = True
        
        if not is_confirmed_indian:
            print("   [No tech partners found. Searching for policy/about pages...]")
            for hint in POLICY_PAGE_HINTS:
                policy_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
                if policy_link:
                    policy_url = urljoin(url, policy_link['href'])
                    print(f"   [Found policy page: {policy_url}]")
                    policy_response = requests.get(policy_url, headers=headers, timeout=10)
                    policy_html = policy_response.text.lower()
                    if any(keyword in policy_html for keyword in INDIA_LOCATION_KEYWORDS['strong']):
                        print("   [Verification success: Found Indian location on policy page.]")
                        is_confirmed_indian = True
                        break

        if not is_confirmed_indian:
            print("   [Verification failed: Could not confirm Indian location.]")
            return {"is_ecommerce": False}

        print("   [Success! It's a confirmed Indian e-commerce site.]")
        email = _extract_email(html_text)
        phone = _extract_phone_number(response.text, soup)

        if not email or not phone:
            # (Contact page search can still run if needed)
            pass

        return {"is_ecommerce": True, "email": email, "phone": phone}
    except requests.exceptions.RequestException as e:
        print(f"   [Could not access the site. Error: {e}]")
        return {"is_ecommerce": False}


def _extract_email(text):
    """Helper function to find the first valid email address."""
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if match and not match.group(0).endswith(('.png', '.jpg', '.gif')):
        return match.group(0)
    return None


def _extract_phone_number(html, soup):
    """Helper function to find a valid Indian phone number."""
    tel_link = soup.find('a', href=re.compile(r'tel:'))
    if tel_link:
        phone = re.sub(r'[^0-9+]', '', tel_link.get('href'))
        if 8 <= len(phone) <= 15:
            return phone
    patterns = [r'(?:(?:\+91|0)[\s-]?)?[6-9]\d{9}', r'(?:(?:\+91|0)[\s-]?)?\d{2,4}[\s-]?\d{6,8}']
    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            cleaned_match = re.sub(r'[^0-9]', '', match)
            if 10 <= len(cleaned_match) <= 12:
                return cleaned_match
    return None


# --- PART 6: THE MAIN SCRIPT ---

def main():
    """Main function to run the discovery tool using a Search API."""
    if not SERPER_API_KEY:
        print("❌ ERROR: SERPER_API_KEY not found. Please set the secret in GitHub Actions.")
        return

    print("🚀 Starting E-commerce Site Discovery Tool...")
    setup_database()
    worksheet = setup_google_sheet()

    with open(SEARCH_CONFIG_FILE, 'r', encoding='utf-8') as f:
        search_categories = json.load(f)

    for category, phrases in search_categories.items():
        print(f"\n--- Searching in category: {category.upper()} ---")
        for phrase in phrases:
            print(f"\n🔍 Searching API with phrase: \"{phrase}\"")
            try:
                headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
                payload = json.dumps({'q': phrase, 'num': 100, 'tbs': 'qdr:y'})
                response = requests.post("https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                response.raise_for_status()
                search_results = response.json().get('organic', [])

                for result in search_results:
                    raw_url = result.get('link')
                    if not raw_url: continue

                    url = clean_and_validate_url(raw_url)
                    if not url:
                        print(f"🟡  Skipping invalid/blacklisted URL: {raw_url}")
                        continue

                    if is_url_in_db(url):
                        print(f"🟡  Skipping known site: {url}")
                        continue

                    analysis = analyze_site(url)
                    if analysis and analysis.get("is_ecommerce"):
                        email, phone = analysis["email"], analysis["phone"]
                        save_site_to_db(url, email, phone)
                        if worksheet:
                            save_to_gsheet(worksheet, url, email, phone)
                    time.sleep(random.randint(2, 5))
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Waiting...")
                time.sleep(30)
    print("\n🎉 Discovery complete!")


if __name__ == "__main__":
