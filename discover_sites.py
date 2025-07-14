"""
This script discovers e-commerce websites, analyzes them for contact info,
social media links, and founder names, and saves the results.
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
BLACKLISTED_DOMAINS = [
    'amazon.com', 'amazon.in', 'flipkart.com', 'walmart.com', 'ebay.com', 'etsy.com',
    'youtube.com', 'pinterest.com', 'facebook.com', 'instagram.com', 'linkedin.com',
    'twitter.com', 'help.ecomposer.io'
]
NEGATIVE_KEYWORDS = [
    '/blog/', '/news/', '/docs/', '/forum/', '/support/',
    'whiskey', 'whisky', 'liquor', 'wine', 'beer', 'alcohol'
]
INDIAN_TECH_KEYWORDS = [
    'razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart'
]
# Updated to find founder-related pages
POLICY_PAGE_HINTS = ['shipping', 'policy', 'terms', 'about', 'legal', 'story', 'founder']
INDIA_LOCATION_KEYWORDS = {
    'strong': [
        'made in india', 'cash on delivery', 'cod', 'shipping in india', 'pan india',
        'mumbai', 'delhi', 'bangalore', 'bengaluru', 'chennai', 'kolkata', 'hyderabad', 'pune'
    ],
    'weak': ['india']
}
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']


# --- PART 3: SETUP AND UTILITY FUNCTIONS ---
def setup_database():
    """Initializes the SQLite database and adds all necessary columns."""
    print("Setting up the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            id INTEGER PRIMARY KEY, url TEXT NOT NULL UNIQUE,
            email TEXT, phone_number TEXT, founder_name TEXT, facebook_url TEXT,
            instagram_url TEXT, twitter_url TEXT, linkedin_url TEXT
        )
    ''')
    # Add columns, ignoring errors if they already exist
    all_columns = ['founder_name', 'facebook_url', 'instagram_url', 'twitter_url', 'linkedin_url']
    for col in all_columns:
        try:
            cursor.execute(f"ALTER TABLE sites ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
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
            worksheet.append_row([
                "URL", "Email", "Phone Number", "Founder Name", "Facebook",
                "Instagram", "Twitter", "LinkedIn", "Scraped Timestamp"
            ])
        print("Google Sheets connection successful.")
        return worksheet
    except Exception as e:
        print(f"❌ Google Sheets Error: {e}")
        return None

# (clean_and_validate_url function is unchanged)
def clean_and_validate_url(url):
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
def is_url_in_db(url):
    # (This function is unchanged)
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM sites WHERE url = ?", (url,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def save_site_to_db(url, lead_data):
    """Saves a new lead, including all data, to the SQLite database."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO sites (url, email, phone_number, founder_name, facebook_url, instagram_url, twitter_url, linkedin_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        url,
        lead_data['email'],
        lead_data['phone'],
        lead_data['founder'],
        lead_data['social_links'].get('facebook'),
        lead_data['social_links'].get('instagram'),
        lead_data['social_links'].get('twitter'),
        lead_data['social_links'].get('linkedin')
    ))
    conn.commit()
    conn.close()
    print(f"✅  Saved to DB: {url}")

def save_to_gsheet(worksheet, url, lead_data):
    """Saves a new row, including all data, to the connected Google Sheet."""
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        worksheet.append_row([
            url,
            lead_data['email'],
            lead_data['phone'],
            lead_data['founder'],
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
def analyze_site(url):
    """Analyzes a URL for e-commerce, location, and contact/social/founder info."""
    print(f"   Analyzing {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html_text = response.text.lower()
        soup = BeautifulSoup(response.text, 'html.parser')

        # (All verification logic is unchanged)
        if not ('cart' in html_text and ('shop' in html_text or 'checkout' in html_text)):
            return None
        is_confirmed_indian = False
        if any(keyword in html_text for keyword in INDIAN_TECH_KEYWORDS):
            is_confirmed_indian = True
        
        founder_name = "Not Found"

        # Search for founder name and location on policy/about pages
        if not is_confirmed_indian:
            print("   [No tech partners found. Searching for policy/about pages...]")
            for hint in POLICY_PAGE_HINTS:
                page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
                if page_link:
                    page_url = urljoin(url, page_link['href'])
                    print(f"   [Found relevant page: {page_url}]")
                    page_response = requests.get(page_url, headers=headers, timeout=10)
                    page_html = page_response.text
                    # Check for location confirmation
                    if any(keyword in page_html.lower() for keyword in INDIA_LOCATION_KEYWORDS['strong']):
                        print("   [Verification success: Found Indian location on relevant page.]")
                        is_confirmed_indian = True
                    # Check for founder name
                    if founder_name == "Not Found":
                         founder_name = _extract_founder_name(page_html)

        if not is_confirmed_indian:
            print("   [Verification failed: Could not confirm Indian location.]")
            return None

        print("   [Success! It's a confirmed Indian e-commerce site.]")
        # Final data collection
        lead_data = {
            "email": _extract_email(html_text),
            "phone": _extract_phone_number(response.text, soup),
            "social_links": _extract_social_links(soup),
            "founder": founder_name
        }
        return lead_data
    except requests.exceptions.RequestException as e:
        print(f"   [Could not access the site. Error: {e}]")
        return None

# (Helper functions _extract_email and _extract_phone_number are unchanged)
def _extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if match and not match.group(0).endswith(('.png', '.jpg', '.gif')):
        return match.group(0)
    return None

def _extract_phone_number(html, soup):
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

def _extract_social_links(soup):
    # (This function is unchanged)
    social_links = {}
    for link in soup.find_all('a', href=True):
        href = link['href']
        for domain in SOCIAL_MEDIA_DOMAINS:
            domain_name = domain.split('.')[0]
            if domain_name == 'googleusercontent': domain_name = 'youtube'
            if domain in href and domain_name not in social_links:
                social_links[domain_name] = href
    return social_links

def _extract_founder_name(html):
    """NEW: Helper function to find a founder's name using keywords."""
    # Look for patterns like "founded by [Name]", "founder is [Name]", etc.
    # This pattern captures 1 to 3 capitalized words following the keyword.
    match = re.search(r'(?:founder|founded by|by)\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2})', html, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return "Not Found"


# --- PART 6: THE MAIN SCRIPT ---
def main():
    """Main function to run the discovery tool."""
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
                payload = json.dumps({'q': phrase, 'num': 100})
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

                    analysis_result = analyze_site(url)
                    if analysis_result:
                        save_site_to_db(url, analysis_result)
                        if worksheet:
                            save_to_gsheet(worksheet, url, analysis_result)
                    time.sleep(random.randint(2, 5))
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Waiting...")
                time.sleep(30)
    print("\n🎉 Discovery complete!")


if __name__ == "__main__":
    main()
