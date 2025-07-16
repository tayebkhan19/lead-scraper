"""
This script discovers Indian e-commerce websites, analyzes them with a
multi-layer verification system, and saves the results to a Google Sheet.
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

# --- REFINEMENT KEYWORDS ---
BLACKLISTED_DOMAINS = [
    'amazon.com', 'amazon.in', 'flipkart.com', 'myntra.com', 'ajio.com',
    'meesho.com', 'nykaa.com', 'snapdeal.com', 'tatacliq.com', 'jiomart.com',
    'pepperfry.com', 'limeroad.com', 'walmart.com', 'ebay.com', 'etsy.com',
    'pinterest.com', 'facebook.com', 'instagram.com', 'linkedin.com',
    'twitter.com', 'youtube.com', 'marketresearch.com', 'dataintelo.com'
]
NEGATIVE_PATH_KEYWORDS = ['blog', 'news', 'docs', 'forum', 'support', 'publication']
NEGATIVE_CONTENT_KEYWORDS = [
    'whiskey', 'whisky', 'liquor', 'wine', 'beer', 'alcohol',
    'market research', 'consulting firm', 'business intelligence'
]
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart']
POLICY_PAGE_HINTS = ['contact', 'about', 'legal', 'policy', 'shipping', 'terms']
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
    """Reads all URLs from the sheet to use for duplicate checking."""
    try:
        print("Fetching existing URLs from Google Sheet...")
        urls = worksheet.col_values(1)
        return set(urls[1:])
    except Exception as e:
        print(f"❌ Could not fetch existing URLs from sheet. Error: {e}")
        return set()

def clean_and_validate_url(url):
    """Cleans URL to its base domain and checks against blacklists."""
    try:
        match = re.search(r'https?://[^\s?#]+', url)
        if not match: return None
        url_to_parse = match.group(0)

        if any(keyword in url_to_parse for keyword in NEGATIVE_PATH_KEYWORDS):
            return None

        parsed = urlparse(url_to_parse)
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        domain = parsed.netloc.replace('www.', '')
        
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS):
            return None
        return cleaned_url
    except Exception:
        return None

# --- PART 4: SAVING FUNCTIONS ---
def save_to_gsheet(worksheet, lead_data):
    """Saves a new row to the connected Google Sheet."""
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        socials = lead_data.get("social_links", {})
        worksheet.append_row([
            lead_data.get("URL", "Not Found"),
            lead_data.get("Email", "Not Found"),
            lead_data.get("Phone Number", "Not Found"),
            socials.get("facebook", "Not Found"),
            socials.get("instagram", "Not Found"),
            socials.get("twitter", "Not Found"),
            socials.get("linkedin", "Not Found"),
            timestamp
        ])
        print(f"✅  Saved to Google Sheet: {lead_data.get('URL')}")
    except gspread.exceptions.APIError as e:
        print(f"❌ Could not write to Google Sheet. API Error: {e}")

# --- PART 5: THE WEBSITE ANALYZER ---
def analyze_site(url):
    """Analyzes a URL with the final, strictest verification rules."""
    print(f"   Analyzing {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        html_text = response.text.lower()
        soup = BeautifulSoup(response.text, 'html.parser')

        # 1. Strict E-commerce Element Check
        is_ecommerce = False
        if len(soup.find_all(attrs={'class': re.compile(r'product', re.IGNORECASE)})) > 3:
             is_ecommerce = True
        elif soup.find(['button', 'a', 'input'], text=re.compile(r'add to cart|buy now', re.IGNORECASE)):
            is_ecommerce = True
        if not is_ecommerce:
            print("   [FAIL] Lacks multiple product elements or a clear 'add to cart' button.")
            return None

        # 2. Negative Content Check
        if any(keyword in html_text for keyword in NEGATIVE_CONTENT_KEYWORDS):
            print("   [FAIL] Found negative content keyword (e.g., alcohol, market research).")
            return None

        # 3. Strict Location Check
        is_confirmed_indian = False
        page_html_to_check = html_text
        for hint in POLICY_PAGE_HINTS:
            page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
            if page_link:
                page_url = urljoin(url, page_link['href'])
                try:
                    page_response = requests.get(page_url, headers=headers, timeout=10)
                    page_html_to_check = page_response.text.lower()
                    print(f"   [INFO] Found and checking '{hint}' page for definitive location proof.")
                    break
                except requests.exceptions.RequestException: continue
        
        if re.search(r'\b(pincode|pin code|pin)[\s:-]*\d{6}\b', page_html_to_check):
             print("   [PASS] Found a 6-digit PIN code with context.")
             is_confirmed_indian = True
        elif any(keyword in html_text for keyword in INDIAN_TECH_KEYWORDS):
             print("   [PASS] Found Indian tech partner.")
             is_confirmed_indian = True

        if not is_confirmed_indian:
            print("   [FAIL] Could not confirm a physical Indian location or tech partner.")
            return None

        print(f"   [Success! Found a valid lead: {url}]")
        lead_data = {
            "URL": url,
            "Email": _extract_email(html_text),
            "Phone Number": _extract_phone_number(response.text, soup),
            "social_links": _extract_social_links(soup)
        }
        return lead_data
    except requests.exceptions.RequestException as e:
        print(f"   [FAIL] Could not access the site: {e}")
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
    if not worksheet: print("Aborting script."); return

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
                    if not base_url or base_url in existing_urls:
                        continue

                    analysis_result = analyze_site(base_url)
                    if analysis_result:
                        save_to_gsheet(worksheet, analysis_result)
                        existing_urls.add(base_url)
                    time.sleep(random.randint(2, 5))
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Waiting...")
                time.sleep(60)
    print("\n🎉 Discovery complete!")

if __name__ == "__main__":
    main()
