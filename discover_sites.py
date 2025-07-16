"""
A scalable script that discovers e-commerce websites, analyzes them in parallel,
and saves the results with professional logging and error handling.
"""
# --- IMPORTS ---
import json
import os
import random
import re
import time
from urllib.parse import urljoin, urlparse, urlunparse
import gspread
import requests
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import phonenumbers

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='lead_discovery.log',
    filemode='w'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# --- CONFIGURATION ---
SEARCH_CONFIG_FILE = "search_phrases.json"
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
SERPER_API_KEY = os.getenv("SERPER_API_KEY")

# --- REFINEMENT KEYWORDS ---
BLACKLISTED_DOMAINS = [
    'amazon.com', 'amazon.in', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com',
    'nykaa.com', 'snapdeal.com', 'tatacliq.com', 'jiomart.com', 'pepperfry.com',
    'limeroad.com', 'walmart.com', 'ebay.com', 'etsy.com', 'pinterest.com',
    'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com',
    'marketresearch.com', 'globalcosmeticsnews.com', 'dataintelo.com'
]
NEGATIVE_PATH_KEYWORDS = ['blog', 'news', 'docs', 'forum', 'support', 'publication']
NEGATIVE_CONTENT_KEYWORDS = [
    'whiskey', 'whisky', 'liquor', 'wine', 'beer', 'alcohol',
    'market research', 'consulting firm', 'business intelligence'
]
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart']
POLICY_PAGE_HINTS = ['contact', 'about', 'legal', 'policy', 'shipping', 'terms']
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']

# --- SETUP AND UTILITY FUNCTIONS ---
def setup_google_sheet():
    try:
        logging.info("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        if not worksheet.get_all_values():
            worksheet.append_row([
                "URL", "Email", "Phone Number", "Facebook", "Instagram",
                "Twitter", "LinkedIn", "Scraped Timestamp"
            ])
        logging.info("Google Sheets connection successful.")
        return worksheet
    except Exception as e:
        logging.error(f"❌ Google Sheets Error: {e}")
        return None

def get_existing_urls_from_sheet(worksheet):
    try:
        logging.info("Fetching existing URLs from Google Sheet...")
        urls = worksheet.col_values(1)
        return set(urls[1:])
    except Exception as e:
        logging.error(f"❌ Could not fetch existing URLs from sheet: {e}")
        return set()

def clean_and_validate_url(url):
    try:
        match = re.search(r'https?://[^\s?#]+', url)
        if not match: return None
        url_to_parse = match.group(0)
        if any(keyword in url_to_parse for keyword in NEGATIVE_PATH_KEYWORDS): return None
        parsed = urlparse(url_to_parse)
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        domain = parsed.netloc.replace('www.', '')
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS): return None
        return cleaned_url
    except Exception: return None

# --- SAVING FUNCTION ---
def save_to_gsheet(worksheet, lead_data):
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        socials = lead_data.get("social_links", {})
        worksheet.append_row([
            lead_data.get("URL", "Not Found"), lead_data.get("Email", "Not Found"),
            lead_data.get("Phone Number", "Not Found"), socials.get("facebook", "Not Found"),
            socials.get("instagram", "Not Found"), socials.get("twitter", "Not Found"),
            socials.get("linkedin", "Not Found"), timestamp
        ])
        logging.info(f"✅  Saved to Google Sheet: {lead_data.get('URL')}")
    except gspread.exceptions.APIError as e:
        logging.error(f"❌ Could not write to Google Sheet: {e}")

# --- WEBSITE ANALYZER ---
def analyze_site(url):
    logging.info(f"   Analyzing {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        html_text = response.text.lower()
        soup = BeautifulSoup(response.text, 'html.parser')

        is_ecommerce = False
        if len(soup.find_all(attrs={'class': re.compile(r'product', re.IGNORECASE)})) > 3 or \
           soup.find(['button', 'a', 'input'], text=re.compile(r'add to cart|buy now', re.IGNORECASE)):
            is_ecommerce = True
        if not is_ecommerce: logging.info("   [FAIL] Lacks e-commerce elements."); return None

        if any(keyword in html_text for keyword in NEGATIVE_CONTENT_KEYWORDS):
            logging.info("   [FAIL] Found negative content keyword."); return None

        is_confirmed_indian = False
        page_html_to_check = html_text
        for hint in POLICY_PAGE_HINTS:
            page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
            if page_link:
                page_url = urljoin(url, page_link['href'])
                try:
                    page_response = requests.get(page_url, headers=headers, timeout=10)
                    page_html_to_check = page_response.text.lower()
                    logging.info(f"   [INFO] Checking '{hint}' page for location proof.")
                    break
                except requests.exceptions.RequestException: continue
        
        if re.search(r'\b(pincode|pin code|pin)[\s:-]*\d{6}\b', page_html_to_check):
             logging.info("   [PASS] Found a PIN code with context."); is_confirmed_indian = True
        elif any(keyword in html_text for keyword in INDIAN_TECH_KEYWORDS):
             logging.info("   [PASS] Found Indian tech partner."); is_confirmed_indian = True

        if not is_confirmed_indian: logging.info("   [FAIL] Could not confirm Indian location."); return None

        logging.info(f"   [Success! Found a valid lead: {url}]")
        lead_data = {
            "URL": url, "Email": _extract_email(html_text),
            "Phone Number": _extract_phone_number(response.text),
            "social_links": _extract_social_links(soup)
        }
        return lead_data
    except requests.exceptions.RequestException as e:
        logging.warning(f"   [FAIL] Could not access the site: {e}"); return None

def _extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    if match and not match.group(0).endswith(('.png', '.jpg', '.gif')): return match.group(0)
    return None

def _extract_phone_number(text):
    for match in re.finditer(r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text):
        try:
            parsed_number = phonenumbers.parse(match.group(0), "IN")
            if phonenumbers.is_valid_number(parsed_number):
                return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
        except phonenumbers.phonenumberutil.NumberParseException: continue
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

# --- MAIN SCRIPT ---
def main():
    if not os.path.exists(GOOGLE_CREDS_FILE):
        logging.error("❌ Missing Google credentials file ('credentials.json'). Exiting.")
        return
    if not SERPER_API_KEY:
        logging.error("❌ SERPER_API_KEY not found. Please set the secret. Exiting.")
        return

    logging.info("🚀 Starting E-commerce Site Discovery Tool...")
    worksheet = setup_google_sheet()
    if not worksheet: logging.info("Aborting script."); return

    existing_urls = get_existing_urls_from_sheet(worksheet)
    logging.info(f"Found {len(existing_urls)} existing URLs in the sheet.")

    with open(SEARCH_CONFIG_FILE, 'r', encoding='utf-8') as f:
        search_categories = json.load(f)

    for category, phrases in search_categories.items():
        logging.info(f"\n--- Searching in category: {category.upper()} ---")
        for phrase in phrases:
            logging.info(f"\n🔍 Searching API with phrase: \"{phrase}\"")
            try:
                headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
                payload = json.dumps({'q': phrase, 'num': 100})
                response = requests.post("https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                response.raise_for_status()
                search_results = response.json().get('organic', [])

                urls_to_check = []
                for result in search_results:
                    raw_url = result.get('link')
                    if not raw_url: continue
                    base_url = clean_and_validate_url(raw_url)
                    if base_url and base_url not in existing_urls:
                        urls_to_check.append(base_url)
                
                with ThreadPoolExecutor(max_workers=8) as executor:
                    future_to_url = {executor.submit(analyze_site, url): url for url in urls_to_check}
                    for future in as_completed(future_to_url):
                        try:
                            analysis_result = future.result()
                            if analysis_result:
                                save_to_gsheet(worksheet, analysis_result)
                                existing_urls.add(analysis_result["URL"])
                        except Exception as e:
                            logging.error(f"Error processing a future: {e}")

            except Exception as e:
                logging.error(f"An unexpected error occurred during search: {e}. Waiting...")
                time.sleep(60)
    logging.info("\n🎉 Discovery complete!")

if __name__ == "__main__":
    main()
