"""
A scalable script that discovers Indian e-commerce websites using a
manual phrase system, analyzes them in parallel, and saves the results.
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

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0'
]

BLACKLISTED_DOMAINS = [
    'amazon.com', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com', 'nykaa.com',
    'snapdeal.com', 'tatacliq.com', 'jiomart.com', 'pepperfry.com', 'limeroad.com',
    'walmart.com', 'ebay.com', 'etsy.com', 'pinterest.com', 'facebook.com', 'instagram.com',
    'linkedin.com', 'twitter.com', 'youtube.com', 'marketresearch.com', 'dataintelo.com'
]
NEGATIVE_PATH_KEYWORDS = ['blog', 'news', 'docs', 'forum', 'support', 'publication', 'careers']
NEGATIVE_CONTENT_KEYWORDS = [
    'whiskey', 'liquor', 'wine', 'beer', 'alcohol', 'market research',
    'consulting firm', 'business intelligence'
]
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart', 'ccavenue']
POLICY_PAGE_HINTS = ['contact', 'about', 'legal', 'policy', 'shipping', 'terms', 'privacy']
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']


# --- SETUP, UTILITY, & SAVING FUNCTIONS ---
def setup_google_sheet():
    try:
        logging.info("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.sheet1
        # Check if sheet is empty and add headers if so
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
        logging.info(f"Found {len(urls) -1} existing URLs in the sheet.")
        return set(urls[1:]) # Return a set for fast lookups
    except Exception as e:
        logging.error(f"❌ Could not fetch existing URLs from sheet: {e}")
        return set()

def clean_and_validate_url(url):
    try:
        # Extract the base URL without parameters
        match = re.search(r'https?://[^\s?#]+', url)
        if not match: return None

        url_to_parse = match.group(0)

        # Check for negative keywords in the URL path
        if any(f"/{keyword}" in url_to_parse for keyword in NEGATIVE_PATH_KEYWORDS): return None

        parsed = urlparse(url_to_parse)
        # Rebuild URL with just scheme and domain to get the homepage
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        domain = parsed.netloc.replace('www.', '')

        # Check against blacklisted domains
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS): return None

        return cleaned_url
    except Exception:
        return None

def save_to_gsheet(worksheet, lead_data):
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
        logging.info(f"✅  Saved to Google Sheet: {lead_data.get('URL')}")
    except gspread.exceptions.APIError as e:
        # This can happen if the API quota is exceeded
        logging.error(f"❌ Could not write to Google Sheet due to API error: {e}")
        time.sleep(60) # Wait a minute before trying again later


# --- DATA EXTRACTION HELPERS ---
def _extract_email(text):
    # Regex to find email addresses
    match = re.search(r'[\w\.\-]+@[\w\.\-]+\.\w+', text)
    return match.group(0) if match else "Not Found"

def _extract_phone_number(text):
    # Use the phonenumbers library to find and format Indian phone numbers
    for match in phonenumbers.PhoneNumberMatcher(text, "IN"):
        formatted_number = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
        return formatted_number
    return "Not Found"

def _extract_social_links(soup):
    social_links = {}
    for a in soup.find_all('a', href=True):
        href = a['href']
        for domain in SOCIAL_MEDIA_DOMAINS:
            if domain in href:
                # Get the social media platform name (e.g., 'facebook')
                platform = domain.split('.')[0]
                if platform not in social_links: # Only save the first link found for each platform
                    social_links[platform] = href
    return social_links


# --- WEBSITE ANALYZER ---
def analyze_site(url):
    logging.info(f"   Analyzing {url}...")
    try:
        time.sleep(random.uniform(1.0, 3.0)) # Anti-scraping delay
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=15, verify=True)
        response.raise_for_status()

        html_text = response.text.lower()
        soup = BeautifulSoup(response.text, 'html.parser')
        score = 0

        # 1. E-commerce check
        is_ecommerce = False
        if soup.find(['button', 'a', 'input'], text=re.compile(r'add to cart|buy now|shop now', re.IGNORECASE)): is_ecommerce = True
        elif len(soup.find_all(attrs={'class': re.compile(r'product|item|grid|listing', re.IGNORECASE)})) >= 3: is_ecommerce = True
        elif '"@type":"product"' in html_text: is_ecommerce = True
        elif any(tag in html_text for tag in ['shopify', 'woocommerce', 'cdn.shopify.com']): is_ecommerce = True

        if is_ecommerce:
            score += 1
        else:
            logging.info("   [FAIL] No strong e-commerce signals found."); return None

        # 2. Negative content check
        if any(keyword in html_text for keyword in NEGATIVE_CONTENT_KEYWORDS):
            logging.warning("   [FAIL] Contains blacklisted content keyword."); return None

        # 3. Indian location check
        is_high_confidence_indian = False
        page_html_to_check = html_text
        # Check secondary pages like 'contact us' for better location signals
        for hint in POLICY_PAGE_HINTS:
            page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
            if page_link and page_link.get('href'):
                page_url = urljoin(url, page_link['href'])
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    page_response = requests.get(page_url, headers=headers, timeout=10)
                    page_html_to_check += page_response.text.lower() # Append content
                    logging.info(f"   [INFO] Checking '{hint}' page for location...")
                    break # Stop after finding the first one to save time
                except requests.exceptions.RequestException:
                    continue # Ignore if the sub-page fails

        if re.search(r'gstin\s*[:\-]?\s*[0-9A-Z]{15}', page_html_to_check):
            logging.info("   [PASS] Found GSTIN number."); is_high_confidence_indian = True
        elif re.search(r'\b(pincode|pin code|pin)[\s:-]*\d{6}\b', page_html_to_check):
            logging.info("   [PASS] Found valid PIN code."); is_high_confidence_indian = True
        elif any(keyword in page_html_to_check for keyword in INDIAN_TECH_KEYWORDS):
            logging.info("   [PASS] Detected Indian payment/shipping tech."); is_high_confidence_indian = True
        elif '.in' in urlparse(url).netloc:
            logging.info("   [PASS] Uses .in domain."); is_high_confidence_indian = True

        if is_high_confidence_indian:
            score += 1
        elif "india" in page_html_to_check:
            logging.info("   [INFO] Mentions India in content."); score += 0.5

        # 4. Final scoring
        if score < 1.5:
            logging.warning(f"   [FAIL] Final score {score}/2 is too low."); return None

        logging.info(f"   ✅ Valid Indian e-commerce lead found (Score: {score}/2): {url}")
        lead_data = {
            "URL": url,
            "Email": _extract_email(page_html_to_check),
            "Phone Number": _extract_phone_number(response.text), # Use original response for phone #
            "social_links": _extract_social_links(soup)
        }
        return lead_data

    except requests.exceptions.RequestException as e:
        logging.warning(f"   [FAIL] Could not access the site: {e}"); return None
    except Exception as e:
        logging.error(f"   [FAIL] An unexpected error occurred during analysis: {e}"); return None

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    logging.info("🚀 Starting E-commerce Site Discovery Tool...")

    if not SERPER_API_KEY:
        logging.error("❌ SERPER_API_KEY environment variable not set. Exiting.")
        exit(1)

    worksheet = setup_google_sheet()
    if not worksheet:
        exit(1)

    existing_urls = get_existing_urls_from_sheet(worksheet)
    all_potential_urls = set()

    try:
        with open(SEARCH_CONFIG_FILE, 'r') as f:
            search_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"❌ Search config file not found at '{SEARCH_CONFIG_FILE}'. Exiting.")
        exit(1)

    # 1. Discover potential URLs from all phrases first
    for category, phrases in search_data.items():
        logging.info(f"\n--- Searching {len(phrases)} phrases in category: {category} ---")
        for phrase in phrases:
            logging.info(f"🔍 Searching API: \"{phrase}\"")
            api_url = "https://google.serper.dev/search"
            payload = json.dumps({"q": phrase, "gl": "in"}) # Geofence to India
            headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
            try:
                response = requests.post(api_url, headers=headers, data=payload, timeout=10)
                results = response.json().get('organic', [])
                for result in results:
                    url = result.get('link')
                    if url:
                        cleaned_url = clean_and_validate_url(url)
                        if cleaned_url:
                            all_potential_urls.add(cleaned_url)
            except Exception as e:
                logging.error(f"❌ Error during API search for '{phrase}': {e}")

    # 2. Filter out URLs that are already in the Google Sheet
    new_urls_to_check = list(all_potential_urls - existing_urls)
    random.shuffle(new_urls_to_check) # Shuffle to vary the order each run
    logging.info(f"\n--- Found {len(all_potential_urls)} potential sites. "
                 f"Analyzing {len(new_urls_to_check)} new sites. ---")

    # 3. Analyze new URLs in parallel
    if new_urls_to_check:
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_url = {executor.submit(analyze_site, url): url for url in new_urls_to_check}
            for future in as_completed(future_to_url):
                try:
                    lead_data = future.result()
                    if lead_data:
                        save_to_gsheet(worksheet, lead_data)
                except Exception as e:
                    logging.error(f"Error processing a future: {e}")

    logging.info("\n🎉 Discovery complete!")
