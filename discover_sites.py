"""
A scalable script that discovers Indian e-commerce websites using a
dynamic scoring system, analyzes them in parallel, and saves the results.
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
USED_PHRASES_LOG = "used_phrases_log.json"

BLACKLISTED_DOMAINS = [
    'amazon.com', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com', 'nykaa.com',
    'snapdeal.com', 'tatacliq.com', 'jiomart.com', 'pepperfry.com', 'limeroad.com',
    'walmart.com', 'ebay.com', 'etsy.com', 'pinterest.com', 'facebook.com', 'instagram.com',
    'linkedin.com', 'twitter.com', 'youtube.com', 'marketresearch.com', 'dataintelo.com'
]
NEGATIVE_PATH_KEYWORDS = ['blog', 'news', 'docs', 'forum', 'support', 'publication']
NEGATIVE_CONTENT_KEYWORDS = [
    'whiskey', 'liquor', 'wine', 'beer', 'alcohol', 'market research',
    'consulting firm', 'business intelligence'
]
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery', 'blue dart']
POLICY_PAGE_HINTS = ['contact', 'about', 'legal', 'policy', 'shipping', 'terms']
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']

# --- PHRASE GENERATION & HISTORY ---
def auto_generate_phrases():
    """Generates a variety of search phrases based on keywords and templates."""
    categories = {
        "womens_fashion": ["sarees", "kurti", "lehenga", "fusion wear"],
        "mens_fashion": ["oversized tshirt", "hoodie for men"],
        "kids_products": ["organic baby clothes", "crochet toys"],
        "accessories_jewelry": ["handmade jewelry", "leather wallet"],
        "home_kitchen": ["wall shelf", "planters"],
        "beauty_personal_care": ["skincare brand india", "herbal shampoo"],
        "gifts_and_other": ["eco friendly gifts", "custom gift box"],
        "brand_philosophy_india": ["sustainable fashion", "zero waste store"]
    }
    templates = [
        '"{kw}" inurl:shop -amazon -flipkart',
        '"{kw}" online india inurl:store site:.in -amazon',
        '"{kw}" buy online site:.in -flipkart -amazon'
    ]
    generated = {}
    for cat, kws in categories.items():
        generated[cat] = [t.format(kw=kw) for kw in kws for t in templates]
    return generated

def get_fresh_phrases(manual_phrases, used_log_file=USED_PHRASES_LOG):
    """Combines manual and auto-generated phrases, filtering out used ones."""
    auto_generated = auto_generate_phrases()
    all_combined = {cat: list(set(manual_phrases.get(cat, []) + auto_generated.get(cat, [])))
                    for cat in set(manual_phrases) | set(auto_generated)}
    used = set()
    if os.path.exists(used_log_file):
        with open(used_log_file, 'r', encoding='utf-8') as f:
            try: used = set(json.load(f))
            except json.JSONDecodeError: pass
    fresh = {cat: [p for p in phrases if p not in used] for cat, phrases in all_combined.items()}
    return fresh

def log_used_phrases(phrases, used_log_file=USED_PHRASES_LOG):
    """Logs the phrases used in the current run to a file."""
    used = set()
    if os.path.exists(used_log_file):
        with open(used_log_file, 'r', encoding='utf-8') as f:
            try: used = set(json.load(f))
            except json.JSONDecodeError: pass
    used.update(phrases)
    with open(used_log_file, 'w', encoding='utf-8') as f:
        json.dump(list(used), f, indent=2)

# --- SETUP, UTILITY, & SAVING FUNCTIONS ---
def setup_google_sheet():
    """Connects to Google Sheets and sets up the header row."""
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
    """Reads all URLs from the sheet to use for duplicate checking."""
    try:
        logging.info("Fetching existing URLs from Google Sheet...")
        urls = worksheet.col_values(1)
        return set(urls[1:])
    except Exception as e:
        logging.error(f"❌ Could not fetch existing URLs from sheet: {e}")
        return set()

def clean_and_validate_url(url):
    """Cleans URL to its base domain and checks against blacklists."""
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

def save_to_gsheet(worksheet, lead_data):
    """Saves a new row to the connected Google Sheet."""
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
    """Analyzes a URL with a flexible scoring system to identify leads."""
    logging.info(f"   Analyzing {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        html_text = unquote(response.text.lower())  # decode URLs if encoded
        soup = BeautifulSoup(response.text, 'html.parser')
        score = 0

        # --- E-commerce Verification ---
        is_ecommerce = False
        if soup.find(['button', 'a', 'input'], text=re.compile(r'add to cart|buy now|shop now', re.IGNORECASE)):
            is_ecommerce = True
        elif len(soup.find_all(attrs={'class': re.compile(r'product|item|grid|listing', re.IGNORECASE)})) >= 3:
            is_ecommerce = True
        elif '"@type":"product"' in html_text:
            is_ecommerce = True
        elif any(tag in html_text for tag in ['shopify', 'woocommerce', 'cdn.shopify.com']):
            is_ecommerce = True
        elif any(tag in str(soup.head) for tag in ['cdn.shopify.com', 'woocommerce']):
            is_ecommerce = True

        if is_ecommerce:
            score += 1
        else:
            logging.info("   [FAIL] No strong e-commerce signals found.")
            return None

        # --- Negative Keyword Filter ---
        if any(keyword in html_text for keyword in NEGATIVE_CONTENT_KEYWORDS):
            logging.warning(f"   [FAIL] Contains blacklisted content keyword.")
            return None

        # --- Indian Location Verification ---
        is_high_confidence_indian = False
        page_html_to_check = html_text
        for hint in POLICY_PAGE_HINTS:
            page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.IGNORECASE))
            if page_link:
                page_url = urljoin(url, page_link['href'])
                try:
                    page_response = requests.get(page_url, headers=headers, timeout=10)
                    page_html_to_check = page_response.text.lower()
                    logging.info(f"   [INFO] Checking '{hint}' page for location...")
                    break
                except requests.exceptions.RequestException:
                    continue

        if re.search(r'gstin\s*[:\-]?\s*[0-9A-Z]{15}', page_html_to_check):
            logging.info("   [PASS] Found GSTIN number.")
            is_high_confidence_indian = True
        elif re.search(r'\b(pincode|pin code|pin)[\s:-]*\d{6}\b', page_html_to_check):
            logging.info("   [PASS] Found valid PIN code.")
            is_high_confidence_indian = True
        elif any(keyword in html_text for keyword in INDIAN_TECH_KEYWORDS):
            logging.info("   [PASS] Detected Indian payment/shipping tech.")
            is_high_confidence_indian = True
        elif '.in' in urlparse(url).netloc:
            logging.info("   [PASS] Uses .in domain.")
            is_high_confidence_indian = True

        if is_high_confidence_indian:
            score += 1
        elif "india" in html_text:
            logging.info("   [INFO] Mentions India in content.")
            score += 0.5

        # --- Fallback if score is borderline ---
        if not is_high_confidence_indian and "india" in html_text:
            score += 0.3

        if 1.0 <= score < 1.5:
            try:
                os.makedirs("debug_html", exist_ok=True)
                with open(f"debug_html/{urlparse(url).netloc}.html", "w", encoding="utf-8") as f:
                    f.write(html_text)
            except Exception as e:
                logging.warning(f"   [DEBUG] Failed to save borderline HTML: {e}")

        if score < 1.5:
            logging.warning(f"   [FAIL] Final score {score}/2 is too low.")
            return None

        logging.info(f"   ✅ Valid Indian e-commerce lead found (Score: {score}/2): {url}")
        lead_data = {
            "URL": url,
            "Email": _extract_email(html_text),
            "Phone Number": _extract_phone_number(response.text),
            "social_links": _extract_social_links(soup)
        }
        return lead_data

    except requests.exceptions.RequestException as e:
        logging.warning(f"   [FAIL] Could not access the site: {e}")
        return None


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
    if not os.path.exists(GOOGLE_CREDS_FILE): logging.error("❌ Missing Google credentials file. Exiting."); return
    if not SERPER_API_KEY: logging.error("❌ SERPER_API_KEY not found. Exiting."); return

    logging.info("🚀 Starting E-commerce Site Discovery Tool...")
    worksheet = setup_google_sheet()
    if not worksheet: logging.info("Aborting script."); return

    existing_urls = get_existing_urls_from_sheet(worksheet)
    logging.info(f"Found {len(existing_urls)} existing URLs in the sheet.")

    with open(SEARCH_CONFIG_FILE, 'r', encoding='utf-8') as f:
        manual_phrases = json.load(f)

    search_categories = get_fresh_phrases(manual_phrases)
    used_this_run = []

    for category, phrases in search_categories.items():
        if not phrases: logging.info(f"No fresh phrases in category: {category}"); continue
        logging.info(f"\n--- Searching {len(phrases)} fresh phrases in category: {category.upper()} ---")
        urls_to_check = set()
        for phrase in phrases:
            logging.info(f"\n🔍 Searching API: \"{phrase}\"")
            try:
                headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
                payload = json.dumps({'q': phrase, 'num': 10}) # Keep num low to vary results
                response = requests.post("https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                response.raise_for_status()
                search_results = response.json().get('organic', [])
                for result in search_results:
                    base_url = clean_and_validate_url(result.get('link', ''))
                    if base_url and base_url not in existing_urls: urls_to_check.add(base_url)
                used_this_run.append(phrase)
            except Exception as e: logging.error(f"API search error for '{phrase}': {e}")
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_url = {executor.submit(analyze_site, url): url for url in urls_to_check}
            for future in as_completed(future_to_url):
                try:
                    analysis_result = future.result()
                    if analysis_result:
                        save_to_gsheet(worksheet, analysis_result)
                        existing_urls.add(analysis_result["URL"])
                except Exception as e: logging.error(f"Error processing a future: {e}")

    log_used_phrases(used_this_run)
    logging.info("\n🎉 Discovery complete!")

if __name__ == "__main__":
    main()
