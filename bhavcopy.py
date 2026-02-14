import requests
import zipfile
import io
import pandas as pd
import datetime
import sys
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import logging
import random

# ========================= 
# LOGGING SETUP
# ========================= 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========================= 
# SMART ANTI-BOT EVASION CONFIG
# ========================= 

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
]

def get_realistic_headers():
    """Generate realistic browser headers that change each time"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "Referer": "https://www.nseindia.com/"
    }

def smart_delay(min_seconds=2, max_seconds=5):
    """Add realistic human-like delays"""
    delay = random.uniform(min_seconds, max_seconds)
    logger.info(f"⏳ Human-like delay: {delay:.2f}s")
    time.sleep(delay)

# ========================= 
# DATE CALCULATION
# ========================= 
IST = timezone(timedelta(hours=5, minutes=30))

def get_last_trading_day():
    """Get the last trading day, skipping weekends"""
    today_ist = datetime.now(IST).date()
    trade_date = today_ist - timedelta(days=1)
    
    if trade_date.weekday() == 6:  # Sunday
        trade_date = trade_date - timedelta(days=2)
    elif trade_date.weekday() == 5:  # Saturday
        trade_date = trade_date - timedelta(days=1)
    
    logger.info(f"📅 Trade Date: {trade_date} ({trade_date.strftime('%A')})")
    return trade_date

trade_date = get_last_trading_day()
date_str_nse = trade_date.strftime("%d-%b-%Y")
date_str_bse = trade_date.strftime("%Y%m%d")

# ========================= 
# GOOGLE SHEETS AUTH
# ========================= 
def setup_google_sheets():
    try:
        creds_dict = json.loads(os.environ["GSHEET_CREDS"])
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(os.environ["SPREADSHEET_ID"])
        logger.info("✅ Google Sheets authentication successful")
        return sheet
    except Exception as e:
        logger.error(f"❌ Google Sheets auth failed: {e}")
        raise

sheet = setup_google_sheets()

# ========================= 
# SESSION SETUP
# ========================= 
def create_smart_session():
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=10,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=1,
        pool_maxsize=1
    )
    
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

# ========================= 
# NSE FETCHER - TRY ZIP FIRST
# ========================= 
def fetch_nse_bhavcopy_smart(trade_date, date_str):
    logger.info("\n🚀 Fetching NSE Bhavcopy (Smart Mode)")
    
    session = create_smart_session()
    
    # Stage 1: Homepage
    try:
        logger.info("🌐 Stage 1: Visiting NSE homepage")
        session.headers.update(get_realistic_headers())
        homepage_resp = session.get("https://www.nseindia.com", timeout=60, allow_redirects=True)
        logger.info(f"   Status: {homepage_resp.status_code}, Cookies: {len(session.cookies)}")
        smart_delay(3, 6)
    except Exception as e:
        logger.warning(f"⚠️ Homepage visit failed: {e}")
    
    # Stage 2: Reports page
    try:
        logger.info("📊 Stage 2: Browsing to reports")
        session.headers.update(get_realistic_headers())
        session.headers["Referer"] = "https://www.nseindia.com"
        reports_resp = session.get("https://www.nseindia.com/all-reports", timeout=60, allow_redirects=True)
        logger.info(f"   Status: {reports_resp.status_code}")
        smart_delay(2, 4)
    except Exception as e:
        logger.warning(f"⚠️ Reports page failed: {e}")
    
    # Stage 3: API request
    logger.info("📥 Stage 3: Requesting Bhavcopy")
    
    API_URL = "https://www.nseindia.com/api/reports"
    ARCHIVES_PAYLOAD = [{
        "name": "CM-UDiFF Common Bhavcopy Final (zip)",
        "type": "daily-reports",
        "category": "capital-market",
        "section": "equities"
    }]
    
    params = {
        "archives": json.dumps(ARCHIVES_PAYLOAD),
        "date": date_str,
        "type": "equities",
        "mode": "single"
    }
    
    session.headers.update(get_realistic_headers())
    session.headers.update({
        "Referer": "https://www.nseindia.com/all-reports",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    })
    
    try:
        resp = session.get(API_URL, params=params, timeout=180)
        logger.info(f"📦 Response: {resp.status_code}")
        
        if resp.status_code == 200:
            content = resp.content
            logger.info(f"   Content length: {len(content)} bytes")
            logger.info(f"   First 4 bytes: {content[:4]}")
            
            # CRITICAL: Try ZIP FIRST (before JSON)
            try:
                z = zipfile.ZipFile(io.BytesIO(content))
                logger.info(f"✅ Direct ZIP with {len(z.namelist())} files: {z.namelist()}")
                return z
            except zipfile.BadZipFile:
                logger.info("   Not a direct ZIP file, trying JSON...")
            
            # Only try JSON if ZIP failed
            try:
                # Try to decode as text first
                text_content = content.decode('utf-8')
                data = json.loads(text_content)
                logger.info(f"   JSON response type: {type(data)}")
                
                if isinstance(data, list) and len(data) > 0 and "filePath" in data[0]:
                    file_path = data[0]["filePath"]
                    zip_url = "https://archives.nseindia.com" + file_path
                    
                    logger.info(f"📥 Downloading from archive: {zip_url}")
                    smart_delay(1, 2)
                    
                    session.headers.update(get_realistic_headers())
                    session.headers["Referer"] = "https://www.nseindia.com/all-reports"
                    zip_resp = session.get(zip_url, timeout=120)
                    
                    if zip_resp.status_code == 200:
                        z = zipfile.ZipFile(io.BytesIO(zip_resp.content))
                        logger.info(f"✅ Downloaded ZIP from archives")
                        return z
                else:
                    logger.warning(f"⚠️ JSON missing filePath")
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.warning(f"⚠️ Not valid JSON: {e}")
                logger.warning(f"   This might be compressed data NSE is sending")
            
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout even with 180s")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return None

def process_nse_data(z):
    try:
        csv_name = z.namelist()[0]
        logger.info(f"📄 Processing NSE file: {csv_name}")
        
        df_nse = pd.read_csv(z.open(csv_name))
        df_nse.columns = [c.strip() for c in df_nse.columns]
        
        logger.info(f"📊 NSE Columns: {df_nse.columns.tolist()[:10]}...")
        
        # NSE column mapping
        possible_mappings = [
            {
                "ISIN": "ISIN",
                "TradDt": "Trade_Date",
                "TckrSymb": "Symbol",
                "ClsPric": "Close_Price",
                "SctySrs": "SctySrs"
            },
            {
                "ISIN": "ISIN",
                "TIMESTAMP": "Trade_Date",
                "SYMBOL": "Symbol",
                "CLOSE": "Close_Price",
                "SERIES": "SctySrs"
            },
        ]
        
        COLUMN_MAP_NSE = None
        for mapping in possible_mappings:
            if all(c in df_nse.columns for c in mapping):
                COLUMN_MAP_NSE = mapping
                logger.info(f"✅ Using NSE mapping: {list(mapping.keys())}")
                break
        
        if not COLUMN_MAP_NSE:
            logger.error(f"❌ No valid NSE mapping found")
            logger.error(f"   Available columns: {df_nse.columns.tolist()}")
            return False
        
        df_nse_final = df_nse[list(COLUMN_MAP_NSE.keys())].rename(columns=COLUMN_MAP_NSE)
        df_nse_final = df_nse_final.replace([float("inf"), float("-inf")], "").fillna("")
        
        logger.info("📤 Uploading to Google Sheets (NSE tab)")
        ws_nse = sheet.worksheet("NSE")
        ws_nse.clear()
        ws_nse.update([df_nse_final.columns.tolist()] + df_nse_final.values.tolist())
        
        logger.info(f"✅ NSE uploaded: {len(df_nse_final)} rows")
        return True
    except Exception as e:
        logger.error(f"❌ NSE processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# ========================= 
# BSE FETCHER
# ========================= 
def fetch_bse_bhavcopy(trade_date, date_str):
    logger.info("\n🚀 Fetching BSE Bhavcopy")
    
    session = create_smart_session()
    session.headers.update(get_realistic_headers())
    session.headers["Referer"] = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
    
    bse_urls = [
        f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_str}_F_0000.CSV",
        f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date_str}_CSV.ZIP",
    ]
    
    for url in bse_urls:
        try:
            logger.info(f"📡 Trying: {url}")
            bse_resp = session.get(url, timeout=90)
            
            if bse_resp.status_code == 200:
                logger.info(f"✅ BSE response: {len(bse_resp.content)} bytes")
                if url.endswith('.ZIP'):
                    z = zipfile.ZipFile(io.BytesIO(bse_resp.content))
                    return pd.read_csv(z.open(z.namelist()[0]))
                else:
                    return pd.read_csv(io.BytesIO(bse_resp.content))
        except Exception as e:
            logger.warning(f"⚠️ Failed: {e}")
    
    return None

def process_bse_data(df_bse):
    try:
        df_bse.columns = [c.strip() for c in df_bse.columns]
        logger.info(f"📊 BSE Columns: {df_bse.columns.tolist()[:10]}...")
        
        # BSE now uses same format as NSE!
        possible_mappings = [
            {
                "ISIN": "ISIN",
                "TradDt": "Trade_Date",
                "TckrSymb": "Symbol",
                "ClsPric": "Close_Price",
                "SctySrs": "SctySrs"
            },
            {
                "ISIN_CODE": "ISIN",
                "TRADING_DATE": "Trade_Date",
                "SC_NAME": "Symbol",
                "CLOSE": "Close_Price",
                "SC_TYPE": "SctySrs"
            },
            {
                "SC_CODE": "ISIN",
                "DATE1": "Trade_Date",
                "SC_NAME": "Symbol",
                "CLOSE": "Close_Price",
                "SC_TYPE": "SctySrs"
            },
        ]
        
        COLUMN_MAP_BSE = None
        for mapping in possible_mappings:
            if all(c in df_bse.columns for c in mapping):
                COLUMN_MAP_BSE = mapping
                logger.info(f"✅ Using BSE mapping: {list(mapping.keys())}")
                break
        
        if not COLUMN_MAP_BSE:
            logger.error(f"❌ No valid BSE mapping found")
            logger.error(f"   Available columns: {df_bse.columns.tolist()}")
            return False
        
        df_bse_final = df_bse[list(COLUMN_MAP_BSE.keys())].rename(columns=COLUMN_MAP_BSE)
        df_bse_final = df_bse_final.replace([float("inf"), float("-inf")], "").fillna("")
        
        logger.info("📤 Uploading to Google Sheets (BSE tab)")
        ws_bse = sheet.worksheet("BSE")
        ws_bse.clear()
        ws_bse.update([df_bse_final.columns.tolist()] + df_bse_final.values.tolist())
        
        logger.info(f"✅ BSE uploaded: {len(df_bse_final)} rows")
        return True
    except Exception as e:
        logger.error(f"❌ BSE processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# ========================= 
# MAIN
# ========================= 
def main():
    logger.info("\n" + "="*60)
    logger.info("SMART BHAVCOPY FETCHER - Anti-Bot Evasion ENABLED")
    logger.info("="*60)
    
    nse_success = False
    bse_success = False
    
    try:
        z = fetch_nse_bhavcopy_smart(trade_date, date_str_nse)
        if z:
            nse_success = process_nse_data(z)
        else:
            logger.warning("⚠️ NSE fetch returned None")
    except Exception as e:
        logger.error(f"❌ NSE exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    if nse_success:
        smart_delay(2, 4)
    
    try:
        df_bse = fetch_bse_bhavcopy(trade_date, date_str_bse)
        if df_bse is not None:
            bse_success = process_bse_data(df_bse)
    except Exception as e:
        logger.error(f"❌ BSE exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("\n" + "="*60)
    logger.info(f"NSE: {'✅ SUCCESS' if nse_success else '❌ FAILED'}")
    logger.info(f"BSE: {'✅ SUCCESS' if bse_success else '❌ FAILED'}")
    logger.info("="*60)
    
    if nse_success or bse_success:
        logger.info("\n🎉 Job completed successfully!")
        sys.exit(0)
    else:
        logger.error("\n💔 Both exchanges failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
