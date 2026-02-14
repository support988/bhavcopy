import requests
import zipfile
import io
import json
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_realistic_headers():
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

# Setup
session = requests.Session()
date_str = "13-Feb-2026"  # Yesterday

# Stage 1: Homepage
print("Stage 1: Visiting homepage...")
session.headers.update(get_realistic_headers())
homepage_resp = session.get("https://www.nseindia.com", timeout=60)
print(f"Homepage status: {homepage_resp.status_code}")
print(f"Cookies: {dict(session.cookies)}")

import time
time.sleep(3)

# Stage 2: Reports page
print("\nStage 2: Reports page...")
session.headers.update(get_realistic_headers())
session.headers["Referer"] = "https://www.nseindia.com"
reports_resp = session.get("https://www.nseindia.com/all-reports", timeout=60)
print(f"Reports status: {reports_resp.status_code}")

time.sleep(2)

# Stage 3: API request
print("\nStage 3: API request...")
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

resp = session.get(API_URL, params=params, timeout=180)

print(f"\n=== NSE API RESPONSE ===")
print(f"Status Code: {resp.status_code}")
print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
print(f"Content-Length: {len(resp.content)} bytes")
print(f"\nFirst 500 characters of response:")
print(resp.content[:500])
print(f"\nLast 200 characters of response:")
print(resp.content[-200:])

# Try to parse as JSON
try:
    data = resp.json()
    print(f"\n=== JSON PARSED SUCCESSFULLY ===")
    print(f"Type: {type(data)}")
    if isinstance(data, list):
        print(f"Length: {len(data)}")
        if len(data) > 0:
            print(f"First item: {data[0]}")
    elif isinstance(data, dict):
        print(f"Keys: {list(data.keys())}")
        print(f"Full content: {data}")
except Exception as e:
    print(f"\n=== JSON PARSING FAILED ===")
    print(f"Error: {e}")

# Try to parse as ZIP
try:
    z = zipfile.ZipFile(io.BytesIO(resp.content))
    print(f"\n=== ZIP PARSED SUCCESSFULLY ===")
    print(f"Files in ZIP: {z.namelist()}")
except Exception as e:
    print(f"\n=== ZIP PARSING FAILED ===")
    print(f"Error: {e}")
