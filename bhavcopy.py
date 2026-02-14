import requests
import json
import random
import time

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

session = requests.Session()
date_str = "13-Feb-2026"

# Stage 1-2
session.headers.update(get_realistic_headers())
session.get("https://www.nseindia.com", timeout=60)
time.sleep(3)
session.headers.update(get_realistic_headers())
session.headers["Referer"] = "https://www.nseindia.com"
session.get("https://www.nseindia.com/all-reports", timeout=60)
time.sleep(2)

# Stage 3
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
content = resp.content

print("=== DIAGNOSTICS ===")
print(f"Length: {len(content)}")
print(f"First 20 bytes (hex): {content[:20].hex()}")
print(f"First 20 bytes (repr): {repr(content[:20])}")

# Try different decompression methods
print("\n=== TRYING ZLIB ===")
try:
    import zlib
    decompressed = zlib.decompress(content)
    print(f"✅ ZLIB SUCCESS! Length: {len(decompressed)}")
    print(f"First 200 chars: {decompressed[:200]}")
except Exception as e:
    print(f"❌ ZLIB failed: {e}")

print("\n=== TRYING ZLIB WITH WBITS ===")
try:
    import zlib
    decompressed = zlib.decompress(content, -zlib.MAX_WBITS)
    print(f"✅ ZLIB (raw deflate) SUCCESS! Length: {len(decompressed)}")
    print(f"First 200 chars: {decompressed[:200]}")
except Exception as e:
    print(f"❌ ZLIB (raw deflate) failed: {e}")

print("\n=== TRYING BROTLI ===")
try:
    import brotli
    decompressed = brotli.decompress(content)
    print(f"✅ BROTLI SUCCESS! Length: {len(decompressed)}")
    print(f"First 200 chars: {decompressed[:200]}")
except ImportError:
    print("❌ brotli not installed")
except Exception as e:
    print(f"❌ BROTLI failed: {e}")

print("\n=== SAVE RAW FILE ===")
with open('/tmp/nse_response.bin', 'wb') as f:
    f.write(content)
print("Saved to /tmp/nse_response.bin")
print("Run: file /tmp/nse_response.bin")
