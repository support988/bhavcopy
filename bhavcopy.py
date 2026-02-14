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

# Stages 1-2
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

print("=== TRYING GZIP (manual) ===")
try:
    import gzip
    decompressed = gzip.decompress(content)
    print(f"✅ GZIP SUCCESS! Length: {len(decompressed)}")
    print(f"Content: {decompressed[:500]}")
except Exception as e:
    print(f"❌ GZIP failed: {e}")

print("\n=== CHECKING IF IT'S AN IMAGE (JPEG) ===")
if content[:2] == b'\xff\xd8':
    print("✅ This is a JPEG image!")
elif content[:4] == b'\x89PNG':
    print("✅ This is a PNG image!")
elif content[:2] == b'BM':
    print("✅ This is a BMP image!")
else:
    print(f"❌ Not a standard image format")
    print(f"First bytes: {content[:10].hex()}")

print("\n=== TRYING TO SKIP HEADER ===")
# Maybe there's a custom header, try skipping first N bytes
for skip in [0, 4, 8, 16, 32, 64]:
    try:
        import zipfile
        import io
        z = zipfile.ZipFile(io.BytesIO(content[skip:]))
        print(f"✅ ZIP SUCCESS by skipping {skip} bytes! Files: {z.namelist()}")
        break
    except:
        pass

print("\n=== CHECK HTTP HEADERS ===")
print(f"Content-Type: {resp.headers.get('Content-Type')}")
print(f"Content-Encoding: {resp.headers.get('Content-Encoding')}")
print(f"All headers: {dict(resp.headers)}")
