# Polaris ORM Crawler v12 (Stable Production Version)

import os, json, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from bs4 import BeautifulSoup

# ================= CONFIG =================
BRAND = "Polaris School of Technology"

QUERIES = [
    "Polaris School of Technology",
    "Polaris School of Technology placement",
    "Polaris vs Scaler School of Technology",
    "Polaris Campus",
    "PST"
    "Polaris",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ================= HELPERS =================
def fetch(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print("ERROR:", e)
        return None

def now():
    return datetime.now(timezone.utc).isoformat()

# ================= GOOGLE SEARCH =================
def google_search(query, site=None):
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        print("❌ Google API not configured")
        return []

    q = f"{query} site:{site}" if site else query

    url = (
        "https://www.googleapis.com/customsearch/v1?"
        f"key={GOOGLE_API_KEY}&cx={GOOGLE_CX}&q={urllib.parse.quote(q)}"
    )

    raw = fetch(url)
    if not raw:
        return []

    try:
        data = json.loads(raw)
        results = []
        for item in data.get("items", []):
            results.append({
                "title": item.get("title"),
                "snippet": item.get("snippet"),
                "url": item.get("link"),
                "platform": site or "web",
                "date": now()
            })
        return results
    except:
        return []

# ================= YOUTUBE =================
def youtube_videos():
    if not YOUTUBE_API_KEY:
        print("❌ No YouTube API")
        return []

    url = (
        "https://www.googleapis.com/youtube/v3/search?"
        f"q={urllib.parse.quote(BRAND)}&type=video&part=snippet"
        f"&maxResults=5&key={YOUTUBE_API_KEY}"
    )

    raw = fetch(url)
    if not raw:
        return []

    results = []
    try:
        items = json.loads(raw).get("items", [])
        for i in items:
            vid = i["id"]["videoId"]
            results.append({
                "title": i["snippet"]["title"],
                "url": f"https://youtube.com/watch?v={vid}",
                "video_id": vid,
                "platform": "youtube",
                "date": now()
            })
    except:
        pass

    return results

# ================= YOUTUBE COMMENTS =================
def youtube_comments(video_id):
    if not YOUTUBE_API_KEY:
        return []

    url = (
        "https://www.googleapis.com/youtube/v3/commentThreads?"
        f"videoId={video_id}&part=snippet&maxResults=20&key={YOUTUBE_API_KEY}"
    )

    raw = fetch(url)
    if not raw:
        return []

    comments = []
    try:
        items = json.loads(raw).get("items", [])
        for i in items:
            text = i["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comments.append(text)
    except:
        pass

    return comments

# ================= BASIC SCRAPER (SAFE FALLBACK) =================
def scrape_page(url):
    html = fetch(url)
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    return text[:1000]  # limit

# ================= MAIN =================
def run():
    print("🚀 Running ORM Crawler v12")

    all_data = []

    # -------- GOOGLE SEARCH --------
    for q in QUERIES:
        print("🔎", q)

        # Only allowed domains (important due to restriction)
        for site in ["reddit.com", "quora.com", "shiksha.com"]:
            results = google_search(q, site)

            for r in results:
                r["content"] = scrape_page(r["url"])
                all_data.append(r)

        time.sleep(2)

    # -------- YOUTUBE --------
    print("▶️ YouTube")
    videos = youtube_videos()

    for v in videos:
        comments = youtube_comments(v["video_id"])
        v["comments"] = comments
        all_data.append(v)

    # -------- SAVE --------
    file = DATA_DIR / "output.json"
    with open(file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2)

    print(f"✅ DONE: {len(all_data)} records saved")


if __name__ == "__main__":
    run()
