# Polaris ORM Crawler v11 (Production Ready)

import os, json, time, urllib.parse, urllib.request
from datetime import datetime, timezone

from pathlib import Path

# ================= CONFIG =================
BRAND = "Polaris School of Technology"

QUERIES = [
    "Polaris School of Technology review",
    "Polaris School of Technology",
    "PST",
    "Polaris vs Scaler School of Technology",
    "Polaris Campus",
    "Polaris",
    "Polaris Bangalore",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ================= HELPERS =================
def fetch(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0"
        })
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
        f"key={GOOGLE_API_KEY}&cx={GOOGLE_CX}&q={urllib.parse.quote(q)}&num=10"
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
        f"&maxResults=10&key={YOUTUBE_API_KEY}"
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
            if "polaris" in text.lower():
                comments.append(text)
    except:
        pass

    return comments

# ================= MAIN =================
def main():
    print("🚀 Running ORM Crawler v11")

    all_data = []

    queries = [
        BRAND,
        f"{BRAND} review",
        f"{BRAND} placement",
        f"{BRAND} vs Scaler",
        "PST Pune BTech"
    ]

    # ---- Google based scraping ----
    for q in queries:
        print("🔎", q)
        all_data += google_search(q)
        all_data += google_search(q, "quora.com")
        all_data += google_search(q, "medium.com")
        all_data += google_search(q, "shiksha.com")
        all_data += google_search(q, "collegedunia.com")
        time.sleep(1)

    # ---- YouTube ----
    yt = youtube_videos()
    all_data += yt

    # ---- YouTube comments ----
    for v in yt[:5]:
        vid = v["url"].split("v=")[-1]
        comments = youtube_comments(vid)
        for c in comments:
            all_data.append({
                "platform": "youtube_comment",
                "text": c,
                "date": now()
            })

    # ---- Save ----
    with open(f"{DATA_DIR}/mentions.json", "w") as f:
        json.dump(all_data, f, indent=2)

    print("✅ DONE:", len(all_data))

if __name__ == "__main__":
    main()
