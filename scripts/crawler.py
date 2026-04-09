"""
Polaris ORM Crawler v10.5
========================
Production-ready (free stack)

Sources:
- Google Custom Search → Quora, Medium, Aggregators, Social
- Reddit JSON
- YouTube API (videos + comments)
"""

import json, os, urllib.request, urllib.parse, time, re
from datetime import datetime, timezone
from pathlib import Path

# ================= CONFIG =================

BRAND = "Polaris School of Technology"

QUERIES = [
    "Polaris School of Technology review",
    "Polaris School of Technology placement",
    "PST",
    "Polaris vs Scaler School of Technology",
    "Polaris Campus",
    "Polaris",
    "Polaris Bangalore",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX  = os.getenv("GOOGLE_CX")
YT_KEY     = os.getenv("YOUTUBE_API_KEY")

# ================= HELPERS =================

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode()
    except Exception as e:
        print("ERROR:", e)
        return None

def now():
    return datetime.now(timezone.utc).isoformat()

def uid(x):
    return str(abs(hash(x)))[:12]

def clean(t):
    return re.sub(r"\s+", " ", t or "").strip()

# ================= SENTIMENT =================

POS = ["good","great","best","excellent","amazing","placement","package"]
NEG = ["bad","worst","fake","scam","poor","waste","no placement"]

def sentiment(text):
    t = text.lower()
    p = sum(w in t for w in POS)
    n = sum(w in t for w in NEG)
    if p > n: return "positive"
    if n > p: return "negative"
    return "neutral"

# ================= GOOGLE SEARCH =================

def google_search(query):
    print("🔎", query)

    url = f"https://www.googleapis.com/customsearch/v1?q={urllib.parse.quote(query)}&key={GOOGLE_KEY}&cx={GOOGLE_CX}"
    raw = fetch(url)

    if not raw:
        return []

    data = json.loads(raw)
    results = []

    for item in data.get("items", []):
        results.append({
            "url": item.get("link"),
            "title": clean(item.get("title")),
            "snippet": clean(item.get("snippet")),
        })

    print("   →", len(results))
    return results

# ================= CLASSIFY =================

def classify(url):
    u = url.lower()
    if "quora.com" in u: return "quora"
    if "medium.com" in u: return "medium"
    if "youtube.com" in u: return "youtube"
    if "reddit.com" in u: return "reddit"
    if any(x in u for x in ["shiksha","collegedunia","careers360","getmyuni"]):
        return "aggregator"
    if any(x in u for x in ["linkedin","twitter","instagram"]):
        return "social"
    return "web"

# ================= REDDIT =================

def crawl_reddit():
    print("\n🔴 Reddit")
    mentions = []

    url = f"https://www.reddit.com/search.json?q={urllib.parse.quote(BRAND)}&limit=25"
    raw = fetch(url)

    if not raw:
        return mentions

    data = json.loads(raw)

    for post in data.get("data", {}).get("children", []):
        d = post["data"]
        text = d["title"]

        mentions.append({
            "id": uid(d["id"]),
            "platform": "reddit",
            "title": text,
            "url": "https://reddit.com" + d["permalink"],
            "score": d["score"],
            "comments": d["num_comments"],
            "sentiment": sentiment(text),
            "date": datetime.fromtimestamp(d["created_utc"], tz=timezone.utc).isoformat(),
        })

    return mentions

# ================= YOUTUBE =================

def get_comments(video_id):
    comments = []

    url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id}&key={YT_KEY}&maxResults=50"
    raw = fetch(url)

    if not raw:
        return comments

    data = json.loads(raw)

    for item in data.get("items", []):
        c = item["snippet"]["topLevelComment"]["snippet"]
        text = clean(c["textDisplay"])

        # only relevant comments
        if "polaris" not in text.lower() and "pst" not in text.lower():
            continue

        comments.append({
            "text": text,
            "likes": c["likeCount"],
            "sentiment": sentiment(text),
        })

    return comments

def crawl_youtube():
    print("\n▶️ YouTube")

    mentions = []

    url = f"https://www.googleapis.com/youtube/v3/search?q={urllib.parse.quote(BRAND)}&key={YT_KEY}&part=snippet&type=video&maxResults=10"
    raw = fetch(url)

    if not raw:
        return mentions

    data = json.loads(raw)

    for item in data.get("items", []):
        vid = item["id"]["videoId"]
        snip = item["snippet"]

        comments = get_comments(vid)

        mentions.append({
            "id": uid(vid),
            "platform": "youtube",
            "title": snip["title"],
            "url": f"https://youtube.com/watch?v={vid}",
            "comments_data": comments,
            "comment_count": len(comments),
            "sentiment": sentiment(snip["title"]),
            "date": snip["publishedAt"],
        })

        time.sleep(1)

    return mentions

# ================= MAIN =================

def main():
    all_mentions = []

    # GOOGLE (Quora, Medium, Aggregators, Social)
    for q in QUERIES:
        for r in google_search(q):
            combined = r["title"] + " " + r["snippet"]

            all_mentions.append({
                "id": uid(r["url"]),
                "platform": classify(r["url"]),
                "title": r["title"],
                "snippet": r["snippet"],
                "url": r["url"],
                "sentiment": sentiment(combined),
                "date": now(),
            })
        time.sleep(1)

    # REDDIT
    all_mentions.extend(crawl_reddit())

    # YOUTUBE
    all_mentions.extend(crawl_youtube())

    # DEDUP
    seen = set()
    final = []
    for m in all_mentions:
        if m["id"] not in seen:
            seen.add(m["id"])
            final.append(m)

    # SAVE
    with open(DATA_DIR / "mentions.json", "w") as f:
        json.dump(final, f, indent=2)

    print("\n✅ DONE:", len(final))

if __name__ == "__main__":
    main()
