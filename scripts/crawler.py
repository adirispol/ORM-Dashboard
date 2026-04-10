import os
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from collections import Counter

# ================= CONFIG ================= #

SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

BRAND_QUERIES = [
    "Polaris School of Technology",
    "Polaris Campus",
    "Polaris BTech AI",
    "PST Bangalore"
]

MAX_LINKS_PER_PLATFORM = 5000

# ================= HELPERS ================= #

def now():
    return datetime.now(timezone.utc).isoformat()

def sentiment(text):
    t = text.lower()
    if any(x in t for x in ["scam","fake","bad","worst"]):
        return "negative"
    if any(x in t for x in ["good","great","best","awesome"]):
        return "positive"
    return "neutral"

def sentiment_score(s):
    return {"positive":1,"neutral":0,"negative":-1}[s]

# ================= SERPER FIXED ================= #

def serper_search(query):
    if not SERPER_API_KEY:
        print("❌ SERPER KEY MISSING")
        return []

    url = "https://google.serper.dev/search"

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }

    payload = json.dumps({
        "q": query,
        "gl": "in",
        "hl": "en",
        "num": 10
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=20) as res:
            data = json.loads(res.read().decode())
            results = data.get("organic", [])
            print(f"DEBUG SERPER: {query} → {len(results)} results")
            return results
    except Exception as e:
        print("❌ Serper error:", e)
        return []

def build_mentions(results, platform):
    mentions = []

    for r in results[:MAX_LINKS_PER_PLATFORM]:
        title = r.get("title", "")
        link = r.get("link", "")

        s = sentiment(title)

        mentions.append({
            "platform": platform,
            "text": title,
            "url": link,
            "date": now(),
            "sentiment": s,
            "score": sentiment_score(s),
            "impressions": 1000
        })

    return mentions

# ================= PLATFORMS ================= #

def crawl_quora():
    print("\n❓ Quora...")
    data = []
    for q in BRAND_QUERIES:
        results = serper_search(f'site:quora.com "{q}"')
        data += build_mentions(results, "quora")
        time.sleep(1)
    print(f"   → {len(data)} mentions")
    return data

def crawl_reddit():
    print("\n🟠 Reddit...")
    data = []
    for q in BRAND_QUERIES:
        results = serper_search(f'site:reddit.com "{q}"')
        data += build_mentions(results, "reddit")
        time.sleep(1)
    print(f"   → {len(data)} mentions")
    return data

def crawl_medium():
    print("\n📝 Medium...")
    data = []
    for q in BRAND_QUERIES:
        results = serper_search(f'site:medium.com "{q}"')
        data += build_mentions(results, "medium")
        time.sleep(1)
    print(f"   → {len(data)} mentions")
    return data

def crawl_portals():
    print("\n🏛 Portals...")
    data = []
    portals = ["shiksha.com", "collegedunia.com", "careers360.com"]

    for site in portals:
        for q in BRAND_QUERIES:
            results = serper_search(f'site:{site} "{q}"')
            data += build_mentions(results, "portal")
            time.sleep(1)

    print(f"   → {len(data)} mentions")
    return data

def crawl_web():
    print("\n🌐 Web...")
    data = []
    for q in BRAND_QUERIES:
        results = serper_search(q)
        data += build_mentions(results, "web")
        time.sleep(1)
    print(f"   → {len(data)} mentions")
    return data

# ================= YOUTUBE ================= #

def fetch_json(url):
    try:
        with urllib.request.urlopen(url, timeout=20) as res:
            return json.loads(res.read().decode())
    except:
        return None

def crawl_youtube():
    print("\n▶ YouTube...")
    data = []

    if not YOUTUBE_API_KEY:
        print("⚠ YOUTUBE KEY MISSING")
        return data

    video_ids = []

    for q in BRAND_QUERIES:
        url = (
            "https://www.googleapis.com/youtube/v3/search?"
            f"part=snippet&q={urllib.parse.quote(q)}"
            f"&type=video&maxResults=10&key={YOUTUBE_API_KEY}"
        )

        res = fetch_json(url)
        if not res:
            continue

        for item in res.get("items", []):
            vid = item["id"]["videoId"]
            title = item["snippet"]["title"]

            video_ids.append(vid)

            s = sentiment(title)

            data.append({
                "platform": "youtube",
                "text": title,
                "url": f"https://youtube.com/watch?v={vid}",
                "date": item["snippet"]["publishedAt"],
                "sentiment": s,
                "score": sentiment_score(s),
                "impressions": 0
            })

        time.sleep(1)

    # fetch comment counts
    if video_ids:
        ids = ",".join(video_ids[:50])
        stats_url = (
            "https://www.googleapis.com/youtube/v3/videos?"
            f"part=statistics&id={ids}&key={YOUTUBE_API_KEY}"
        )

        stats = fetch_json(stats_url)
        if stats:
            for i, item in enumerate(stats.get("items", [])):
                count = int(item["statistics"].get("commentCount", 0))
                if i < len(data):
                    data[i]["impressions"] = count

    print(f"   → {len(data)} videos")
    return data

# ================= MAIN ================= #

def main():
    print("🚀 Running ORM crawler...\n")

    print("SERPER:", "OK" if SERPER_API_KEY else "MISSING")
    print("YOUTUBE:", "OK" if YOUTUBE_API_KEY else "MISSING")

    all_data = []
    all_data += crawl_reddit()
    all_data += crawl_quora()
    all_data += crawl_medium()
    all_data += crawl_portals()
    all_data += crawl_web()
    all_data += crawl_youtube()

    total = len(all_data)
    sentiments = Counter([x["sentiment"] for x in all_data])
    platforms = Counter([x["platform"] for x in all_data])
    impressions = sum([x["impressions"] for x in all_data])

    summary = {
        "total_mentions": total,
        "platforms": platforms,
        "sentiment": sentiments,
        "impressions": impressions,
        "last_updated": now()
    }

    os.makedirs("data", exist_ok=True)

    with open("data/mentions.json", "w") as f:
        json.dump(all_data, f, indent=2)

    with open("data/summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n✅ DONE")
    print("Mentions:", total)
    print("Impressions:", impressions)


if __name__ == "__main__":
    main()
