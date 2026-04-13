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

# Estimated impressions by organic search position (based on avg CTR data)
POSITION_IMPRESSIONS = {
    1: 8500, 2: 4500, 3: 2800, 4: 1900, 5: 1400,
    6: 1100, 7: 900,  8: 750,  9: 600,  10: 500
}

# ================= HELPERS ================= #

def now():
    return datetime.now(timezone.utc).isoformat()

def sentiment(text):
    t = text.lower()
    if any(x in t for x in ["scam", "fake", "bad", "worst", "fraud", "cheating", "avoid", "terrible"]):
        return "negative"
    if any(x in t for x in ["good", "great", "best", "awesome", "excellent", "top", "recommended"]):
        return "positive"
    return "neutral"

def sentiment_score(s):
    return {"positive": 1, "neutral": 0, "negative": -1}[s]

def position_to_impressions(position):
    return POSITION_IMPRESSIONS.get(int(position), 400)

# ================= SERPER ================= #

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
            print(f"  DEBUG SERPER: {query} → {len(results)} results")
            return results
    except Exception as e:
        print("❌ Serper error:", e)
        return []

def build_mentions(results, platform):
    mentions = []
    for r in results[:MAX_LINKS_PER_PLATFORM]:
        title = r.get("title", "")
        link = r.get("link", "")
        position = r.get("position", 10)
        s = sentiment(title)

        mentions.append({
            "platform": platform,
            "text": title,
            "url": link,
            "date": now(),
            "sentiment": s,
            "score": sentiment_score(s),
            "impressions": position_to_impressions(position)
        })
    return mentions

# ================= REDDIT ENRICHMENT ================= #

def get_reddit_stats(url):
    """Fetch real upvote + comment count from Reddit's public JSON API."""
    try:
        json_url = url.rstrip("/") + ".json"
        req = urllib.request.Request(
            json_url,
            headers={"User-Agent": "ORM-Crawler/1.0 (github.com/adirispol/ORM-Dashboard)"}
        )
        with urllib.request.urlopen(req, timeout=10) as res:
            data = json.loads(res.read().decode())
            post = data[0]["data"]["children"][0]["data"]
            score = max(int(post.get("score", 0)), 0)
            comments = int(post.get("num_comments", 0))
            return score + comments
    except Exception:
        return None

def enrich_reddit_impressions(mentions):
    """Replace estimated impressions with real Reddit engagement data."""
    print("  Enriching Reddit stats...")
    enriched = 0
    for mention in mentions:
        url = mention.get("url", "")
        if "reddit.com" in url and "/comments/" in url:
            stats = get_reddit_stats(url)
            if stats is not None:
                mention["impressions"] = stats
                enriched += 1
            time.sleep(0.8)  # Respect Reddit rate limits
    print(f"  Enriched {enriched}/{len(mentions)} Reddit posts with real stats")
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
    data = enrich_reddit_impressions(data)
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
    except Exception as e:
        print("❌ fetch_json error:", e)
        return None

def crawl_youtube():
    print("\n▶ YouTube...")
    data = []

    if not YOUTUBE_API_KEY:
        print("⚠ YOUTUBE KEY MISSING")
        return data

    video_ids = []
    video_map = {}  # vid -> index in data

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
            if vid in video_map:
                continue  # skip duplicate video IDs
            title = item["snippet"]["title"]
            s = sentiment(title)

            video_map[vid] = len(data)
            video_ids.append(vid)
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

    # Fetch real view counts in batches of 50
    for batch_start in range(0, len(video_ids), 50):
        batch = video_ids[batch_start:batch_start + 50]
        ids = ",".join(batch)
        stats_url = (
            "https://www.googleapis.com/youtube/v3/videos?"
            f"part=statistics&id={ids}&key={YOUTUBE_API_KEY}"
        )
        stats = fetch_json(stats_url)
        if stats:
            for item in stats.get("items", []):
                vid = item["id"]
                view_count = int(item["statistics"].get("viewCount", 0))
                if vid in video_map:
                    data[video_map[vid]]["impressions"] = view_count

    print(f"   → {len(data)} videos")
    return data

# ================= MAIN ================= #

def deduplicate(mentions):
    """Remove duplicate URLs, keeping the first occurrence."""
    seen = set()
    result = []
    for m in mentions:
        url = m.get("url", "")
        if url and url not in seen:
            seen.add(url)
            result.append(m)
    removed = len(mentions) - len(result)
    if removed:
        print(f"  Removed {removed} duplicate URLs")
    return result

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

    all_data = deduplicate(all_data)

    total = len(all_data)
    sentiments = Counter([x["sentiment"] for x in all_data])
    platforms = Counter([x["platform"] for x in all_data])
    impressions = sum([x["impressions"] for x in all_data])

    summary = {
        "total_mentions": total,
        "platforms": dict(platforms),
        "sentiment": dict(sentiments),
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
