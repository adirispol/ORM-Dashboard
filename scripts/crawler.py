import os
import json
import requests
from datetime import datetime, timezone
from collections import Counter

# ---------------- CONFIG ---------------- #

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

BRAND_QUERIES = [
    "Polaris School of Technology",
    "Polaris Campus",
    "Polaris BTech AI",
    "Polaris",
    "PST"
]

MAX_RESULTS = 10

# ---------------- HELPERS ---------------- #

def now():
    return datetime.now(timezone.utc).isoformat()

def sentiment(text):
    text = text.lower()
    if any(x in text for x in ["scam", "fake", "bad", "worst"]):
        return "negative"
    if any(x in text for x in ["good", "great", "best", "awesome"]):
        return "positive"
    return "neutral"

def sentiment_score(s):
    return {"positive": 1, "neutral": 0, "negative": -1}[s]

def safe_get(url):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code == 200:
            return r
    except:
        return None

# ---------------- REDDIT HYBRID ---------------- #

def crawl_reddit():
    print("🟠 Reddit (Hybrid)...")
    data = []

    if not SERPER_API_KEY:
        return data

    for query in BRAND_QUERIES:
        try:
            res = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY},
                json={"q": f"{query} site:reddit.com"}
            )

            if res.status_code != 200:
                continue

            links = res.json().get("organic", [])[:MAX_RESULTS]

            for r in links:
                link = r.get("link")
                if not link or "reddit.com" not in link:
                    continue

                json_url = link.rstrip("/") + ".json"
                reddit_res = safe_get(json_url)

                if not reddit_res:
                    continue

                try:
                    j = reddit_res.json()
                    post = j[0]["data"]["children"][0]["data"]

                    text = f"{post.get('title','')} {post.get('selftext','')}"
                    s = sentiment(text)

                    data.append({
                        "platform": "reddit",
                        "text": text[:200],
                        "url": link,
                        "date": datetime.fromtimestamp(post["created_utc"], tz=timezone.utc).isoformat(),
                        "sentiment": s,
                        "score": sentiment_score(s),
                        "impressions": post.get("score", 1) * 25
                    })

                except:
                    continue

        except:
            continue

    print(f"   → {len(data)} mentions")
    return data

# ---------------- YOUTUBE ---------------- #

def crawl_youtube():
    print("▶ YouTube...")
    data = []

    if not YOUTUBE_API_KEY:
        return data

    for query in BRAND_QUERIES:
        try:
            url = "https://www.googleapis.com/youtube/v3/search"

            params = {
                "part": "snippet",
                "q": query,
                "key": YOUTUBE_API_KEY,
                "maxResults": MAX_RESULTS,
                "type": "video"
            }

            res = requests.get(url, params=params)

            if res.status_code != 200:
                continue

            for item in res.json().get("items", []):
                title = item["snippet"]["title"]
                s = sentiment(title)

                data.append({
                    "platform": "youtube",
                    "text": title,
                    "url": f"https://youtube.com/watch?v={item['id']['videoId']}",
                    "date": item["snippet"]["publishedAt"],
                    "sentiment": s,
                    "score": sentiment_score(s),
                    "impressions": 5000
                })

        except:
            continue

    print(f"   → {len(data)} videos")
    return data

# ---------------- WEB ---------------- #

def crawl_web():
    print("🌐 Web...")
    data = []

    if not SERPER_API_KEY:
        return data

    for query in BRAND_QUERIES:
        try:
            res = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY},
                json={"q": query}
            )

            if res.status_code != 200:
                continue

            for r in res.json().get("organic", [])[:MAX_RESULTS]:
                text = r.get("title", "")
                s = sentiment(text)

                data.append({
                    "platform": "web",
                    "text": text,
                    "url": r.get("link"),
                    "date": now(),
                    "sentiment": s,
                    "score": sentiment_score(s),
                    "impressions": 1000
                })

        except:
            continue

    print(f"   → {len(data)} results")
    return data

# ---------------- MAIN ---------------- #

def main():
    print("🚀 Running Hybrid ORM Crawler...")

    all_data = []
    all_data += crawl_reddit()
    all_data += crawl_youtube()
    all_data += crawl_web()

    # Deduplicate
    unique = {item["url"]: item for item in all_data}.values()
    all_data = list(unique)

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
