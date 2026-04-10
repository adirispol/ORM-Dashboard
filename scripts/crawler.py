import os
import json
import time
import requests
from datetime import datetime, timezone
from collections import Counter

# Optional APIs
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Reddit (PRAW)
import praw

# ---------------- CONFIG ---------------- #

BRAND_QUERIES = [
    "Polaris School of Technology",
    "Polaris Campus",
    "Polaris BTech AI"
    "Polaris"
    "PST"
]

MAX_RESULTS = 25

# ---------------- HELPERS ---------------- #

def now():
    return datetime.now(timezone.utc).isoformat()

def safe_request(url, params=None):
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code != 200:
            return None
        return res.json()
    except:
        return None

def sentiment(text):
    text = text.lower()
    if any(x in text for x in ["bad", "worst", "fake", "scam"]):
        return "negative"
    if any(x in text for x in ["good", "great", "best", "awesome"]):
        return "positive"
    return "neutral"

def sentiment_score(s):
    return {"positive": 1, "neutral": 0, "negative": -1}[s]

# ---------------- REDDIT ---------------- #

def crawl_reddit():
    print("🟠 Reddit...")
    data = []

    try:
        reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent="orm-dashboard"
        )

        for query in BRAND_QUERIES:
            for post in reddit.subreddit("all").search(query, limit=MAX_RESULTS):
                text = f"{post.title} {post.selftext}"
                s = sentiment(text)

                data.append({
                    "platform": "reddit",
                    "text": text[:200],
                    "url": f"https://reddit.com{post.permalink}",
                    "date": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                    "sentiment": s,
                    "score": sentiment_score(s),
                    "impressions": post.score * 20
                })

    except Exception as e:
        print("Reddit error:", e)

    print(f"   → {len(data)} posts")
    return data

# ---------------- YOUTUBE ---------------- #

def crawl_youtube():
    print("▶ YouTube...")
    data = []

    if not YOUTUBE_API_KEY:
        return data

    try:
        for query in BRAND_QUERIES:
            search_url = "https://www.googleapis.com/youtube/v3/search"

            params = {
                "part": "snippet",
                "q": query,
                "key": YOUTUBE_API_KEY,
                "maxResults": 10,
                "type": "video"
            }

            res = safe_request(search_url, params)
            if not res:
                continue

            for item in res.get("items", []):
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

    except Exception as e:
        print("YouTube error:", e)

    print(f"   → {len(data)} videos")
    return data

# ---------------- WEB (SERPER) ---------------- #

def crawl_web():
    print("🌐 Web...")
    data = []

    if not SERPER_API_KEY:
        return data

    try:
        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": SERPER_API_KEY}

        for query in BRAND_QUERIES:
            res = requests.post(url, json={"q": query}, headers=headers)

            if res.status_code != 200:
                continue

            results = res.json().get("organic", [])[:10]

            for r in results:
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

    except Exception as e:
        print("Web error:", e)

    print(f"   → {len(data)} results")
    return data

# ---------------- MAIN ---------------- #

def main():
    print("🚀 Running ORM crawler...")

    all_data = []
    all_data += crawl_reddit()
    all_data += crawl_youtube()
    all_data += crawl_web()

    # ---------------- SUMMARY ---------------- #

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

    # ---------------- SAVE ---------------- #

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
