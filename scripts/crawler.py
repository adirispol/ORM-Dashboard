import os
import json
import time
import requests
import snscrape.modules.reddit as sreddit
from datetime import datetime, timezone
from collections import Counter

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

BRAND_QUERIES = [
    "Polaris School of Technology",
    "Polaris Campus",
    "Polaris BTech AI",
    "Polaris",
    "PST"
]

MAX_TOTAL = 300

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

def score(s):
    return {"positive": 1, "neutral": 0, "negative": -1}[s]

# ---------------- REDDIT (SNSCRAPE) ---------------- #

def crawl_reddit():
    print("🟠 Reddit via snscrape...")
    data = []

    for query in BRAND_QUERIES:
        try:
            scraper = sreddit.RedditSearchScraper(query)

            for i, post in enumerate(scraper.get_items()):
                if i > 50:
                    break

                text = f"{post.title} {post.selftext}"
                s = sentiment(text)

                data.append({
                    "platform": "reddit",
                    "text": text[:200],
                    "url": post.url,
                    "date": post.date.isoformat(),
                    "sentiment": s,
                    "score": score(s),
                    "impressions": post.score * 20 if post.score else 100
                })

                if len(data) >= MAX_TOTAL:
                    return data

        except Exception as e:
            print("Reddit error:", e)

    print(f"   → {len(data)} posts")
    return data

# ---------------- YOUTUBE ---------------- #

def fetch_comments(video_id):
    comments = []

    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": YOUTUBE_API_KEY,
        "maxResults": 10
    }

    try:
        res = requests.get(url, params=params)
        if res.status_code != 200:
            return comments

        for item in res.json().get("items", []):
            c = item["snippet"]["topLevelComment"]["snippet"]
            text = c["textDisplay"]
            s = sentiment(text)

            comments.append({
                "platform": "youtube_comment",
                "text": text[:200],
                "url": f"https://youtube.com/watch?v={video_id}",
                "date": c["publishedAt"],
                "sentiment": s,
                "score": score(s),
                "impressions": 50
            })
    except:
        pass

    return comments


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
                "maxResults": 10,
                "type": "video"
            }

            res = requests.get(url, params=params)
            if res.status_code != 200:
                continue

            for item in res.json().get("items", []):
                vid = item["id"]["videoId"]
                title = item["snippet"]["title"]
                s = sentiment(title)

                data.append({
                    "platform": "youtube",
                    "text": title,
                    "url": f"https://youtube.com/watch?v={vid}",
                    "date": item["snippet"]["publishedAt"],
                    "sentiment": s,
                    "score": score(s),
                    "impressions": 5000
                })

                data += fetch_comments(vid)

                if len(data) >= MAX_TOTAL:
                    return data

        except:
            continue

    print(f"   → {len(data)} items")
    return data

# ---------------- WEB ---------------- #

def crawl_web():
    print("🌐 Web (Serper)...")
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

            for r in res.json().get("organic", [])[:10]:
                link = r.get("link", "")
                text = r.get("title", "")
                s = sentiment(text)

                platform = "web"
                if "quora.com" in link:
                    platform = "quora"
                elif "medium.com" in link:
                    platform = "medium"

                data.append({
                    "platform": platform,
                    "text": text,
                    "url": link,
                    "date": now(),
                    "sentiment": s,
                    "score": score(s),
                    "impressions": 1000
                })

                if len(data) >= MAX_TOTAL:
                    return data

        except:
            continue

    print(f"   → {len(data)} results")
    return data

# ---------------- MAIN ---------------- #

def main():
    print("🚀 Running ORM crawler...")

    all_data = []
    all_data += crawl_reddit()
    all_data += crawl_youtube()
    all_data += crawl_web()

    # Deduplicate
    unique = {x["url"] + x["text"]: x for x in all_data}
    all_data = list(unique.values())

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

    print("✅ DONE")
    print("Mentions:", total)

if __name__ == "__main__":
    main()
