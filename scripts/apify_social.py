"""
Polaris ORM — Apify Social Crawler
Writes: data/social.json
Platforms: LinkedIn, Twitter/X, Instagram, Facebook
Run: python scripts/apify_social.py
Triggered by: .github/workflows/apify-social-crawler.yml
"""

import os, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_BASE  = "https://api.apify.com/v2"

BRAND_KEYWORD = "Polaris School of Technology"
BRAND_TERMS   = ["polaris", "polariscampus", "polaris school of technology", "pst bengaluru", "polaris btech"]
COMPETITORS   = ["scaler school of technology", "newton school", "upgrad", "great learning"]

# -------------------------------------------------------
# ACTOR IDs — verified working as of April 2025
# -------------------------------------------------------
ACTORS = {
    "linkedin":  "curious_coder~linkedin-post-search-scraper",
    "twitter":   "apidojo~tweet-scraper",
    "instagram": "apify~instagram-scraper",
    "facebook":  "apify~facebook-pages-scraper",
}

MAX_ITEMS = {
    "linkedin":  25,
    "twitter":   50,
    "instagram": 20,
    "facebook":  20,
}

# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------
def now():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]   {msg}", flush=True)

def sentiment(text):
    t = text.lower()
    neg = ["scam","fake","bad","worst","fraud","cheating","avoid","terrible","useless",
           "disappointed","overrated","waste","horrible","pathetic"]
    pos = ["good","great","best","awesome","excellent","top","recommended","proud",
           "brilliant","selected","placement","placed","got into","joined","accepted",
           "offer","gsoc","lxf","c4gt","amazing","love","happy","congrats"]
    if any(w in t for w in neg): return "negative"
    if any(w in t for w in pos): return "positive"
    return "neutral"

def tags_polaris(text):
    return "Yes" if any(k in text.lower() for k in BRAND_TERMS) else "No"

def check_competitor(text):
    found = [c.title() for c in COMPETITORS if c in text.lower()]
    return ", ".join(found) if found else "None"

def apify_request(method, path, body=None):
    url = f"{APIFY_BASE}{path}?token={APIFY_TOKEN}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if body else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP Error {e.code}: {e.reason} | {body_text[:300]}")

def run_actor(actor_id, payload, label=""):
    log(f"Starting {label} actor: {actor_id}")
    res = apify_request("POST", f"/acts/{actor_id}/runs", payload)
    run_id = res["data"]["id"]
    log(f"  Run ID: {run_id}")

    # Poll for completion — max 5 minutes
    for i in range(75):
        time.sleep(4)
        try:
            status_data = apify_request("GET", f"/actor-runs/{run_id}")
            status = status_data["data"]["status"]
            if i % 5 == 0:
                log(f"  {label} status: {status} ({i*4}s elapsed)")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
        except Exception as e:
            log(f"  Status check error: {e}")
            continue

    if status != "SUCCEEDED":
        log(f"  {label} ended with: {status}")
        return []

    try:
        items = apify_request("GET", f"/actor-runs/{run_id}/dataset/items")
        count = len(items) if isinstance(items, list) else 0
        log(f"  {label}: {count} raw items retrieved")
        return items if isinstance(items, list) else []
    except Exception as e:
        log(f"  Failed to fetch dataset: {e}")
        return []

# -------------------------------------------------------
# DATE PARSER
# -------------------------------------------------------
def parse_dt(item):
    for key in ("posted_at","createdAt","created_at","postedAtISO","publishedAt",
                "date","timestamp","time","created_time","updatedAt"):
        val = item.get(key)
        if not val:
            continue
        if isinstance(val, dict):
            ts = val.get("timestamp")
            if ts:
                try:
                    t = int(ts)
                    if t > 1e12: t //= 1000
                    return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
                except:
                    pass
            continue
        s = str(val).strip()
        if s.isdigit():
            try:
                t = int(s)
                if t > 1e12: t //= 1000
                return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
            except:
                pass
        # Try ISO parse
        try:
            return datetime.fromisoformat(s.replace("Z","+00:00")).isoformat()
        except:
            return s
    return now()

# -------------------------------------------------------
# INGEST — LinkedIn
# Input schema for curious_coder~linkedin-post-search-scraper:
#   searchUrl: LinkedIn search URL
#   count: number of posts
# -------------------------------------------------------
def ingest_linkedin(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        # Author info
        af = item.get("author", {}) or {}
        if not isinstance(af, dict):
            af = {}
        first = af.get("firstName","")
        last  = af.get("lastName","")
        name  = af.get("name","") or f"{first} {last}".strip() or item.get("authorName","Unknown")

        # Stats
        stats   = item.get("stats", {}) or {}
        likes   = int(stats.get("numLikes", item.get("numLikes", item.get("likes", 0))) or 0)
        comments= int(stats.get("numComments", item.get("numComments", 0)) or 0)
        reposts = int(stats.get("numShares", item.get("numShares", item.get("shares", 0))) or 0)

        # URL
        act_id = str(item.get("activity_id","") or item.get("activityId",""))
        url    = str(item.get("post_url","") or item.get("url","") or item.get("postUrl",""))
        if not url and act_id.isdigit():
            url = f"https://www.linkedin.com/feed/update/urn:li:activity:{act_id}/"
        if not url:
            continue

        text = str(item.get("text","") or item.get("commentary",""))
        posts.append({
            "platform":    "linkedin",
            "author":      name,
            "text":        text[:400],
            "url":         url,
            "date":        parse_dt(item),
            "likes":       likes,
            "comments":    comments,
            "reposts":     reposts,
            "impressions": likes * 80,
            "sentiment":   sentiment(text),
            "tags_polaris":tags_polaris(text),
            "competitor":  check_competitor(text),
            "scraped_at":  now(),
        })
    # Deduplicate by URL
    return list({p["url"]: p for p in posts}.values())

# -------------------------------------------------------
# INGEST — Twitter/X
# Input schema for apidojo~tweet-scraper:
#   searchTerms: list of query strings
#   maxItems: number
#   queryType: "Latest" or "Top"
# -------------------------------------------------------
def ingest_twitter(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        ai     = item.get("author", item.get("user", {})) or {}
        if not isinstance(ai, dict): ai = {}
        author = ai.get("name","") or item.get("authorName","Unknown")
        handle = ai.get("userName","") or ai.get("username","") or ai.get("screen_name","")
        if handle and not handle.startswith("@"):
            handle = f"@{handle}"

        likes    = int(item.get("likeCount", item.get("likes", item.get("favorite_count", 0))) or 0)
        replies  = int(item.get("replyCount", item.get("replies", item.get("reply_count", 0))) or 0)
        retweets = int(item.get("retweetCount", item.get("retweets", item.get("retweet_count", 0))) or 0)
        views    = int(item.get("viewCount", item.get("views", 0)) or 0)
        url      = item.get("url","") or item.get("tweetUrl","")
        text     = str(item.get("text","") or item.get("full_text",""))

        if not url:
            continue
        posts.append({
            "platform":    "twitter",
            "author":      f"{author} {handle}".strip(),
            "text":        text[:400],
            "url":         url,
            "date":        parse_dt(item),
            "likes":       likes,
            "comments":    replies,
            "reposts":     retweets,
            "impressions": views or (likes * 35),
            "sentiment":   sentiment(text),
            "tags_polaris":tags_polaris(text),
            "competitor":  check_competitor(text),
            "scraped_at":  now(),
        })
    return list({p["url"]: p for p in posts if p["url"]}.values())

# -------------------------------------------------------
# INGEST — Instagram
# Input schema for apify~instagram-scraper:
#   directUrls: list of hashtag/profile URLs
#   resultsLimit: number
# -------------------------------------------------------
def ingest_instagram(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        username = item.get("ownerUsername","") or item.get("username","") or "unknown"
        caption  = str(item.get("caption","") or item.get("text",""))
        url      = item.get("url","") or ""
        sc       = item.get("shortCode","") or item.get("id","")
        if not url and sc:
            url = f"https://www.instagram.com/p/{sc}/"
        likes = int(item.get("likesCount","0") or item.get("likes", 0) or 0)
        comms = int(item.get("commentsCount","0") or item.get("comments", 0) or 0)

        posts.append({
            "platform":    "instagram",
            "author":      f"@{username}",
            "text":        caption[:400],
            "url":         url,
            "date":        parse_dt(item),
            "likes":       likes,
            "comments":    comms,
            "reposts":     0,
            "impressions": likes * 10,
            "sentiment":   sentiment(caption),
            "tags_polaris":tags_polaris(caption),
            "competitor":  check_competitor(caption),
            "scraped_at":  now(),
        })
    return posts

# -------------------------------------------------------
# INGEST — Facebook
# Input schema for apify~facebook-pages-scraper:
#   startUrls: list of page URLs
#   maxPosts: number
# -------------------------------------------------------
def ingest_facebook(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        user   = item.get("user",{}) or {}
        author = (user.get("name","") if isinstance(user,dict) else str(user)) or item.get("pageName","Unknown")
        text   = str(item.get("text","") or item.get("message","") or item.get("story",""))
        url    = item.get("url","") or item.get("link","")
        likes  = int(item.get("likes","0") or item.get("reactions",0) or 0)
        shares = int(item.get("shares","0") or 0)
        comms  = int(item.get("comments","0") or 0)

        posts.append({
            "platform":    "facebook",
            "author":      author,
            "text":        text[:400],
            "url":         url or "",
            "date":        parse_dt(item),
            "likes":       likes,
            "comments":    comms,
            "reposts":     shares,
            "impressions": likes * 20,
            "sentiment":   sentiment(text),
            "tags_polaris":tags_polaris(text),
            "competitor":  check_competitor(text),
            "scraped_at":  now(),
        })
    return posts

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    if not APIFY_TOKEN:
        log("APIFY_TOKEN not set — exiting")
        return

    log(f"Starting Polaris social crawl — brand: '{BRAND_KEYWORD}'")
    all_social = []

    # LinkedIn
    # curious_coder~linkedin-post-search-scraper needs a LinkedIn search URL
    # We search for posts mentioning Polaris School of Technology
    li_search_url = "https://www.linkedin.com/search/results/content/?keywords=Polaris%20School%20of%20Technology&sortBy=%22date_posted%22"
    try:
        raw = run_actor(ACTORS["linkedin"], {
            "searchUrl": li_search_url,
            "count":     MAX_ITEMS["linkedin"],
        }, "LinkedIn")
        posts = ingest_linkedin(raw)
        log(f"  LinkedIn: {len(posts)} posts ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  LinkedIn error: {e}")
    time.sleep(5)

    # Twitter/X — apidojo~tweet-scraper
    try:
        raw = run_actor(ACTORS["twitter"], {
            "searchTerms": [
                BRAND_KEYWORD,
                "#polariscampus",
                "Polaris BTech Bangalore",
                "PST Bengaluru",
            ],
            "maxItems":  MAX_ITEMS["twitter"],
            "queryType": "Latest",
        }, "Twitter")
        posts = ingest_twitter(raw)
        log(f"  Twitter: {len(posts)} tweets ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  Twitter error: {e}")
    time.sleep(5)

    # Instagram — apify~instagram-scraper
    # Uses hashtag URLs or profile URLs
    try:
        raw = run_actor(ACTORS["instagram"], {
            "directUrls": [
                "https://www.instagram.com/explore/tags/polariscampus/",
                "https://www.instagram.com/explore/tags/polarisschooloftechnology/",
            ],
            "resultsLimit": MAX_ITEMS["instagram"],
        }, "Instagram")
        posts = ingest_instagram(raw)
        log(f"  Instagram: {len(posts)} posts ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  Instagram error: {e}")
    time.sleep(5)

    # Facebook — apify~facebook-pages-scraper
    # Needs the Polaris Facebook page URL — update this if you have the real URL
    POLARIS_FB_PAGE = os.getenv("POLARIS_FB_PAGE", "https://www.facebook.com/polariscampus")
    try:
        raw = run_actor(ACTORS["facebook"], {
            "startUrls": [{"url": POLARIS_FB_PAGE}],
            "maxPosts":  MAX_ITEMS["facebook"],
        }, "Facebook")
        posts = ingest_facebook(raw)
        log(f"  Facebook: {len(posts)} posts ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  Facebook error: {e}")

    # Write output
    os.makedirs("data", exist_ok=True)
    out_path = "data/social.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_social, f, indent=2, ensure_ascii=False)

    log(f"social.json written — {len(all_social)} total records")
    log(f"  LinkedIn:  {sum(1 for p in all_social if p['platform']=='linkedin')}")
    log(f"  Twitter:   {sum(1 for p in all_social if p['platform']=='twitter')}")
    log(f"  Instagram: {sum(1 for p in all_social if p['platform']=='instagram')}")
    log(f"  Facebook:  {sum(1 for p in all_social if p['platform']=='facebook')}")

if __name__ == "__main__":
    main()
