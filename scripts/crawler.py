"""
Polaris ORM — Apify Social Crawler
Writes: data/social.json
Platforms: LinkedIn, Twitter, Instagram, Facebook
Run: python scripts/apify_social.py
Triggered by: .github/workflows/apify-social-crawler.yml
"""

import os, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_BASE  = "https://api.apify.com/v2"

BRAND_KEYWORD = "Polaris School of Technology"
BRAND_TERMS   = ["polaris", "polariscampus", "polaris school of technology", "pst bengaluru"]
COMPETITORS   = ["scaler school of technology", "newton school", "upgrad", "great learning"]

# Apify actor IDs — these are public community actors
ACTORS = {
    "linkedin":  "supreme_coder~linkedin-post",
    "twitter":   "xquik~x-tweet-scraper",
    "instagram": "apify~instagram-hashtag-scraper",
    "facebook":  "apify~facebook-posts-scraper",
}

MAX_ITEMS = {
    "linkedin":  30,
    "twitter":   50,
    "instagram": 20,
    "facebook":  20,
}

# ─── HELPERS ───────────────────────────────────────────

def now():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def sentiment(text):
    t = text.lower()
    neg_w = ["scam","fake","bad","worst","fraud","cheating","avoid","terrible","useless","disappointed","overrated"]
    pos_w = ["good","great","best","awesome","excellent","top","recommended","proud","brilliant","selected",
             "placement","placed","got into","joined","accepted","offer","gsoc","lxf","c4gt"]
    if any(w in t for w in neg_w): return "negative"
    if any(w in t for w in pos_w): return "positive"
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
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

def run_actor(actor_id, payload, label=""):
    log(f"▶ Starting {label} actor…")
    res = apify_request("POST", f"/acts/{actor_id}/runs", payload)
    run_id = res["data"]["id"]
    log(f"  Run ID: {run_id}")

    for i in range(80):
        time.sleep(4)
        status_data = apify_request("GET", f"/actor-runs/{run_id}")
        status = status_data["data"]["status"]
        log(f"  {label} status: {status} ({i*4}s)")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break

    if status != "SUCCEEDED":
        log(f"  ❌ {label} ended with: {status}")
        return []

    items = apify_request("GET", f"/actor-runs/{run_id}/dataset/items")
    log(f"  ✅ {label}: {len(items)} items")
    return items if isinstance(items, list) else []

# ─── INGEST ────────────────────────────────────────────

def parse_dt(item):
    for key in ("posted_at","createdAt","created_at","postedAtISO","publishedAt","date","timestamp"):
        val = item.get(key)
        if not val: continue
        if isinstance(val, dict):
            ts = val.get("timestamp")
            if ts:
                try: return datetime.fromtimestamp(int(ts)//1000 if int(ts)>1e12 else int(ts), tz=timezone.utc).isoformat()
                except: pass
            continue
        s = str(val).strip()
        if s.isdigit():
            ts = int(s)
            if ts > 1e12: ts //= 1000
            try: return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except: pass
        return s
    return now()

def ingest_linkedin(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        af = item.get("author", {}) or {}
        if not isinstance(af, dict): af = {}
        author   = (af.get("name") or f"{af.get('firstName','')} {af.get('lastName','')}").strip() or item.get("authorName","Unknown")
        stats    = item.get("stats", {}) or {}
        likes    = int(stats.get("total_reactions", item.get("likes", item.get("numLikes", 0))) or 0)
        comments = int(stats.get("numComments",  item.get("numComments", 0)) or 0)
        reposts  = int(stats.get("numShares",    item.get("numShares", 0)) or 0)
        act_id   = str(item.get("activity_id",""))
        url      = str(item.get("post_url","") or item.get("url","") or item.get("postUrl",""))
        if not url and act_id.isdigit():
            url = f"https://www.linkedin.com/feed/update/urn:li:activity:{act_id}/"
        if not url: continue
        text = str(item.get("text",""))
        posts.append({
            "platform":      "linkedin",
            "author":        author,
            "text":          text[:400],
            "url":           url,
            "date":          parse_dt(item),
            "likes":         likes,
            "comments":      comments,
            "reposts":       reposts,
            "impressions":   likes * 80,
            "sentiment":     sentiment(text),
            "tags_polaris":  tags_polaris(text),
            "competitor":    check_competitor(text),
            "scraped_at":    now(),
        })
    return list({p["url"]:p for p in posts}.values())

def ingest_twitter(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        ai = item.get("author", item.get("user", {})) or {}
        if not isinstance(ai, dict): ai = {}
        author = ai.get("name","") or item.get("authorName","Unknown")
        handle = ai.get("username","") or ai.get("screen_name","") or ""
        if handle and not handle.startswith("@"): handle = f"@{handle}"
        likes    = int(item.get("likes") or item.get("likeCount") or item.get("favorite_count") or 0)
        replies  = int(item.get("replies") or item.get("reply_count") or 0)
        retweets = int(item.get("retweets") or item.get("retweet_count") or 0)
        url  = item.get("url","") or item.get("tweetUrl","")
        text = str(item.get("text","") or item.get("full_text",""))
        if not url: continue
        posts.append({
            "platform":      "twitter",
            "author":        f"{author} {handle}".strip(),
            "text":          text[:400],
            "url":           url,
            "date":          parse_dt(item),
            "likes":         likes,
            "replies":       replies,
            "retweets":      retweets,
            "impressions":   likes * 35,
            "sentiment":     sentiment(text),
            "tags_polaris":  tags_polaris(text),
            "competitor":    check_competitor(text),
            "scraped_at":    now(),
        })
    return list({p["url"]:p for p in posts if p["url"]}.values())

def ingest_instagram(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        username = item.get("ownerUsername","") or item.get("username","") or "unknown"
        caption  = str(item.get("caption","") or item.get("text",""))
        url      = item.get("url","") or item.get("shortCode","")
        if url and not url.startswith("http"): url = f"https://instagram.com/p/{url}"
        likes    = int(item.get("likesCount","0") or item.get("likes",0) or 0)
        comms    = int(item.get("commentsCount","0") or item.get("comments",0) or 0)
        posts.append({
            "platform":      "instagram",
            "author":        f"@{username}",
            "text":          caption[:400],
            "url":           url or "",
            "date":          parse_dt(item),
            "likes":         likes,
            "comments":      comms,
            "impressions":   likes * 10,
            "sentiment":     sentiment(caption),
            "tags_polaris":  tags_polaris(caption),
            "competitor":    check_competitor(caption),
            "scraped_at":    now(),
        })
    return posts

def ingest_facebook(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        user   = item.get("user",{}) or {}
        author = (user.get("name","") if isinstance(user,dict) else str(user)) or item.get("pageName","Unknown")
        text   = str(item.get("text","") or item.get("message",""))
        url    = item.get("url","") or item.get("link","")
        likes  = int(item.get("likes","0") or item.get("reactions",0) or 0)
        shares = int(item.get("shares","0") or 0)
        comms  = int(item.get("comments","0") or 0)
        posts.append({
            "platform":      "facebook",
            "author":        author,
            "text":          text[:400],
            "url":           url or "",
            "date":          parse_dt(item),
            "likes":         likes,
            "shares":        shares,
            "comments":      comms,
            "impressions":   likes * 20,
            "sentiment":     sentiment(text),
            "tags_polaris":  tags_polaris(text),
            "competitor":    check_competitor(text),
            "scraped_at":    now(),
        })
    return posts

# ─── MAIN ─────────────────────────────────────────────

def main():
    if not APIFY_TOKEN:
        log("❌ APIFY_TOKEN not set — exiting")
        return

    all_social = []

    # LinkedIn
    try:
        raw = run_actor(ACTORS["linkedin"], {
            "query": BRAND_KEYWORD,
            "maxResults": MAX_ITEMS["linkedin"],
            "datePosted": "pastMonth",
        }, "LinkedIn")
        posts = ingest_linkedin(raw)
        log(f"  LinkedIn: {len(posts)} posts ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  ❌ LinkedIn error: {e}")

    time.sleep(3)

    # Twitter
    try:
        raw = run_actor(ACTORS["twitter"], {
            "searchTerms": [BRAND_KEYWORD, "#polariscampus", "PST Bangalore"],
            "maxItems": MAX_ITEMS["twitter"],
            "queryType": "Latest",
        }, "Twitter")
        posts = ingest_twitter(raw)
        log(f"  Twitter: {len(posts)} tweets ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  ❌ Twitter error: {e}")

    time.sleep(3)

    # Instagram
    try:
        raw = run_actor(ACTORS["instagram"], {
            "hashtags": ["polariscampus", "polarisschooloftechnology"],
            "resultsLimit": MAX_ITEMS["instagram"],
        }, "Instagram")
        posts = ingest_instagram(raw)
        log(f"  Instagram: {len(posts)} posts ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  ❌ Instagram error: {e}")

    time.sleep(3)

    # Facebook
    try:
        raw = run_actor(ACTORS["facebook"], {
            "query": BRAND_KEYWORD,
            "maxPosts": MAX_ITEMS["facebook"],
        }, "Facebook")
        posts = ingest_facebook(raw)
        log(f"  Facebook: {len(posts)} posts ingested")
        all_social.extend(posts)
    except Exception as e:
        log(f"  ❌ Facebook error: {e}")

    # Write output
    os.makedirs("data", exist_ok=True)
    with open("data/social.json", "w") as f:
        json.dump(all_social, f, indent=2, ensure_ascii=False)

    log(f"✅ social.json written — {len(all_social)} total records")
    log(f"   LinkedIn: {sum(1 for p in all_social if p['platform']=='linkedin')}")
    log(f"   Twitter:  {sum(1 for p in all_social if p['platform']=='twitter')}")
    log(f"   Instagram:{sum(1 for p in all_social if p['platform']=='instagram')}")
    log(f"   Facebook: {sum(1 for p in all_social if p['platform']=='facebook')}")

if __name__ == "__main__":
    main()
