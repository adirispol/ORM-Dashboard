"""
Polaris ORM — Apify Social Crawler v2
Fixes:
  - LinkedIn: switched to working actor ID + hashtag/keyword queries
  - Instagram: scrapes #polariscampus + @polariscampus
  - History: appends every run to data/history.json (never overwrites)
  - Impressions: labelled as estimated, real engagement used where available
  - VibeCon / Lyzr / GSoC hashtags now included in all platform queries
Writes: data/social.json, data/history.json
"""

import os, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_BASE  = "https://api.apify.com/v2"

# ── Brand identifiers ──────────────────────────────────────────────────────
BRAND_KEYWORD = "Polaris School of Technology"
BRAND_TERMS   = [
    "polaris school of technology", "polariscampus", "polaris btech",
    "pst bengaluru", "polaris bangalore", "@polaris_code",
]

# ── Event / Campaign hashtags (add new events here) ───────────────────────
EVENT_HASHTAGS = [
    "#VibeCon", "#VibeCon2025", "#VibeCon2026", "VibeCon",
    "#LyzrAI", "Lyzr Agentathon", "Agentathon",
    "#GSoC2025", "GSoC Polaris",
    "#PolarisSchoolOfTechnology", "#polariscampus",
]

COMPETITORS = [
    "scaler school of technology", "newton school of technology",
    "upgrad", "great learning",
]

# ── Actor IDs (verified April 2025) ───────────────────────────────────────
# LinkedIn: using 2SyF0bVxmgGr8IVCZ (Linkedin Post Searcher) — more reliable
# than curious_coder actor which requires cookies
ACTORS = {
    "linkedin":   "2SyF0bVxmgGr8IVCZ",
    "twitter":    "apidojo~tweet-scraper",
    "instagram":  "apify~instagram-scraper",
    "facebook":   "apify~facebook-pages-scraper",
}

MAX_ITEMS = {
    "linkedin":   50,   # upped from 25 — VibeCon had 200+ posts
    "twitter":    80,
    "instagram":  30,
    "facebook":   20,
}

# ── Helpers ────────────────────────────────────────────────────────────────
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]  {msg}", flush=True)

def sentiment(text):
    t = (text or "").lower()
    neg_words = ["scam","fake","bad","worst","fraud","cheating","avoid",
                 "terrible","useless","disappointed","overrated","waste",
                 "horrible","pathetic","misleading","wrong","lied"]
    pos_words = ["good","great","best","awesome","excellent","top",
                 "recommended","proud","brilliant","selected","placement",
                 "placed","got into","joined","accepted","offer","gsoc",
                 "amazing","love","incredible","fantastic","congrats",
                 "winner","built","shipped","demo","hackathon win",
                 "vibecon","lyzr","agentathon","impressed","productive"]
    if any(w in t for w in neg_words): return "negative"
    if any(w in t for w in pos_words): return "positive"
    return "neutral"

def is_polaris(text):
    t = (text or "").lower()
    return any(term in t for term in BRAND_TERMS + [ht.lower().lstrip("#") for ht in EVENT_HASHTAGS])

def detect_campaign(text):
    t = (text or "").lower()
    if any(k in t for k in ["vibecon","vibe con"]): return "VibeCon"
    if any(k in t for k in ["lyzr","agentathon"]): return "Lyzr/Agentathon"
    if "gsoc" in t or "google summer of code" in t: return "GSoC 2025"
    if any(k in t for k in ["admission","fee","scholarship","join","pat exam","jee"]): return "Admissions"
    if any(k in t for k in ["scaler","newton","nxtwave","intellipaat","upgrad"]): return "vs Competitors"
    return None

def check_competitor(text):
    t = (text or "").lower()
    for c in COMPETITORS:
        if c in t: return c.title()
    return "None"

# ── Apify API wrapper ──────────────────────────────────────────────────────
def apify_request(method, path, body=None):
    url = f"{APIFY_BASE}{path}?token={APIFY_TOKEN}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  API error {method} {path}: {e}")
        return {}

def run_actor(actor_id, payload, label=""):
    log(f"Starting [{label}] actor: {actor_id}")
    res = apify_request("POST", f"/acts/{actor_id}/runs", payload)
    if not res or "data" not in res:
        log(f"  [{label}] failed to start — no data in response")
        return []
    run_id = res["data"]["id"]
    log(f"  [{label}] Run ID: {run_id}")

    # Poll for completion — max 6 minutes
    status = "RUNNING"
    for i in range(90):
        time.sleep(4)
        try:
            sd = apify_request("GET", f"/actor-runs/{run_id}")
            status = (sd.get("data") or {}).get("status", "UNKNOWN")
            if i % 5 == 0:
                log(f"  [{label}] status: {status} ({i*4}s)")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
        except Exception as e:
            log(f"  [{label}] poll error: {e}")

    if status != "SUCCEEDED":
        log(f"  [{label}] ended with status: {status} — returning []")
        return []

    try:
        items = apify_request("GET", f"/actor-runs/{run_id}/dataset/items")
        count = len(items) if isinstance(items, list) else 0
        log(f"  [{label}] fetched {count} items")
        return items if isinstance(items, list) else []
    except Exception as e:
        log(f"  [{label}] dataset fetch error: {e}")
        return []

# ── Date parser ────────────────────────────────────────────────────────────
def parse_dt(item):
    for key in ("posted_at","createdAt","created_at","postedAtISO","publishedAt",
                "date","timestamp","time","created_time","updatedAt","taken_at_timestamp"):
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
                except: pass
            continue
        s = str(val).strip()
        if s.isdigit():
            try:
                t = int(s)
                if t > 1e12: t //= 1000
                return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
            except: pass
        try:
            return datetime.fromisoformat(s.replace("Z","+00:00")).isoformat()
        except: pass
    return now_iso()

# ── Ingest: LinkedIn ───────────────────────────────────────────────────────
def ingest_linkedin(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        # Author
        af = item.get("author", {}) or {}
        if not isinstance(af, dict): af = {}
        name = (af.get("name") or
                f"{af.get('firstName','')} {af.get('lastName','')}".strip() or
                item.get("authorName","Unknown"))
        # Text
        text = (item.get("text") or item.get("content") or
                item.get("commentary") or item.get("description") or "")
        if not text: continue
        # Stats — LinkedIn Post Searcher uses different field names
        stats   = item.get("stats", {}) or item.get("socialActivityCountsInsight", {}) or {}
        likes   = int(item.get("numLikes") or item.get("likes") or
                      stats.get("numLikes") or stats.get("likeCount") or
                      item.get("reactionCount") or 0)
        comments= int(item.get("numComments") or item.get("comments") or
                      stats.get("numComments") or stats.get("commentCount") or 0)
        reposts = int(item.get("numShares") or item.get("shares") or
                      stats.get("numShares") or stats.get("repostCount") or 0)
        # Impressions: LinkedIn doesn't expose them via Apify
        # Use engagement proxy: (likes + comments*3 + reposts*5) * 50 as estimate
        engagement = likes + comments * 3 + reposts * 5
        impressions_est = max(engagement * 50, likes * 10, 100)

        post_url = (item.get("url") or item.get("postUrl") or
                    item.get("shareUrl") or item.get("link") or "")

        posts.append({
            "platform":       "linkedin",
            "author":         name,
            "author_url":     af.get("profileUrl", af.get("url", "")),
            "text":           text.strip()[:1000],
            "url":            post_url,
            "date":           parse_dt(item),
            "likes":          likes,
            "comments":       comments,
            "reposts":        reposts,
            "impressions":    impressions_est,
            "impressions_type": "estimated",
            "sentiment":      sentiment(text),
            "campaign":       detect_campaign(text),
            "tags_polaris":   "Yes" if is_polaris(text) else "No",
            "competitor":     check_competitor(text),
            "scraped_at":     now_iso(),
        })
    log(f"  LinkedIn: ingested {len(posts)} posts")
    return posts

# ── Ingest: Twitter ────────────────────────────────────────────────────────
def ingest_twitter(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        text = (item.get("full_text") or item.get("text") or
                item.get("rawContent") or item.get("content") or "")
        if not text: continue
        author = (item.get("user", {}) or {}).get("name") or item.get("author","")
        likes   = int(item.get("likeCount") or item.get("favorite_count") or
                      item.get("likes") or 0)
        reposts = int(item.get("retweetCount") or item.get("retweet_count") or
                      item.get("retweets") or 0)
        comments= int(item.get("replyCount") or item.get("reply_count") or 0)
        # Twitter impressions: real field if available, else estimate
        impr_raw = item.get("viewCount") or item.get("impressionCount") or 0
        impressions = int(impr_raw) if impr_raw else max((likes+reposts+comments)*30, 10)
        impr_type   = "real" if impr_raw else "estimated"

        posts.append({
            "platform":         "twitter",
            "author":           author,
            "text":             text.strip()[:1000],
            "url":              item.get("url") or item.get("twitterUrl") or "",
            "date":             parse_dt(item),
            "likes":            likes,
            "comments":         comments,
            "reposts":          reposts,
            "impressions":      impressions,
            "impressions_type": impr_type,
            "sentiment":        sentiment(text),
            "campaign":         detect_campaign(text),
            "tags_polaris":     "Yes" if is_polaris(text) else "No",
            "competitor":       check_competitor(text),
            "scraped_at":       now_iso(),
        })
    log(f"  Twitter: ingested {len(posts)} posts")
    return posts

# ── Ingest: Instagram ──────────────────────────────────────────────────────
def ingest_instagram(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        caption = (item.get("caption") or item.get("alt") or
                   item.get("accessibility_caption") or "")
        likes   = int(item.get("likesCount") or item.get("likes_count") or
                      item.get("edge_media_preview_like",{}).get("count",0) or 0)
        comments= int(item.get("commentsCount") or item.get("comments_count") or 0)
        impr    = int(item.get("videoViewCount") or item.get("videoPlayCount") or 0)
        impressions = impr if impr else max((likes + comments*5) * 20, 50)

        posts.append({
            "platform":         "instagram",
            "author":           item.get("ownerUsername") or item.get("username") or "",
            "text":             caption.strip()[:1000],
            "url":              item.get("url") or item.get("link") or "",
            "date":             parse_dt(item),
            "likes":            likes,
            "comments":         comments,
            "reposts":          0,
            "impressions":      impressions,
            "impressions_type": "real" if impr else "estimated",
            "sentiment":        sentiment(caption),
            "campaign":         detect_campaign(caption),
            "tags_polaris":     "Yes" if is_polaris(caption) else "No",
            "competitor":       check_competitor(caption),
            "scraped_at":       now_iso(),
        })
    log(f"  Instagram: ingested {len(posts)} posts")
    return posts

# ── Ingest: Facebook ───────────────────────────────────────────────────────
def ingest_facebook(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        text = (item.get("text") or item.get("story") or
                item.get("message") or "")
        likes   = int(item.get("likes") or item.get("likesCount") or 0)
        comments= int(item.get("comments") or item.get("commentsCount") or 0)
        shares  = int(item.get("shares") or item.get("sharesCount") or 0)
        impressions = max((likes + comments*3 + shares*5) * 40, 50)

        posts.append({
            "platform":         "facebook",
            "author":           item.get("pageName") or item.get("authorName") or "",
            "text":             text.strip()[:1000],
            "url":              item.get("url") or item.get("postUrl") or "",
            "date":             parse_dt(item),
            "likes":            likes,
            "comments":         comments,
            "reposts":          shares,
            "impressions":      impressions,
            "impressions_type": "estimated",
            "sentiment":        sentiment(text),
            "campaign":         detect_campaign(text),
            "tags_polaris":     "Yes" if is_polaris(text) else "No",
            "competitor":       check_competitor(text),
            "scraped_at":       now_iso(),
        })
    log(f"  Facebook: ingested {len(posts)} posts")
    return posts

# ── History: append to data/history.json ──────────────────────────────────
def append_history(all_posts, path="data/history.json"):
    """
    Adds a dated snapshot to history.json so we never lose past data.
    Each entry: { date, total, by_platform, by_campaign, impressions, posts }
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Load existing history
    existing = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception as e:
            log(f"  History read error: {e} — starting fresh")
            existing = []

    # Remove today's entry if it already exists (idempotent re-runs)
    existing = [e for e in existing if e.get("date") != today]

    # Build today's snapshot
    by_platform = {}
    by_campaign  = {}
    total_impr   = 0
    total_likes  = 0
    for post in all_posts:
        p = post.get("platform","unknown")
        by_platform[p] = by_platform.get(p, 0) + 1
        c = post.get("campaign") or "General"
        by_campaign[c] = by_campaign.get(c, 0) + 1
        total_impr  += post.get("impressions", 0)
        total_likes += post.get("likes", 0)

    snapshot = {
        "date":        today,
        "total_posts": len(all_posts),
        "by_platform": by_platform,
        "by_campaign": by_campaign,
        "impressions": total_impr,
        "likes":       total_likes,
        "scraped_at":  now_iso(),
        # Store last 30 posts per day for drill-down (keep file manageable)
        "posts_sample": all_posts[:30],
    }
    existing.append(snapshot)

    # Keep max 365 days
    existing = sorted(existing, key=lambda x: x["date"])[-365:]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    log(f"  History updated: {len(existing)} days recorded, today: {len(all_posts)} posts")

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    if not APIFY_TOKEN:
        log("ERROR: APIFY_TOKEN not set — check GitHub Secrets")
        return

    all_posts = []

    # ── LinkedIn ──────────────────────────────────────────────────────────
    # Actor 2SyF0bVxmgGr8IVCZ = LinkedIn Post Searcher
    # Queries: brand name + all event hashtags
    log("=== LinkedIn ===")
    li_queries = [
        BRAND_KEYWORD,
        "#PolarisSchoolOfTechnology",
        "#polariscampus",
        "#VibeCon",
        "#VibeCon2025",
        "#VibeCon2026",
        "VibeCon Polaris",
        "#LyzrAI Polaris",
        "Agentathon Polaris",
        "GSoC Polaris",
    ]
    for q in li_queries:
        raw = run_actor(ACTORS["linkedin"], {
            "searchQuery":     q,
            "maxResults":      MAX_ITEMS["linkedin"],
            "onlyWithLinkedIn": False,
        }, label=f"LinkedIn:{q[:30]}")
        all_posts.extend(ingest_linkedin(raw))
        time.sleep(2)

    li_count_before = len(all_posts)
    # Deduplicate LinkedIn by URL
    seen_urls = set()
    deduped = []
    for p in all_posts:
        key = p.get("url") or p.get("text","")[:80]
        if key not in seen_urls:
            seen_urls.add(key)
            deduped.append(p)
    all_posts = deduped
    log(f"  LinkedIn after dedup: {len(all_posts)} (was {li_count_before})")

    # ── Twitter / X ───────────────────────────────────────────────────────
    log("=== Twitter / X ===")
    tw_queries = [
        f'"{BRAND_KEYWORD}"',
        "#polariscampus",
        "#VibeCon",
        "#VibeCon2025 polaris",
        "Agentathon polaris",
        "@polaris_code",
    ]
    for q in tw_queries:
        raw = run_actor(ACTORS["twitter"], {
            "searchTerms":       [q],
            "maxTweets":         MAX_ITEMS["twitter"],
            "addUserInfo":       True,
            "scrapeTweetReplies": False,
        }, label=f"Twitter:{q[:25]}")
        all_posts.extend(ingest_twitter(raw))
        time.sleep(2)

    # ── Instagram ─────────────────────────────────────────────────────────
    log("=== Instagram ===")
    raw = run_actor(ACTORS["instagram"], {
        "directUrls": [
            "https://www.instagram.com/explore/tags/polariscampus/",
            "https://www.instagram.com/explore/tags/polarisschooloftechnology/",
            "https://www.instagram.com/explore/tags/vibecon/",
            "https://www.instagram.com/polariscampus/",
        ],
        "resultsType": "posts",
        "resultsLimit": MAX_ITEMS["instagram"],
    }, label="Instagram")
    all_posts.extend(ingest_instagram(raw))
    time.sleep(2)

    # ── Facebook ──────────────────────────────────────────────────────────
    log("=== Facebook ===")
    raw = run_actor(ACTORS["facebook"], {
        "startUrls": [
            {"url": "https://www.facebook.com/polariscampus/"},
        ],
        "maxPosts": MAX_ITEMS["facebook"],
    }, label="Facebook")
    all_posts.extend(ingest_facebook(raw))

    # ── Final deduplication (all platforms) ───────────────────────────────
    seen = set()
    final = []
    for p in all_posts:
        key = (p.get("url") or "")[:100] or (p.get("text","")[:60] + p.get("platform",""))
        if key not in seen:
            seen.add(key)
            final.append(p)

    # Sort newest first
    final.sort(key=lambda x: x.get("date",""), reverse=True)

    log(f"\n=== DONE: {len(final)} total unique posts ===")
    log(f"  By platform: " + ", ".join(
        f"{p}: {sum(1 for x in final if x['platform']==p)}"
        for p in ["linkedin","twitter","instagram","facebook"]
    ))
    campaigns = {}
    for p in final:
        c = p.get("campaign") or "General"
        campaigns[c] = campaigns.get(c,0)+1
    log(f"  By campaign: " + ", ".join(f"{k}: {v}" for k,v in campaigns.items()))

    # ── Write social.json ─────────────────────────────────────────────────
    os.makedirs("data", exist_ok=True)
    with open("data/social.json", "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    log("  data/social.json written")

    # ── Append to history.json ────────────────────────────────────────────
    append_history(final, "data/history.json")

if __name__ == "__main__":
    main()
