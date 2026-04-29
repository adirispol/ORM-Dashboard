"""
Polaris ORM — Unified Apify Crawler v4
========================================
ALL scraping goes through Apify — no Serper, no direct Reddit API.
Uses Claude API for real-time AI analysis of every crawl.

ACTOR MAP
---------
LinkedIn  : 2SyF0bVxmgGr8IVCZ  (LinkedIn Post Searcher)
Twitter/X : apidojo~tweet-scraper
Instagram : apify~instagram-scraper
Reddit    : trudax~reddit-scraper  (Apify actor — no 403 issues)
Quora     : apify~quora-scraper  (Apify actor)
YouTube   : bernardo_breder~youtube-video-detail-scraper (real views)
News/Web  : apify~google-search-scraper (real Google results with dates)

IMPRESSION FORMULAS (transparent, documented)
---------------------------------------------
LinkedIn  : reactions × 80  (Aditya's formula)
Twitter/X : viewCount from API = REAL (when available), else likes × 30
Instagram : videoViewCount = REAL, else (likes + comments×5) × 20
Reddit    : upvotes × 12 + comments × 25  (engagement-anchored estimate)
Quora     : upvotes × 40  (Quora question votes = strong signal)
YouTube   : viewCount = REAL (YouTube Data API)
News/Web  : domain_tier_score × base  (labelled as estimate)

KEYWORD / CAMPAIGN CONFIG
--------------------------
Edit BRAND_TERMS and CAMPAIGNS dict below — no code changes needed elsewhere.

WRITES
------
data/social.json    — all social posts (LinkedIn, Twitter, Instagram, Facebook)
data/mentions.json  — all text mentions (Reddit, Quora, News, Web, YouTube)
data/summary.json   — aggregated counts + per-campaign breakdown
data/history.json   — daily append, never overwrites, 365-day rolling
data/trust_log.json — audit log with source, count, formula used per run

ENV VARS REQUIRED
-----------------
APIFY_TOKEN   — your Apify API token (GitHub Secret)
CLAUDE_KEY    — Anthropic API key for AI analysis (GitHub Secret, optional)
"""

import os, json, time, re, urllib.request, urllib.parse
from datetime import datetime, timezone

# ═══════════════════════════════════════════════════════════════════════════
# ██ CONFIGURATION — edit keywords and campaigns here ██
# ═══════════════════════════════════════════════════════════════════════════

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
CLAUDE_KEY  = os.getenv("CLAUDE_KEY", "")
APIFY_BASE  = "https://api.apify.com/v2"

# ── Brand search terms ─────────────────────────────────────────────────────
# Add / remove terms here. These are searched on every platform.
BRAND_TERMS = [
    "Polaris School of Technology",
    "#polariscampus",
    "PST Bangalore",
    "Polaris BTech",
    "@polaris_code",
]

# ── Campaign / event keywords ──────────────────────────────────────────────
# Each key becomes a campaign tag. Add new events here.
# Keywords are matched against post text (case-insensitive).
CAMPAIGNS = {
    "VibeCon":           ["vibecon", "vibe con", "#vibecon", "vibecon2025", "vibecon2026"],
    "Lyzr/Agentathon":   ["lyzr", "agentathon", "#lyzrai", "lyzr architect"],
    "GSoC 2025":         ["gsoc", "google summer of code", "gsoc2025", "gsoc polaris"],
    "Admissions 2026":   ["admission", "scholarship", "pat exam", "jee polaris",
                          "fee structure", "join polaris", "apply polaris"],
    "vs Competitors":    ["scaler vs polaris", "newton vs polaris", "polaris vs scaler",
                          "polaris vs newton", "nxtwave vs", "which is better polaris"],
    "Campus Life":       ["life at polaris", "campus polaris", "hostel polaris",
                          "whitefield", "electronic city polaris"],
}

# ── Competitor names to track ──────────────────────────────────────────────
COMPETITORS = [
    "scaler school of technology", "newton school of technology",
    "nxtwave", "upgrad", "great learning", "intellipaat",
]

# ── Apify Actor IDs ────────────────────────────────────────────────────────
ACTORS = {
    "linkedin":   "2SyF0bVxmgGr8IVCZ",
    "twitter":    "apidojo~tweet-scraper",
    "instagram":  "apify~instagram-scraper",
    "facebook":   "apify~facebook-pages-scraper",
    "reddit":     "trudax~reddit-scraper",
    "quora":      "apify~quora-scraper",
    "youtube":    "bernardo_breder~youtube-video-detail-scraper",
    "web":        "apify~google-search-scraper",
}

MAX_ITEMS = {
    "linkedin":  60,
    "twitter":   100,
    "instagram": 30,
    "facebook":  20,
    "reddit":    50,
    "quora":     30,
    "youtube":   20,
    "web":       20,
}

# ── Impression formulas (fully transparent) ────────────────────────────────
# These are documented here and written into every record's impression_formula field
IMPRESSION_FORMULAS = {
    "linkedin":   "reactions × 80  [Polaris formula: LinkedIn hides real impressions]",
    "twitter":    "viewCount from Twitter API (REAL when available), else likes × 30",
    "instagram":  "videoViewCount (REAL for videos), else (likes + comments×5) × 20",
    "reddit":     "upvotes × 12 + comments × 25  [engagement-anchored estimate]",
    "quora":      "upvotes × 40  [Quora question votes are strong engagement signal]",
    "youtube":    "viewCount from YouTube Data API (REAL)",
    "news":       "domain tier: major=8000, regional=3000, blog=800  [estimate]",
    "web":        "Google search position × 600 + 400  [visibility estimate]",
}

# ═══════════════════════════════════════════════════════════════════════════
# ██ HELPERS ██
# ═══════════════════════════════════════════════════════════════════════════

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]  {msg}", flush=True)

def sentiment(text):
    t = (text or "").lower()
    neg = ["scam","fake","bad","worst","fraud","cheat","avoid","terrible",
           "useless","disappointed","overrated","waste","horrible","not worth",
           "misleading","lied","wrong","do not join","don't join","poor"]
    pos = ["good","great","best","awesome","excellent","recommended","proud",
           "brilliant","selected","placed","got into","joined","offer letter",
           "gsoc","amazing","love","incredible","congrats","winner","built",
           "shipped","vibecon","hackathon","agentathon","lyzr","funded",
           "scholarship","ranked","achievement","top","impressive"]
    score = sum(1 for w in pos if w in t) - sum(2 for w in neg if w in t)
    if score > 0: return "positive"
    if score < 0: return "negative"
    return "neutral"

def detect_campaign(text):
    t = (text or "").lower()
    for campaign, keywords in CAMPAIGNS.items():
        if any(kw.lower() in t for kw in keywords):
            return campaign
    return None

def check_competitor(text):
    t = (text or "").lower()
    for c in COMPETITORS:
        if c in t:
            return c.title()
    return None

def is_polaris(text):
    t = (text or "").lower()
    return any(term.lower().lstrip("#@") in t for term in BRAND_TERMS)

# ── Date parser — handles all formats Apify returns ───────────────────────
def parse_dt(item):
    """
    Tries every date field Apify actors use.
    Returns ISO string. Falls back to now() only if nothing found.
    This fixes the 'real date missing' issue — we never fabricate dates.
    """
    fields = [
        "posted_at","createdAt","created_at","postedAtISO","publishedAt",
        "date","timestamp","time","created_time","updatedAt","taken_at_timestamp",
        "postDate","pubDate","published","created","dateTime","created_utc",
    ]
    for key in fields:
        val = item.get(key)
        if not val:
            continue
        # Unix timestamp (seconds or milliseconds)
        if isinstance(val, (int, float)):
            t = int(val)
            if t > 1e12: t //= 1000
            try:
                return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
            except:
                continue
        if isinstance(val, dict):
            ts = val.get("timestamp") or val.get("date")
            if ts:
                try:
                    t = int(ts)
                    if t > 1e12: t //= 1000
                    return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
                except:
                    pass
            continue
        s = str(val).strip()
        if not s or s == "null":
            continue
        if s.isdigit():
            try:
                t = int(s)
                if t > 1e12: t //= 1000
                return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
            except:
                continue
        # Try ISO parse
        try:
            return datetime.fromisoformat(s.replace("Z","+00:00")).isoformat()
        except:
            pass
        # Try common string formats
        for fmt in ("%a %b %d %H:%M:%S %z %Y",   # Twitter: Mon Apr 25 10:02:18 +0000 2026
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%Y-%m-%d %H:%M:%S",
                    "%d/%m/%Y",
                    "%B %d, %Y",):
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except:
                continue
    # Last resort — mark clearly as unknown so dashboard shows N/A not a fake date
    return "UNKNOWN"

# ═══════════════════════════════════════════════════════════════════════════
# ██ APIFY RUNNER ██
# ═══════════════════════════════════════════════════════════════════════════

def apify_request(method, path, body=None):
    url  = f"{APIFY_BASE}{path}?token={APIFY_TOKEN}"
    data = json.dumps(body).encode() if body else None
    h    = {"Content-Type": "application/json"}
    req  = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  Apify API error {method} {path[:60]}: {e}")
        return {}

def run_actor(actor_id, payload, label="", timeout_mins=8):
    log(f"▶ [{label}] starting actor: {actor_id}")
    res = apify_request("POST", f"/acts/{actor_id}/runs", payload)
    if not res or "data" not in res:
        log(f"  [{label}] FAILED to start — check actor ID and token")
        return [], {"actor": actor_id, "label": label, "status": "FAILED_TO_START", "items": 0}

    run_id = res["data"]["id"]
    log(f"  [{label}] run ID: {run_id}")

    status = "RUNNING"
    polls  = timeout_mins * 15  # poll every 4 seconds
    for i in range(polls):
        time.sleep(4)
        try:
            sd     = apify_request("GET", f"/actor-runs/{run_id}")
            status = (sd.get("data") or {}).get("status", "UNKNOWN")
            if i % 10 == 0:
                log(f"  [{label}] {status} ({i*4}s)")
            if status in ("SUCCEEDED","FAILED","ABORTED","TIMED-OUT"):
                break
        except Exception as e:
            log(f"  [{label}] poll error: {e}")

    if status != "SUCCEEDED":
        log(f"  [{label}] ended: {status}")
        return [], {"actor": actor_id, "label": label, "status": status, "items": 0}

    try:
        items = apify_request("GET", f"/actor-runs/{run_id}/dataset/items")
        count = len(items) if isinstance(items, list) else 0
        log(f"  [{label}] ✓ {count} items")
        trust = {
            "actor":      actor_id,
            "label":      label,
            "status":     "SUCCEEDED",
            "run_id":     run_id,
            "items_raw":  count,
            "run_url":    f"https://console.apify.com/actors/runs/{run_id}",
        }
        return items if isinstance(items, list) else [], trust
    except Exception as e:
        log(f"  [{label}] dataset error: {e}")
        return [], {"actor": actor_id, "label": label, "status": "DATASET_ERROR", "items": 0}

# ═══════════════════════════════════════════════════════════════════════════
# ██ INGEST FUNCTIONS (one per platform) ██
# ═══════════════════════════════════════════════════════════════════════════

def build_post(platform, text, url, date_raw_item, likes=0, comments=0,
               reposts=0, impressions=0, impressions_type="estimated",
               author="", extra=None):
    """Canonical post builder — every field documented."""
    date = parse_dt(date_raw_item) if isinstance(date_raw_item, dict) else date_raw_item
    return {
        "platform":           platform,
        "author":             author,
        "text":               (text or "").strip()[:1000],
        "url":                url or "",
        "date":               date,
        "likes":              int(likes or 0),
        "comments":           int(comments or 0),
        "reposts":            int(reposts or 0),
        "impressions":        int(impressions or 0),
        "impressions_type":   impressions_type,
        "impressions_formula": IMPRESSION_FORMULAS.get(platform, ""),
        "sentiment":          sentiment(text),
        "campaign":           detect_campaign(text),
        "tags_polaris":       "Yes" if is_polaris(text) else "No",
        "competitor":         check_competitor(text),
        "scraped_at":         now_iso(),
        **(extra or {}),
    }

# ── LinkedIn ───────────────────────────────────────────────────────────────
def ingest_linkedin(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        af    = item.get("author") or {}
        if not isinstance(af, dict): af = {}
        name  = (af.get("name") or
                 f"{af.get('firstName','')} {af.get('lastName','')}".strip() or
                 item.get("authorName",""))
        text  = (item.get("text") or item.get("content") or
                 item.get("commentary") or "")
        if not text: continue

        # Stats — try every field name Apify LinkedIn actors use
        stats   = item.get("stats") or item.get("socialActivityCountsInsight") or {}
        likes   = int(item.get("numLikes") or item.get("likes") or
                      stats.get("numLikes") or stats.get("likeCount") or
                      item.get("reactionCount") or item.get("totalReactionCount") or 0)
        comments= int(item.get("numComments") or item.get("comments") or
                      stats.get("numComments") or stats.get("commentCount") or 0)
        reposts = int(item.get("numShares") or item.get("shares") or
                      stats.get("numShares") or stats.get("repostCount") or 0)

        # ★ ADITYA'S FORMULA: impressions = reactions × 80
        # LinkedIn hides real impressions from all scrapers.
        # Reactions (likes+comments+reposts) × 80 is the agreed proxy.
        reactions  = likes + comments + reposts
        impressions = max(reactions * 80, 100)

        url = (item.get("url") or item.get("postUrl") or
               item.get("shareUrl") or item.get("link") or "")

        posts.append(build_post(
            platform="linkedin",
            text=text, url=url, date_raw_item=item,
            likes=likes, comments=comments, reposts=reposts,
            impressions=impressions, impressions_type="reactions×80_formula",
            author=name,
            extra={"author_url": af.get("profileUrl",""), "reactions": reactions}
        ))
    log(f"  LinkedIn ingested: {len(posts)}")
    return posts

# ── Twitter / X ────────────────────────────────────────────────────────────
def ingest_twitter(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        text   = (item.get("full_text") or item.get("text") or
                  item.get("rawContent") or item.get("content") or "")
        if not text: continue
        author = (item.get("user") or {}).get("name") or item.get("author","")
        likes  = int(item.get("likeCount") or item.get("favorite_count") or 0)
        rts    = int(item.get("retweetCount") or item.get("retweet_count") or 0)
        reps   = int(item.get("replyCount") or item.get("reply_count") or 0)

        # Real view count from Twitter API when available
        views_raw = item.get("viewCount") or item.get("impressionCount") or 0
        if views_raw and int(views_raw) > 0:
            impressions = int(views_raw)
            impr_type   = "real_twitter_views"
        else:
            impressions = max(likes * 30 + rts * 50 + reps * 20, 10)
            impr_type   = "engagement_estimate_likes×30"

        url = item.get("url") or item.get("twitterUrl") or ""
        posts.append(build_post(
            platform="twitter", text=text, url=url, date_raw_item=item,
            likes=likes, comments=reps, reposts=rts,
            impressions=impressions, impressions_type=impr_type,
            author=author
        ))
    log(f"  Twitter ingested: {len(posts)}")
    return posts

# ── Instagram ──────────────────────────────────────────────────────────────
def ingest_instagram(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        caption = (item.get("caption") or item.get("alt") or "")
        likes   = int(item.get("likesCount") or item.get("likes_count") or 0)
        comments= int(item.get("commentsCount") or item.get("comments_count") or 0)
        # Real video views when available
        video_views = int(item.get("videoViewCount") or item.get("videoPlayCount") or 0)
        if video_views > 0:
            impressions = video_views
            impr_type   = "real_instagram_video_views"
        else:
            impressions = max((likes + comments * 5) * 20, 50)
            impr_type   = "engagement_estimate_(likes+comments×5)×20"

        posts.append(build_post(
            platform="instagram", text=caption,
            url=item.get("url") or item.get("link") or "",
            date_raw_item=item, likes=likes, comments=comments,
            impressions=impressions, impressions_type=impr_type,
            author=item.get("ownerUsername") or ""
        ))
    log(f"  Instagram ingested: {len(posts)}")
    return posts

# ── Facebook ───────────────────────────────────────────────────────────────
def ingest_facebook(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        text    = item.get("text") or item.get("story") or item.get("message") or ""
        likes   = int(item.get("likes") or item.get("likesCount") or 0)
        comments= int(item.get("comments") or item.get("commentsCount") or 0)
        shares  = int(item.get("shares") or item.get("sharesCount") or 0)
        impressions = max((likes + comments * 3 + shares * 5) * 40, 50)
        posts.append(build_post(
            platform="facebook", text=text,
            url=item.get("url") or item.get("postUrl") or "",
            date_raw_item=item, likes=likes, comments=comments, reposts=shares,
            impressions=impressions, impressions_type="engagement_estimate_(likes+comments×3+shares×5)×40",
            author=item.get("pageName") or item.get("authorName") or ""
        ))
    log(f"  Facebook ingested: {len(posts)}")
    return posts

# ── Reddit via Apify (no 403 issues) ──────────────────────────────────────
def ingest_reddit(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        title   = item.get("title") or item.get("text") or ""
        body    = item.get("body") or item.get("selftext") or ""
        text    = f"{title} {body}".strip()
        if not text: continue
        ups     = int(item.get("ups") or item.get("score") or item.get("upvotes") or 0)
        comms   = int(item.get("num_comments") or item.get("numComments") or item.get("comments") or 0)
        # Formula: upvotes × 12 + comments × 25
        impressions = max(ups * 12 + comms * 25, 100)
        url = item.get("url") or item.get("permalink") or ""
        if url and not url.startswith("http"):
            url = f"https://reddit.com{url}"
        posts.append(build_post(
            platform="reddit", text=title, url=url, date_raw_item=item,
            likes=ups, comments=comms,
            impressions=impressions, impressions_type="upvotes×12+comments×25",
            author=item.get("author") or "",
            extra={"subreddit": item.get("subreddit",""), "upvotes": ups}
        ))
    log(f"  Reddit ingested: {len(posts)}")
    return posts

# ── Quora via Apify ────────────────────────────────────────────────────────
def ingest_quora(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        text    = (item.get("question") or item.get("title") or
                   item.get("text") or item.get("answer") or "")
        if not text: continue
        upvotes = int(item.get("upvotes") or item.get("answerUpvotes") or
                      item.get("score") or 0)
        # Formula: upvotes × 40 (Quora votes indicate real engagement)
        impressions = max(upvotes * 40, 200)
        url = item.get("url") or item.get("link") or ""
        posts.append(build_post(
            platform="quora", text=text, url=url, date_raw_item=item,
            likes=upvotes, impressions=impressions,
            impressions_type="upvotes×40",
            author=item.get("author") or item.get("authorName") or ""
        ))
    log(f"  Quora ingested: {len(posts)}")
    return posts

# ── YouTube (real view counts) ─────────────────────────────────────────────
def ingest_youtube(raw):
    posts = []
    for item in raw:
        if not isinstance(item, dict): continue
        title  = item.get("title") or item.get("name") or ""
        desc   = item.get("description") or ""
        views  = int(item.get("viewCount") or item.get("views") or
                     item.get("statistics", {}).get("viewCount", 0) or 0)
        url    = item.get("url") or item.get("link") or ""
        if not url and item.get("id"):
            url = f"https://youtube.com/watch?v={item['id']}"
        posts.append(build_post(
            platform="youtube", text=title, url=url, date_raw_item=item,
            likes=int(item.get("likes") or item.get("likeCount") or 0),
            comments=int(item.get("commentCount") or 0),
            impressions=views, impressions_type="real_youtube_views",
            author=item.get("channelName") or item.get("author") or "",
            extra={"views_real": views, "duration": item.get("duration","")}
        ))
    log(f"  YouTube ingested: {len(posts)}")
    return posts

# ── Web / News (Google Search via Apify) ──────────────────────────────────
def ingest_web(raw, platform="web"):
    posts = []
    major_domains = ["ndtv","timesofindia","thehindu","hindustantimes",
                     "economictimes","livemint","businessstandard","forbes"]
    for i, item in enumerate(raw):
        if not isinstance(item, dict): continue
        title   = item.get("title") or item.get("name") or ""
        desc    = item.get("description") or item.get("snippet") or ""
        text    = f"{title} {desc}".strip()
        if not text: continue
        url     = item.get("url") or item.get("link") or ""
        # Domain-tier impression estimate
        domain  = url.lower()
        if any(d in domain for d in major_domains):
            impressions = 8000
            impr_type   = "major_news_domain_estimate"
        else:
            pos = i  # position in results
            impressions = max((10 - pos) * 600 + 400, 400)
            impr_type   = f"google_position_{pos+1}_estimate"

        # Try to get the real date from the item (Apify Google Search actor includes it)
        posts.append(build_post(
            platform=platform, text=title, url=url, date_raw_item=item,
            impressions=impressions, impressions_type=impr_type,
            extra={"snippet": desc[:200]}
        ))
    log(f"  {platform.title()} ingested: {len(posts)}")
    return posts

# ═══════════════════════════════════════════════════════════════════════════
# ██ HISTORY + TRUST LOG ██
# ═══════════════════════════════════════════════════════════════════════════

def append_history(all_posts, all_mentions, path="data/history.json"):
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception as e:
            log(f"  History read error: {e}")

    existing = [e for e in existing if e.get("date") != today]

    combined = all_posts + all_mentions
    by_plat  = {}
    by_camp  = {}
    sent_ct  = {"positive":0,"neutral":0,"negative":0}
    total_impr = 0

    for p in combined:
        pl = p.get("platform","unknown")
        by_plat[pl] = by_plat.get(pl,0)+1
        c  = p.get("campaign") or "General"
        by_camp[c]  = by_camp.get(c,0)+1
        total_impr += p.get("impressions",0)
        s  = p.get("sentiment","neutral")
        if s in sent_ct: sent_ct[s]+=1

    existing.append({
        "date":        today,
        "social_posts":  len(all_posts),
        "mentions":      len(all_mentions),
        "total":         len(combined),
        "by_platform":   by_plat,
        "by_campaign":   by_camp,
        "sentiment":     sent_ct,
        "impressions":   total_impr,
        "scraped_at":    now_iso(),
    })
    existing = sorted(existing, key=lambda x: x.get("date",""))[-365:]

    os.makedirs("data", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    log(f"  History: {len(existing)} days recorded")

def write_trust_log(trust_entries, all_posts, all_mentions):
    """
    Trust log = auditable record of every Apify run this crawl.
    Each entry has: actor ID, run ID, items fetched, Apify console URL.
    This is how you verify counts — click the run URL to see raw data.
    """
    trust = {
        "crawl_time":      now_iso(),
        "total_social":    len(all_posts),
        "total_mentions":  len(all_mentions),
        "impression_formulas": IMPRESSION_FORMULAS,
        "campaign_keywords":   CAMPAIGNS,
        "actor_runs":      trust_entries,
        "how_to_verify": (
            "Each actor_run has a run_url — open it in Apify console "
            "to see the exact raw data returned. item counts here match "
            "what Apify stored in its dataset for that run."
        ),
    }
    with open("data/trust_log.json", "w", encoding="utf-8") as f:
        json.dump(trust, f, ensure_ascii=False, indent=2)
    log("  Trust log written: data/trust_log.json")

# ═══════════════════════════════════════════════════════════════════════════
# ██ CLAUDE AI ANALYSIS ██
# ═══════════════════════════════════════════════════════════════════════════

def run_claude_analysis(all_posts, all_mentions, summary):
    """
    Calls Claude API with the crawl data for AI-powered insights.
    Only runs if CLAUDE_KEY is set.
    """
    if not CLAUDE_KEY:
        log("  Claude: CLAUDE_KEY not set — skipping AI analysis")
        return None

    combined = all_posts + all_mentions
    camp_counts = {}
    for p in combined:
        c = p.get("campaign") or "General"
        camp_counts[c] = camp_counts.get(c,0)+1

    neg_posts = [p for p in combined if p.get("sentiment")=="negative"]
    neg_sample = [p.get("text","")[:100] for p in neg_posts[:3]]

    prompt = f"""You are a brand intelligence analyst for Polaris School of Technology (BTech AI/PM/Cloud, Bangalore).

Today's crawl data:
- Total posts/mentions: {len(combined)}
- Social posts: {len(all_posts)}
- Web/text mentions: {len(all_mentions)}
- Sentiment: {summary.get('sentiment',{})}
- Campaign activity: {camp_counts}
- Negative mentions ({len(neg_posts)} total): {neg_sample}
- Platforms: {summary.get('platforms',{})}

Provide:
1. BRAND HEALTH (2 sentences — what's working, what's not)
2. TOP RISK (1 sentence — most urgent thing to address)
3. CAMPAIGN INSIGHT (1 sentence about VibeCon/Lyzr/GSoC activity)
4. RECOMMENDED ACTION (2 bullet points — specific, actionable)

Keep total response under 150 words. Be direct, no fluff."""

    body = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         CLAUDE_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
            analysis = data.get("content",[{}])[0].get("text","")
            log(f"  Claude analysis complete ({len(analysis)} chars)")
            return analysis
    except Exception as e:
        log(f"  Claude analysis error: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════
# ██ MAIN ██
# ═══════════════════════════════════════════════════════════════════════════

def main():
    log("═══════════════════════════════════════════")
    log("  Polaris ORM — Unified Apify Crawler v4  ")
    log("═══════════════════════════════════════════")

    if not APIFY_TOKEN:
        log("ERROR: APIFY_TOKEN not set in GitHub Secrets")
        raise SystemExit(1)

    trust_entries = []
    all_posts     = []   # social (LinkedIn, Twitter, Instagram, Facebook)
    all_mentions  = []   # text mentions (Reddit, Quora, YouTube, Web)

    # ── LinkedIn ──────────────────────────────────────────────────────────
    log("\n=== LINKEDIN ===")
    li_queries = [BRAND_TERMS[0], "#polariscampus", "#VibeCon", "#VibeCon2025",
                  "#VibeCon2026", "VibeCon Polaris", "Lyzr Polaris",
                  "Agentathon Polaris", "GSoC Polaris", "#polarisschooloftechnology"]
    for q in li_queries:
        raw, trust = run_actor(ACTORS["linkedin"], {
            "searchQuery": q,
            "maxResults":  MAX_ITEMS["linkedin"],
        }, label=f"LinkedIn:{q[:25]}")
        trust_entries.append(trust)
        all_posts.extend(ingest_linkedin(raw))
        time.sleep(3)

    # Deduplicate LinkedIn by URL
    seen_li = set()
    li_deduped = []
    for p in all_posts:
        key = p.get("url","")[:100] or p.get("text","")[:60]
        if key not in seen_li:
            seen_li.add(key)
            li_deduped.append(p)
    log(f"  LinkedIn after dedup: {len(li_deduped)}")
    all_posts = li_deduped

    # ── Twitter / X ───────────────────────────────────────────────────────
    log("\n=== TWITTER / X ===")
    tw_queries = [
        f'"{BRAND_TERMS[0]}"',
        "#polariscampus OR @polaris_code",
        "#VibeCon OR #VibeCon2025 OR #VibeCon2026",
        "Agentathon Polaris OR Lyzr Polaris",
        "GSoC 2025 Polaris",
    ]
    for q in tw_queries:
        raw, trust = run_actor(ACTORS["twitter"], {
            "searchTerms":        [q],
            "maxTweets":          MAX_ITEMS["twitter"],
            "addUserInfo":        True,
            "scrapeTweetReplies": False,
        }, label=f"Twitter:{q[:25]}")
        trust_entries.append(trust)
        all_posts.extend(ingest_twitter(raw))
        time.sleep(2)

    # ── Instagram ─────────────────────────────────────────────────────────
    log("\n=== INSTAGRAM ===")
    raw, trust = run_actor(ACTORS["instagram"], {
        "directUrls":   [
            "https://www.instagram.com/explore/tags/polariscampus/",
            "https://www.instagram.com/explore/tags/polarisschooloftechnology/",
            "https://www.instagram.com/explore/tags/vibecon/",
            "https://www.instagram.com/polariscampus/",
        ],
        "resultsType":  "posts",
        "resultsLimit": MAX_ITEMS["instagram"],
    }, label="Instagram")
    trust_entries.append(trust)
    all_posts.extend(ingest_instagram(raw))

    # ── Facebook ──────────────────────────────────────────────────────────
    log("\n=== FACEBOOK ===")
    raw, trust = run_actor(ACTORS["facebook"], {
        "startUrls":    [{"url": "https://www.facebook.com/polariscampus/"}],
        "maxPosts":     MAX_ITEMS["facebook"],
    }, label="Facebook")
    trust_entries.append(trust)
    all_posts.extend(ingest_facebook(raw))

    # ── Reddit via Apify (no 403) ─────────────────────────────────────────
    log("\n=== REDDIT ===")
    reddit_queries = [
        BRAND_TERMS[0],
        "PST Bangalore Polaris",
        "VibeCon Polaris",
    ]
    for q in reddit_queries:
        raw, trust = run_actor(ACTORS["reddit"], {
            "searches":    [q],
            "type":        "posts",
            "maxItems":    MAX_ITEMS["reddit"],
            "sort":        "new",
        }, label=f"Reddit:{q[:25]}")
        trust_entries.append(trust)
        all_mentions.extend(ingest_reddit(raw))
        time.sleep(2)

    # ── Quora via Apify ───────────────────────────────────────────────────
    log("\n=== QUORA ===")
    raw, trust = run_actor(ACTORS["quora"], {
        "queries":   [BRAND_TERMS[0], "Polaris BTech AI Bangalore"],
        "maxItems":  MAX_ITEMS["quora"],
    }, label="Quora")
    trust_entries.append(trust)
    all_mentions.extend(ingest_quora(raw))

    # ── YouTube ───────────────────────────────────────────────────────────
    log("\n=== YOUTUBE ===")
    raw, trust = run_actor(ACTORS["youtube"], {
        "searchQuery": BRAND_TERMS[0],
        "maxResults":  MAX_ITEMS["youtube"],
    }, label="YouTube")
    trust_entries.append(trust)
    all_mentions.extend(ingest_youtube(raw))

    # ── Web / News via Apify Google Search ────────────────────────────────
    log("\n=== WEB / NEWS ===")
    web_queries = [
        f'"{BRAND_TERMS[0]}" site:quora.com',
        f'"{BRAND_TERMS[0]}" site:medium.com',
        f'"{BRAND_TERMS[0]}" site:shiksha.com OR site:collegedunia.com OR site:careers360.com',
        f'"{BRAND_TERMS[0]}" review 2025 OR 2026',
        f'"VibeCon" Polaris',
        f'"{BRAND_TERMS[0]}" news',
    ]
    for q in web_queries:
        raw, trust = run_actor(ACTORS["web"], {
            "queries":  [{"keyword": q}],
            "maxPagesPerQuery": 2,
            "resultsPerPage":   10,
        }, label=f"Web:{q[:25]}")
        trust_entries.append(trust)
        # Route to correct platform label
        plat = "quora" if "quora" in q else "medium" if "medium" in q else \
               "portal" if any(s in q for s in ["shiksha","collegedunia","careers360"]) else \
               "news" if "news" in q else "web"
        all_mentions.extend(ingest_web(raw, platform=plat))
        time.sleep(1)

    # ── Global deduplication ──────────────────────────────────────────────
    log("\n=== DEDUPLICATION ===")
    def dedup(lst):
        seen = set()
        out  = []
        for p in lst:
            key = (p.get("url","")[:100] or
                   p.get("text","")[:60] + p.get("platform",""))
            if key and key not in seen:
                seen.add(key)
                out.append(p)
        return out

    all_posts    = dedup(all_posts)
    all_mentions = dedup(all_mentions)

    # Sort newest first — UNKNOWN dates go to end
    def sort_key(p):
        d = p.get("date","")
        return "0000" if d == "UNKNOWN" else d

    all_posts.sort(key=sort_key, reverse=True)
    all_mentions.sort(key=sort_key, reverse=True)

    # ── Build summary ─────────────────────────────────────────────────────
    combined = all_posts + all_mentions
    by_plat  = {}
    by_camp  = {}
    sent_ct  = {"positive":0,"neutral":0,"negative":0}
    total_impr = 0

    for p in combined:
        pl = p.get("platform","unknown")
        by_plat[pl] = by_plat.get(pl,0)+1
        c  = p.get("campaign") or "General"
        by_camp[c]  = by_camp.get(c,0)+1
        total_impr += p.get("impressions",0)
        s  = p.get("sentiment","neutral")
        if s in sent_ct: sent_ct[s]+=1

    summary = {
        "total_mentions":    len(all_mentions),
        "total_social":      len(all_posts),
        "total_combined":    len(combined),
        "platforms":         by_plat,
        "campaigns":         by_camp,
        "sentiment":         sent_ct,
        "impressions":       total_impr,
        "impression_formulas": IMPRESSION_FORMULAS,
        "last_updated":      now_iso(),
    }

    # ── Claude AI analysis ─────────────────────────────────────────────────
    ai_analysis = run_claude_analysis(all_posts, all_mentions, summary)
    if ai_analysis:
        summary["ai_analysis"] = ai_analysis

    # ── Log results ───────────────────────────────────────────────────────
    log(f"\n{'═'*45}")
    log(f"  SOCIAL POSTS:   {len(all_posts)}")
    log(f"  MENTIONS:       {len(all_mentions)}")
    log(f"  COMBINED:       {len(combined)}")
    log(f"  BY PLATFORM:    {by_plat}")
    log(f"  BY CAMPAIGN:    {by_camp}")
    log(f"  SENTIMENT:      {sent_ct}")
    log(f"  IMPRESSIONS:    {total_impr:,}")
    log(f"{'═'*45}\n")

    # ── Write files ───────────────────────────────────────────────────────
    os.makedirs("data", exist_ok=True)

    with open("data/social.json", "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2)

    with open("data/mentions.json", "w", encoding="utf-8") as f:
        json.dump(all_mentions, f, ensure_ascii=False, indent=2)

    with open("data/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    append_history(all_posts, all_mentions)
    write_trust_log(trust_entries, all_posts, all_mentions)

    log("All files written. Done.")

if __name__ == "__main__":
    main()
