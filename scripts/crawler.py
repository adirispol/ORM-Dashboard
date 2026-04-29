"""
Polaris ORM — Main Mentions Crawler v2
Fixes vs v1:
  - Impressions: Reddit uses real upvotes/comments, everything else labelled as estimated
  - History: every run appends to data/history.json, never overwrites
  - Data stored by date so dashboard can show trends
  - VibeCon / Lyzr / GSoC keywords added
  - Quora: real question view counts not available (labelled estimated)
Writes: data/mentions.json, data/summary.json, data/history.json
Triggered by: .github/workflows/polaris-crawler.yml (daily)
"""

import os, json, time, re, urllib.request, urllib.parse
from datetime import datetime, timezone

# ── Config from environment ────────────────────────────────────────────────
SERPER_KEY   = os.getenv("SERPER_KEY", "")
YOUTUBE_KEY  = os.getenv("YOUTUBE_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# ── Brand keywords ─────────────────────────────────────────────────────────
KW_BRAND = [
    "Polaris School of Technology",
    "PST Bangalore",
    "polariscampus",
    "Polaris BTech",
    "Polaris Campus Bengaluru",
]

KW_EVENTS = [
    "VibeCon Polaris",
    "Polaris Lyzr",
    "Polaris Agentathon",
    "Polaris GSoC 2025",
    "Polaris VibeCon",
]

# ── Helpers ────────────────────────────────────────────────────────────────
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]  {msg}", flush=True)

def http_get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  HTTP error: {e} — {url[:80]}")
        return {}

def http_post(url, body, headers=None):
    data = json.dumps(body).encode()
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        log(f"  HTTP POST error: {e}")
        return {}

def sentiment(text):
    t = (text or "").lower()
    neg = ["scam","fake","bad","worst","fraud","cheat","avoid","terrible",
           "useless","disappointed","overrated","waste","horrible","pathetic",
           "misleading","lied","wrong","bad college","not worth"]
    pos = ["good","great","best","awesome","excellent","top","recommended",
           "proud","brilliant","selected","placed","got into","joined","offer",
           "gsoc","amazing","love","incredible","congrats","winner","funded",
           "vibecon","hackathon","agentathon","lyzr","built","shipped"]
    if any(w in t for w in neg): return "negative"
    if any(w in t for w in pos): return "positive"
    return "neutral"

def detect_campaign(text):
    t = (text or "").lower()
    if any(k in t for k in ["vibecon","vibe con"]): return "VibeCon"
    if any(k in t for k in ["lyzr","agentathon"]): return "Lyzr/Agentathon"
    if "gsoc" in t or "google summer of code" in t: return "GSoC 2025"
    if any(k in t for k in ["admission","scholarship","join polaris","pat exam","fee structure"]): return "Admissions"
    if any(k in t for k in ["scaler","newton","nxtwave","intellipaat","vs polaris","polaris vs"]): return "vs Competitors"
    return None

# ── Reddit (real upvotes + comments) ──────────────────────────────────────
def fetch_reddit():
    """
    Uses Reddit's public JSON API — no auth required.
    Returns real upvote + comment counts.
    Impression estimate: subreddit_subscribers * 0.02 * upvote_ratio (conservative)
    """
    posts = []
    subreddits = ["Btechtards","india","CollegeDropouts","courselearning",
                  "developersIndia","cscareerquestions","EngineeringStudents"]
    search_terms = [kw for kw in KW_BRAND + KW_EVENTS]

    seen_ids = set()
    for term in search_terms[:6]:  # limit API calls
        encoded = urllib.parse.quote(term)
        url = f"https://www.reddit.com/search.json?q={encoded}&sort=new&limit=25&t=year"
        headers = {"User-Agent": "PolarisORM/2.0 (brand monitoring)"}
        data = http_get(url, headers)
        children = (data.get("data") or {}).get("children") or []
        for child in children:
            item = child.get("data") or {}
            pid = item.get("id","")
            if not pid or pid in seen_ids: continue
            seen_ids.add(pid)

            title   = item.get("title","")
            selftext= item.get("selftext","")
            text    = f"{title} {selftext}".strip()
            ups     = int(item.get("ups") or item.get("score") or 0)
            comments= int(item.get("num_comments") or 0)
            ratio   = float(item.get("upvote_ratio") or 0.5)
            subs    = int(item.get("subreddit_subscribers") or 5000)

            # Real impression estimate: subreddit size * upvote ratio factor
            # Upvoted posts get shown more; this is still an estimate but anchored to real data
            engagement_score = ups + comments * 3
            impressions_est  = int(min(subs * ratio * 0.03 + engagement_score * 20, subs))
            impressions_est  = max(impressions_est, ups * 10, 200)

            posts.append({
                "platform":         "reddit",
                "text":             title[:200],
                "url":              f"https://reddit.com{item.get('permalink','')}",
                "date":             datetime.fromtimestamp(
                                        item.get("created_utc", time.time()),
                                        tz=timezone.utc
                                    ).isoformat(),
                "sentiment":        sentiment(text),
                "campaign":         detect_campaign(text),
                "score":            1 if sentiment(text)=="positive" else (-1 if sentiment(text)=="negative" else 0),
                "upvotes":          ups,
                "comments":         comments,
                "impressions":      impressions_est,
                "impressions_type": "estimated_from_engagement",
                "subreddit":        item.get("subreddit",""),
                "scraped_at":       now_iso(),
            })
        time.sleep(1.5)  # be polite to Reddit

    log(f"  Reddit: {len(posts)} posts")
    return posts

# ── Serper (Google Search — Quora, Medium, News, Portal, Web) ─────────────
def fetch_serper(platform_label, query, num=10):
    if not SERPER_KEY:
        log(f"  Serper: no key — skipping {platform_label}")
        return []
    body = {"q": query, "num": num, "gl": "in", "hl": "en"}
    data = http_post("https://google.serper.dev/search", body,
                     headers={"X-API-KEY": SERPER_KEY})
    results = data.get("organic") or []
    posts = []
    for i, r in enumerate(results):
        title  = r.get("title","")
        snippet= r.get("snippet","")
        text   = f"{title} {snippet}".strip()
        link   = r.get("link","")
        # Position-based impression estimate — clearly labelled
        # Position 1 = highest visibility, position 10 = lowest
        pos_score = max(0, 10 - i)
        impressions_est = pos_score * 500 + 500  # 500-5500 range, search-position based
        posts.append({
            "platform":         platform_label,
            "text":             title[:200],
            "url":              link,
            "date":             now_iso(),
            "sentiment":        sentiment(text),
            "campaign":         detect_campaign(text),
            "score":            1 if sentiment(text)=="positive" else (-1 if sentiment(text)=="negative" else 0),
            "impressions":      impressions_est,
            "impressions_type": "search_position_estimate",
            "scraped_at":       now_iso(),
        })
    log(f"  Serper [{platform_label}] '{query[:40]}': {len(posts)} results")
    return posts

# ── Quora ──────────────────────────────────────────────────────────────────
def fetch_quora():
    queries = [
        f'site:quora.com "Polaris School of Technology"',
        f'site:quora.com "PST Bangalore" OR "Polaris Campus"',
    ]
    posts = []
    for q in queries:
        posts.extend(fetch_serper("quora", q, num=15))
    # Deduplicate by URL
    seen = set()
    deduped = []
    for p in posts:
        if p["url"] not in seen:
            seen.add(p["url"])
            deduped.append(p)
    log(f"  Quora total: {len(deduped)}")
    return deduped

# ── Medium ─────────────────────────────────────────────────────────────────
def fetch_medium():
    queries = [
        f'site:medium.com "Polaris School of Technology"',
        f'site:medium.com "PST" "Polaris" BTech',
    ]
    posts = []
    for q in queries:
        posts.extend(fetch_serper("medium", q, num=10))
    seen = set()
    deduped = [p for p in posts if not (p["url"] in seen or seen.add(p["url"]))]
    log(f"  Medium total: {len(deduped)}")
    return deduped

# ── News ───────────────────────────────────────────────────────────────────
def fetch_news():
    if not NEWS_API_KEY:
        # Fallback to Serper news
        return fetch_serper("news", '"Polaris School of Technology" news India', num=10)
    url = (f"https://newsapi.org/v2/everything"
           f"?q=Polaris+School+of+Technology"
           f"&language=en&sortBy=publishedAt&pageSize=20"
           f"&apiKey={NEWS_API_KEY}")
    data = http_get(url)
    articles = data.get("articles") or []
    posts = []
    for a in articles:
        title   = a.get("title","") or ""
        desc    = a.get("description","") or ""
        text    = f"{title} {desc}".strip()
        # News impressions: no real data available, estimate from source
        source  = (a.get("source") or {}).get("name","")
        impressions = 5000 if source in ("Times of India","NDTV","Hindustan Times") else 1000
        posts.append({
            "platform":         "news",
            "text":             title[:200],
            "url":              a.get("url",""),
            "date":             a.get("publishedAt") or now_iso(),
            "sentiment":        sentiment(text),
            "campaign":         detect_campaign(text),
            "score":            1 if sentiment(text)=="positive" else (-1 if sentiment(text)=="negative" else 0),
            "impressions":      impressions,
            "impressions_type": "source_tier_estimate",
            "source":           source,
            "scraped_at":       now_iso(),
        })
    log(f"  News: {len(posts)} articles")
    return posts

# ── Portal listings ────────────────────────────────────────────────────────
def fetch_portals():
    queries = [
        f'(site:shiksha.com OR site:careers360.com OR site:collegedunia.com OR site:getmyuni.com) "Polaris School of Technology"',
    ]
    posts = []
    for q in queries:
        posts.extend(fetch_serper("portal", q, num=20))
    seen = set()
    deduped = [p for p in posts if not (p["url"] in seen or seen.add(p["url"]))]
    log(f"  Portals total: {len(deduped)}")
    return deduped

# ── Web (general brand mentions) ───────────────────────────────────────────
def fetch_web():
    queries = [
        '"Polaris School of Technology" review',
        '"Polaris School of Technology" BTech AI 2026',
        '"VibeCon" Polaris',
        '"Polaris" "Lyzr" OR "Agentathon"',
        '"GSoC" "Polaris School of Technology"',
    ]
    posts = []
    for q in queries:
        posts.extend(fetch_serper("web", q, num=8))
    seen = set()
    deduped = [p for p in posts if not (p["url"] in seen or seen.add(p["url"]))]
    log(f"  Web total: {len(deduped)}")
    return deduped

# ── YouTube ────────────────────────────────────────────────────────────────
def fetch_youtube():
    if not YOUTUBE_KEY:
        log("  YouTube: no API key — skipping")
        return []
    q = urllib.parse.quote("Polaris School of Technology")
    url = (f"https://www.googleapis.com/youtube/v3/search"
           f"?part=snippet&q={q}&type=video&maxResults=15"
           f"&order=relevance&key={YOUTUBE_KEY}")
    data = http_get(url)
    items = data.get("items") or []

    # Get real view counts for all video IDs
    video_ids = [i["id"]["videoId"] for i in items if i.get("id",{}).get("videoId")]
    view_counts = {}
    if video_ids:
        ids_str = ",".join(video_ids)
        stats_url = (f"https://www.googleapis.com/youtube/v3/videos"
                     f"?part=statistics&id={ids_str}&key={YOUTUBE_KEY}")
        stats_data = http_get(stats_url)
        for v in (stats_data.get("items") or []):
            vid = v.get("id","")
            stats = v.get("statistics",{})
            view_counts[vid] = int(stats.get("viewCount",0))

    posts = []
    for item in items:
        vid_id  = item.get("id",{}).get("videoId","")
        snippet = item.get("snippet",{}) or {}
        title   = snippet.get("title","")
        desc    = snippet.get("description","")
        views   = view_counts.get(vid_id, 0)
        posts.append({
            "platform":         "youtube",
            "text":             title[:200],
            "url":              f"https://youtube.com/watch?v={vid_id}" if vid_id else "",
            "date":             snippet.get("publishedAt") or now_iso(),
            "sentiment":        sentiment(f"{title} {desc}"),
            "campaign":         detect_campaign(f"{title} {desc}"),
            "score":            0,
            "impressions":      views,
            "impressions_type": "real_youtube_views",
            "scraped_at":       now_iso(),
        })
    log(f"  YouTube: {len(posts)} videos, views: {sum(view_counts.values()):,}")
    return posts

# ── History append ─────────────────────────────────────────────────────────
def append_history(all_mentions, path="data/history.json"):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception as e:
            log(f"  History read error: {e}")

    # Remove today's entry (idempotent re-runs)
    existing = [e for e in existing if e.get("date") != today]

    # Build per-platform, per-campaign counts
    by_platform = {}
    by_campaign  = {}
    total_impr   = 0
    sentiment_counts = {"positive":0,"neutral":0,"negative":0}

    for m in all_mentions:
        p = m.get("platform","unknown")
        by_platform[p] = by_platform.get(p, 0) + 1
        c = m.get("campaign") or "General"
        by_campaign[c]  = by_campaign.get(c, 0) + 1
        total_impr      += m.get("impressions", 0)
        s = m.get("sentiment","neutral")
        if s in sentiment_counts: sentiment_counts[s] += 1

    existing.append({
        "date":        today,
        "total":       len(all_mentions),
        "by_platform": by_platform,
        "by_campaign": by_campaign,
        "impressions": total_impr,
        "sentiment":   sentiment_counts,
        "scraped_at":  now_iso(),
    })
    existing = sorted(existing, key=lambda x: x["date"])[-365:]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    log(f"  History: {len(existing)} days recorded")

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    log("=== Polaris ORM Crawler v2 ===")

    all_mentions = []
    all_mentions.extend(fetch_reddit())
    all_mentions.extend(fetch_quora())
    all_mentions.extend(fetch_medium())
    all_mentions.extend(fetch_news())
    all_mentions.extend(fetch_portals())
    all_mentions.extend(fetch_web())
    all_mentions.extend(fetch_youtube())

    # Deduplicate by URL
    seen = set()
    final = []
    for m in all_mentions:
        key = (m.get("url") or "")[:100] or m.get("text","")[:60]
        if key and key not in seen:
            seen.add(key)
            final.append(m)

    # Sort newest first
    final.sort(key=lambda x: x.get("date",""), reverse=True)

    # Build summary
    by_platform = {}
    by_campaign  = {}
    total_impr   = 0
    sentiment_counts = {"positive":0,"neutral":0,"negative":0}
    for m in final:
        p = m.get("platform","unknown")
        by_platform[p] = by_platform.get(p,0)+1
        c = m.get("campaign") or "General"
        by_campaign[c]  = by_campaign.get(c,0)+1
        total_impr     += m.get("impressions",0)
        s = m.get("sentiment","neutral")
        if s in sentiment_counts: sentiment_counts[s]+=1

    summary = {
        "total_mentions": len(final),
        "platforms":      by_platform,
        "campaigns":      by_campaign,
        "sentiment":      sentiment_counts,
        "impressions":    total_impr,
        "impressions_note": "Mixed: Reddit=engagement_estimate, YouTube=real_views, others=search_position_estimate",
        "last_updated":   now_iso(),
    }

    log(f"\n=== TOTAL: {len(final)} unique mentions ===")
    log("  By platform: " + ", ".join(f"{k}: {v}" for k,v in by_platform.items()))
    log("  By campaign: " + ", ".join(f"{k}: {v}" for k,v in by_campaign.items()))
    log("  Sentiment: " + str(sentiment_counts))
    log(f"  Total impressions: {total_impr:,}")

    os.makedirs("data", exist_ok=True)
    with open("data/mentions.json", "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    with open("data/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Append to history
    append_history(final, "data/history.json")

    log("Done. data/mentions.json, data/summary.json, data/history.json written.")

if __name__ == "__main__":
    main()
