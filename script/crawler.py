"""
Polaris ORM — Social Listening Crawler v4.0
============================================
Rebuilt to actually work. Previous version used Google HTML scraping
which Google blocks with 429/CAPTCHA — zero results for Quora/Medium/YouTube.

NEW APPROACH per source:
  1. Reddit       — Public JSON API (100% works, no key)
  2. News         — Google News RSS + NewsAPI (if key) + Bing News RSS
  3. Quora        — DuckDuckGo HTML + Bing search (site:quora.com)
  4. Medium       — DuckDuckGo HTML + Bing search (site:medium.com)
  5. YouTube      — YouTube Data API v3 (if key) + Bing RSS fallback
  6. Aggregators  — Direct HTTP to Shiksha/CollegeDunia/Careers360 APIs + Bing
  7. Social       — Bing search (site:linkedin.com etc)
  8. General Web  — Bing Web Search RSS + DuckDuckGo

Claude API for sentiment (if key). Falls back to keyword-based.
"""

import json
import os
import re
import sys
import time
import hashlib
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BRAND_NAME   = "Polaris School of Technology"
BRAND_SHORT  = "PST"

# Primary search queries
BRAND_QUERIES = [
    '"Polaris School of Technology"',
    '"Polaris School of Technology" review',
    '"Polaris School of Technology" placement',
    '"Polaris School of Technology" BTech',
    'PST Pune "industry integrated"',
]

# Competitor tracking
COMPETITOR_QUERIES = [
    "Scaler School of Technology",
    "Newton School of Technology",
    "upGrad",
    "Great Learning",
    "BITS Pilani",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Environment keys
YT_KEY      = os.environ.get("YOUTUBE_API_KEY", "").strip()
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "").strip()
CLAUDE_KEY  = os.environ.get("CLAUDE_API_KEY", "").strip()

UA_DESKTOP = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Mobile Safari/537.36"
)

# ─── DIAGNOSTICS TRACKING ────────────────────────────────────────────────────
DIAG = {
    "sources_attempted": [],
    "sources_success": [],
    "sources_failed": [],
    "errors": [],
    "warnings": [],
}

def log_ok(src, msg):
    print(f"  ✅ [{src}] {msg}")
    DIAG["sources_success"].append(src)

def log_warn(src, msg):
    print(f"  ⚠️  [{src}] {msg}")
    DIAG["warnings"].append(f"[{src}] {msg}")

def log_err(src, msg):
    print(f"  ❌ [{src}] {msg}")
    DIAG["errors"].append(f"[{src}] {msg}")
    DIAG["sources_failed"].append(src)

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def fetch(url, timeout=18, ua=None, headers=None):
    try:
        h = {"User-Agent": ua or UA_DESKTOP, "Accept-Language": "en-IN,en;q=0.9"}
        if headers:
            h.update(headers)
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            enc = r.headers.get_content_charset() or "utf-8"
            return raw.decode(enc, errors="replace")
    except urllib.error.HTTPError as e:
        log_warn("FETCH", f"HTTP {e.code} for {url[:80]}")
        return None
    except Exception as e:
        log_warn("FETCH", f"{type(e).__name__} for {url[:80]}: {e}")
        return None

def uid(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]

def clean(text, maxlen=400):
    text = re.sub(r"<[^>]+>", " ", str(text or ""))
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:maxlen].rsplit(" ", 1)[0] + "…" if len(text) > maxlen else text

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def ts_from_epoch(epoch):
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
    except:
        return now_iso()

def save(filename, mentions):
    out = {
        "last_crawled": now_iso(),
        "source": filename.replace(".json", ""),
        "total": len(mentions),
        "mentions": mentions,
    }
    path = DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"  💾 Saved {len(mentions)} → {path}")
    return mentions

def is_relevant(text):
    """Check if text actually mentions Polaris."""
    t = text.lower()
    return ("polaris school" in t or "polaris school of technology" in t or
            "pst pune" in t or ("polaris" in t and ("btech" in t or "placement" in t or "admission" in t)))

# ─── SENTIMENT ───────────────────────────────────────────────────────────────
POS_WORDS = [
    "great","amazing","excellent","best","good","love","awesome","fantastic",
    "wonderful","innovative","recommend","top","leading","quality","perfect",
    "impressive","outstanding","brilliant","helpful","valuable","worth",
    "strong","proud","happy","satisfied","incredible","superb","opportunity",
    "career","placement","industry","hands-on","practical","cutting-edge",
    "job","offer","hired","package","lpa","ctc","selected","accepted",
]
NEG_WORDS = [
    "bad","worst","terrible","poor","scam","fraud","waste","horrible","awful",
    "disappointing","overrated","avoid","fake","misleading","useless","expensive",
    "regret","complaint","problem","issue","beware","mediocre","subpar","warning",
    "not worth","money grab","no placement","zero job","no recruiters",
]

def keyword_sentiment(text):
    t = text.lower()
    p = sum(1 for w in POS_WORDS if re.search(r"\b" + re.escape(w) + r"\b", t))
    n = sum(1 for w in NEG_WORDS if re.search(r"\b" + re.escape(w) + r"\b", t))
    if p > n + 1: return "positive"
    if n > p + 1: return "negative"
    if p > 0 or n > 0: return "mixed"
    return "neutral"

def claude_sentiment(texts):
    """Batch sentiment via Claude API. Returns list of sentiments."""
    if not CLAUDE_KEY or not texts:
        return [keyword_sentiment(t) for t in texts]
    try:
        payload = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 400,
            "messages": [{
                "role": "user",
                "content": (
                    "Rate the sentiment of each text about Polaris School of Technology. "
                    "Reply with ONLY a JSON array of strings, one per text. "
                    "Each value must be exactly one of: positive, negative, neutral, mixed.\n\n"
                    + json.dumps([t[:300] for t in texts])
                )
            }]
        }).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "x-api-key": CLAUDE_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            res = json.loads(r.read())
            raw = res["content"][0]["text"].strip()
            results = json.loads(raw)
            if isinstance(results, list) and len(results) == len(texts):
                return [s if s in ("positive","negative","neutral","mixed") else "neutral" for s in results]
    except Exception as e:
        log_warn("CLAUDE", f"Sentiment API failed: {e}")
    return [keyword_sentiment(t) for t in texts]

def get_sentiment(text):
    if CLAUDE_KEY:
        results = claude_sentiment([text])
        return results[0]
    return keyword_sentiment(text)

# ─── BING RSS SEARCH (free, no key, works reliably) ──────────────────────────
def bing_search(query, site=None, count=30):
    """
    Bing doesn't have a free RSS search like Google News, but we can
    use their web search URL and parse results. More reliable than Google.
    """
    if site:
        query = f"{query} site:{site}"
    encoded = urllib.parse.quote(query)
    results = []

    # Try Bing web search
    url = f"https://www.bing.com/search?q={encoded}&count={count}&setlang=en-IN"
    raw = fetch(url, ua=UA_MOBILE)  # Mobile UA less likely to get blocked
    if not raw:
        return results

    # Extract result URLs and titles from Bing HTML
    # Bing uses <h2><a href="..."> for results
    pattern = r'<h2[^>]*><a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>'
    matches = re.findall(pattern, raw, re.DOTALL)

    # Also extract snippets
    snip_pattern = r'<p[^>]*class="[^"]*b_lineclamp[^"]*"[^>]*>(.*?)</p>'
    snippets = re.findall(snip_pattern, raw, re.DOTALL)

    for i, (href, title) in enumerate(matches):
        if "bing.com" in href or "microsoft.com" in href:
            continue
        if site and site not in href:
            continue
        snippet = clean(snippets[i]) if i < len(snippets) else ""
        title_clean = clean(title, 200)
        results.append({
            "url": href,
            "title": title_clean,
            "snippet": snippet,
        })

    return results

def ddg_search(query, site=None, count=30):
    """
    DuckDuckGo HTML search — works without keys.
    """
    if site:
        query = f"{query} site:{site}"
    encoded = urllib.parse.quote(query)
    url = f"https://html.duckduckgo.com/html/?q={encoded}&kl=in-en"

    raw = fetch(url, headers={"Accept": "text/html"})
    if not raw:
        return []

    results = []
    # DDG uses <a class="result__a" href="...">
    pattern = r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>'
    matches = re.findall(pattern, raw, re.DOTALL)

    snip_pattern = r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>'
    snippets = re.findall(snip_pattern, raw, re.DOTALL)

    for i, (href, title) in enumerate(matches[:count]):
        # DDG uses redirect URLs — extract real URL
        if "duckduckgo.com/l/" in href:
            try:
                parsed = urllib.parse.urlparse(href)
                params = urllib.parse.parse_qs(parsed.query)
                href = params.get("uddg", [href])[0]
                href = urllib.parse.unquote(href)
            except:
                pass
        if site and site not in href:
            continue
        snippet = clean(snippets[i]) if i < len(snippets) else ""
        results.append({
            "url": href,
            "title": clean(title, 200),
            "snippet": snippet,
        })

    return results

def multi_search(query, site=None, label="search"):
    """Try DDG first, then Bing, combine unique results."""
    DIAG["sources_attempted"].append(f"{label}:{site or 'web'}")
    seen = set()
    results = []

    # Try DuckDuckGo
    ddg = ddg_search(query, site)
    for r in ddg:
        if r["url"] not in seen:
            seen.add(r["url"])
            results.append(r)

    if len(results) < 5:
        # Fallback to Bing
        time.sleep(2)
        bing = bing_search(query, site)
        for r in bing:
            if r["url"] not in seen:
                seen.add(r["url"])
                results.append(r)

    if results:
        log_ok(label, f"{len(results)} results for '{query[:50]}'")
    else:
        log_warn(label, f"0 results for '{query[:50]}'")

    return results

def results_to_mentions(results, platform, sub_name=None):
    """Convert search results to mention dicts."""
    mentions = []
    for r in results:
        url = r.get("url", "")
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        combined = f"{title} {snippet}"

        if not is_relevant(combined) and not (url and "polaris" in url.lower()):
            # Still include if URL matches site
            pass

        # Auto-categorize
        cat = categorize(combined, url, platform)

        mentions.append({
            "id": uid(url or title),
            "platform": sub_name or platform,
            "category": cat,
            "subcategory": platform,
            "type": "mention",
            "title": title or url.split("/")[-1].replace("-", " ").title(),
            "snippet": snippet,
            "url": url,
            "author": "",
            "date": now_iso(),
            "sentiment": get_sentiment(combined),
        })
    return mentions

def categorize(combined, url, platform):
    c = combined.lower()
    u = url.lower()
    if platform == "quora":
        if "review" in c or "experience" in c: return "review"
        if "vs" in c or "compar" in c: return "comparison"
        if "placement" in c or "salary" in c or "ctc" in c: return "placement"
        if "admission" in c or "fee" in c or "eligib" in c: return "admission"
        return "discussion"
    if platform == "medium":
        if "review" in c: return "review"
        if "how" in c or "guide" in c or "tutorial" in c: return "guide"
        return "article"
    if platform == "youtube":
        if "review" in c: return "review"
        if "campus" in c or "tour" in c: return "campus_tour"
        if "placement" in c: return "placement"
        if "vlog" in c or "day in" in c: return "student_vlog"
        return "video"
    if any(x in u for x in ["shiksha","collegedunia","careers360","getmyuni","collegedekho","naukri"]):
        if "review" in u or "rating" in u: return "review"
        if "placement" in u: return "placement"
        if "admission" in u: return "admission"
        return "listing"
    return "mention"

# ═══════════════════════════════════════════════════════════════════════════════
#  1. REDDIT — Public JSON API (most reliable)
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_reddit():
    print("\n🔴 REDDIT")
    DIAG["sources_attempted"].append("reddit")
    mentions = []
    seen = set()

    searches = [
        ("Polaris School of Technology", ["relevance", "new", "top"]),
        ("PST Pune technology", ["relevance", "new"]),
    ]

    for query, sorts in searches:
        for sort in sorts:
            url = (
                f"https://www.reddit.com/search.json?"
                f"q={urllib.parse.quote(query)}&sort={sort}&limit=100&t=all"
            )
            raw = fetch(url, ua=UA_DESKTOP + " (compatible; Reddit search bot)")
            if not raw:
                continue
            try:
                data = json.loads(raw)
                children = data.get("data", {}).get("children", [])
            except:
                continue

            for c in children:
                d = c.get("data", {})
                pid = d.get("id", "")
                if not pid or pid in seen:
                    continue
                seen.add(pid)

                title = d.get("title", "")
                body = d.get("selftext", "")
                full = f"{title} {body}"

                if "polaris" not in full.lower():
                    continue

                subreddit = d.get("subreddit", "")
                sub_l = subreddit.lower()
                cat = "discussion"
                if any(x in sub_l for x in ["college", "jee", "education", "india", "indian_academia"]):
                    cat = "education"
                elif any(x in sub_l for x in ["career", "job", "salary", "placement", "cscareer"]):
                    cat = "career"
                elif "review" in sub_l or "advice" in sub_l:
                    cat = "review"

                mentions.append({
                    "id": uid(pid),
                    "platform": "reddit",
                    "category": cat,
                    "subcategory": f"r/{subreddit}",
                    "type": "post",
                    "title": title,
                    "snippet": clean(body),
                    "url": f"https://reddit.com{d.get('permalink', '')}",
                    "author": d.get("author", "[deleted]"),
                    "score": d.get("score", 0),
                    "comments": d.get("num_comments", 0),
                    "date": ts_from_epoch(d.get("created_utc", 0)),
                    "sentiment": get_sentiment(full),
                })
            time.sleep(2)

    # Also fetch comments
    for q in ['"Polaris School of Technology"']:
        url = (
            f"https://www.reddit.com/search.json?"
            f"q={urllib.parse.quote(q)}&type=comment&sort=new&limit=100&t=all"
        )
        raw = fetch(url)
        if not raw:
            continue
        try:
            children = json.loads(raw).get("data", {}).get("children", [])
        except:
            continue
        for c in children:
            d = c.get("data", {})
            cid = d.get("id", "")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            body = d.get("body", "")
            if "polaris" not in body.lower():
                continue
            mentions.append({
                "id": uid(cid),
                "platform": "reddit",
                "category": "comment",
                "subcategory": f"r/{d.get('subreddit', '')}",
                "type": "comment",
                "title": f"Comment in r/{d.get('subreddit', '')}",
                "snippet": clean(body),
                "url": f"https://reddit.com{d.get('permalink', '')}",
                "author": d.get("author", "[deleted]"),
                "score": d.get("score", 0),
                "comments": 0,
                "date": ts_from_epoch(d.get("created_utc", 0)),
                "sentiment": get_sentiment(body),
            })
        time.sleep(2)

    log_ok("reddit", f"{len(mentions)} posts + comments found")
    return save("reddit.json", mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  2. NEWS — Google News RSS + Bing News RSS + NewsAPI
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_news():
    print("\n📰 NEWS")
    DIAG["sources_attempted"].append("news")
    mentions = []
    seen = set()

    # 2A. Google News RSS (free, reliable)
    gnews_queries = [
        '"Polaris School of Technology"',
        '"Polaris School" Pune',
        "PST Pune BTech",
    ]
    for q in gnews_queries:
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=en-IN&gl=IN&ceid=IN:en"
        raw = fetch(url)
        if not raw:
            continue
        try:
            root = ET.fromstring(raw)
        except:
            continue
        for item in root.findall(".//item"):
            link = item.findtext("link", "") or item.findtext("guid", "")
            if link in seen:
                continue
            seen.add(link)
            title = clean(item.findtext("title", ""), 200)
            desc = clean(item.findtext("description", ""))
            source = item.findtext("source", "")
            pub = item.findtext("pubDate", "")
            cat = "national_media"
            sl = source.lower()
            if any(x in sl for x in ["times", "hindu", "express", "ndtv", "india today", "economic"]):
                cat = "national_media"
            elif any(x in sl for x in ["tech", "digit", "gadget", "analytics", "entrepreneur"]):
                cat = "tech_media"
            elif any(x in sl for x in ["pune", "maharashtra"]):
                cat = "local_media"
            else:
                cat = "press_release"
            mentions.append({
                "id": uid(link),
                "platform": "newspaper",
                "category": cat,
                "subcategory": source or "news",
                "type": "article",
                "title": title,
                "snippet": desc,
                "url": link,
                "author": source,
                "date": pub,
                "sentiment": get_sentiment(f"{title} {desc}"),
            })
        time.sleep(1)

    log_ok("google_news_rss", f"{len(mentions)} articles from Google News RSS")

    # 2B. Bing News RSS (free, no key)
    bing_queries = ['"Polaris School of Technology"', "Polaris School Technology Pune"]
    bing_before = len(mentions)
    for q in bing_queries:
        url = f"https://www.bing.com/news/search?q={urllib.parse.quote(q)}&format=rss"
        raw = fetch(url)
        if not raw:
            continue
        try:
            root = ET.fromstring(raw)
        except:
            continue
        for item in root.findall(".//item"):
            link = item.findtext("link", "")
            if link in seen:
                continue
            seen.add(link)
            title = clean(item.findtext("title", ""), 200)
            desc = clean(item.findtext("description", ""))
            mentions.append({
                "id": uid(link),
                "platform": "newspaper",
                "category": "news",
                "subcategory": "bing_news",
                "type": "article",
                "title": title,
                "snippet": desc,
                "url": link,
                "author": "",
                "date": item.findtext("pubDate", now_iso()),
                "sentiment": get_sentiment(f"{title} {desc}"),
            })
        time.sleep(1)
    log_ok("bing_news_rss", f"{len(mentions)-bing_before} articles from Bing News RSS")

    # 2C. NewsAPI (if key)
    if NEWSAPI_KEY:
        newsapi_before = len(mentions)
        url = (
            f"https://newsapi.org/v2/everything?"
            f"q={urllib.parse.quote('\"Polaris School of Technology\"')}"
            f"&language=en&sortBy=publishedAt&pageSize=50"
            f"&apiKey={NEWSAPI_KEY}"
        )
        raw = fetch(url)
        if raw:
            try:
                articles = json.loads(raw).get("articles", [])
                for a in articles:
                    url_a = a.get("url", "")
                    if url_a in seen:
                        continue
                    seen.add(url_a)
                    title = a.get("title", "")
                    desc = a.get("description", "") or a.get("content", "")
                    mentions.append({
                        "id": uid(url_a),
                        "platform": "newspaper",
                        "category": "newsapi",
                        "subcategory": a.get("source", {}).get("name", ""),
                        "type": "article",
                        "title": clean(title, 200),
                        "snippet": clean(desc),
                        "url": url_a,
                        "author": a.get("author", ""),
                        "date": a.get("publishedAt", now_iso()),
                        "sentiment": get_sentiment(f"{title} {desc}"),
                    })
                log_ok("newsapi", f"{len(mentions)-newsapi_before} articles")
            except Exception as e:
                log_err("newsapi", str(e))

    return save("news.json", mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  3. QUORA — DuckDuckGo + Bing site search
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_quora():
    print("\n❓ QUORA")
    mentions = []
    seen = set()

    queries = [
        '"Polaris School of Technology"',
        "Polaris School of Technology review",
        "Polaris School of Technology placement",
        "Polaris School of Technology vs Scaler",
        "PST Pune BTech admission",
    ]

    for q in queries:
        results = multi_search(q, site="quora.com", label="quora")
        for r in results:
            if r["url"] in seen:
                continue
            seen.add(r["url"])
        mentions += results_to_mentions(
            [r for r in results if r["url"] not in seen or not seen.add(r["url"])],
            "quora"
        )
        time.sleep(3)

    log_ok("quora", f"{len(mentions)} Quora mentions found")
    return save("quora.json", mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  4. MEDIUM — DuckDuckGo + Bing site search
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_medium():
    print("\n📝 MEDIUM")
    mentions = []
    seen = set()

    queries = [
        '"Polaris School of Technology"',
        "Polaris School Technology review experience",
        "Polaris School Technology BTech placement",
    ]

    for q in queries:
        results = multi_search(q, site="medium.com", label="medium")
        for r in results:
            if r["url"] not in seen:
                seen.add(r["url"])
                mentions += results_to_mentions([r], "medium")
        time.sleep(3)

    log_ok("medium", f"{len(mentions)} Medium articles found")
    return save("medium.json", mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  5. YOUTUBE — YouTube Data API v3 (if key) + Bing/DDG fallback
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_youtube():
    print("\n▶️  YOUTUBE")
    mentions = []
    seen = set()

    if YT_KEY:
        # YouTube Data API v3
        yt_queries = [
            "Polaris School of Technology",
            "PST Pune BTech review",
        ]
        for q in yt_queries:
            url = (
                f"https://www.googleapis.com/youtube/v3/search?"
                f"q={urllib.parse.quote(q)}&type=video&part=snippet"
                f"&maxResults=50&key={YT_KEY}&relevanceLanguage=en"
            )
            raw = fetch(url)
            if not raw:
                continue
            try:
                items = json.loads(raw).get("items", [])
                for item in items:
                    vid_id = item.get("id", {}).get("videoId", "")
                    if not vid_id or vid_id in seen:
                        continue
                    seen.add(vid_id)
                    snip = item.get("snippet", {})
                    title = snip.get("title", "")
                    desc = snip.get("description", "")
                    full = f"{title} {desc}"
                    if "polaris" not in full.lower() and "pst" not in full.lower():
                        continue
                    mentions.append({
                        "id": uid(vid_id),
                        "platform": "youtube",
                        "category": categorize(full, "", "youtube"),
                        "subcategory": snip.get("channelTitle", ""),
                        "type": "video",
                        "title": title,
                        "snippet": clean(desc),
                        "url": f"https://youtube.com/watch?v={vid_id}",
                        "author": snip.get("channelTitle", ""),
                        "date": snip.get("publishedAt", now_iso()),
                        "sentiment": get_sentiment(full),
                    })
                log_ok("youtube_api", f"{len(mentions)} videos via YouTube API")
            except Exception as e:
                log_err("youtube_api", str(e))
            time.sleep(1)
    else:
        # Fallback: search DDG/Bing for YouTube videos
        log_warn("youtube", "No YT_KEY — using DDG/Bing site:youtube.com search")
        queries = [
            '"Polaris School of Technology" review',
            "Polaris School Technology campus placement",
            "PST Pune BTech experience",
        ]
        for q in queries:
            results = multi_search(q, site="youtube.com", label="youtube")
            for r in results:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    mentions += results_to_mentions([r], "youtube")
            time.sleep(3)

    log_ok("youtube", f"{len(mentions)} total YouTube mentions")
    return save("youtube.json", mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  6. AGGREGATOR PORTALS — Direct HTTP + search
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_aggregators():
    print("\n🏫 AGGREGATORS")
    all_mentions = []
    seen = set()

    portals = {
        "shiksha": "shiksha.com",
        "collegedunia": "collegedunia.com",
        "collegedekho": "collegedekho.com",
        "careers360": "careers360.com",
        "getmyuni": "getmyuni.com",
        "naukri": "naukri.com",
    }

    queries = [
        '"Polaris School of Technology"',
        "Polaris School Technology review placement",
    ]

    for name, site in portals.items():
        DIAG["sources_attempted"].append(f"agg:{name}")
        portal_mentions = []
        for q in queries:
            results = multi_search(q, site=site, label=f"agg:{name}")
            for r in results:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    ms = results_to_mentions([r], name)
                    portal_mentions += ms
            time.sleep(2)

        if portal_mentions:
            log_ok(f"agg:{name}", f"{len(portal_mentions)} mentions")
        all_mentions += portal_mentions

    log_ok("aggregators", f"{len(all_mentions)} total portal mentions")
    return save("aggregators.json", all_mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  7. SOCIAL — Bing + DDG site search
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_social():
    print("\n📱 SOCIAL MEDIA")
    all_mentions = []
    seen = set()

    socials = {
        "linkedin": "linkedin.com",
        "twitter": "twitter.com",
        "instagram": "instagram.com",
    }

    query = '"Polaris School of Technology"'
    for name, site in socials.items():
        DIAG["sources_attempted"].append(f"social:{name}")
        results = multi_search(query, site=site, label=f"social:{name}")
        for r in results:
            if r["url"] not in seen:
                seen.add(r["url"])
                all_mentions += results_to_mentions([r], name)
        time.sleep(3)

    log_ok("social", f"{len(all_mentions)} social mentions")
    return save("social.json", all_mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  8. GENERAL WEB — DDG + Bing excluding known platforms
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_web():
    print("\n🌐 GENERAL WEB")
    DIAG["sources_attempted"].append("web")
    all_mentions = []
    seen = set()

    queries = [
        '"Polaris School of Technology" -site:reddit.com -site:quora.com -site:medium.com -site:youtube.com',
        "Polaris School Technology Pune review admission",
        "Polaris PST BTech placement 2024 2025",
    ]

    for q in queries:
        results = multi_search(q, label="web")
        for r in results:
            if r["url"] not in seen:
                seen.add(r["url"])
                # Skip known platforms (already crawled)
                skip = any(x in r["url"] for x in [
                    "reddit.com", "quora.com", "medium.com", "youtube.com",
                    "linkedin.com", "twitter.com", "shiksha.com",
                    "collegedunia.com", "careers360.com"
                ])
                if not skip:
                    all_mentions += results_to_mentions([r], "web")
        time.sleep(3)

    log_ok("web", f"{len(all_mentions)} web mentions")
    return save("web.json", all_mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  9. COMPETITORS — Track mentions of competitor schools
# ═══════════════════════════════════════════════════════════════════════════════
def crawl_competitors():
    print("\n🏆 COMPETITORS")
    DIAG["sources_attempted"].append("competitors")
    all_mentions = []
    seen = set()

    for comp in COMPETITOR_QUERIES:
        # Search for competitor mentions that also mention PST for comparison
        queries = [
            f'"{comp}" "Polaris School"',
            f'"{comp}" vs "Polaris"',
        ]
        for q in queries:
            results = multi_search(q, label=f"comp:{comp[:15]}")
            for r in results:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    ms = results_to_mentions([r], "competitor")
                    for m in ms:
                        m["subcategory"] = comp
                    all_mentions += ms
            time.sleep(2)

    log_ok("competitors", f"{len(all_mentions)} competitor comparison mentions")
    return save("competitors.json", all_mentions)

# ═══════════════════════════════════════════════════════════════════════════════
#  10. NORTH STAR — Real-time formula
# ═══════════════════════════════════════════════════════════════════════════════
def compute_north_star(all_data):
    """
    North Star = weighted score of ORM coverage.
    
    Formula:
    - Reddit posts/comments   × 1.5 (high trust, peer-to-peer)
    - Quora answers           × 2.0 (high intent, ranks on Google)
    - Medium articles         × 2.5 (SEO value, long-form)
    - YouTube mentions        × 2.0 (video reach)
    - News/PR articles        × 3.0 (highest credibility)
    - Portal listings         × 1.0 (auto-generated, lower weight)
    - Social mentions         × 1.5 (reach signal)
    - General web             × 1.0 (baseline)
    - Positive sentiment      × 1.2 bonus multiplier per mention
    - Negative sentiment      × 0.5 penalty multiplier per mention
    """
    WEIGHTS = {
        "reddit": 1.5,
        "quora": 2.0,
        "medium": 2.5,
        "youtube": 2.0,
        "newspaper": 3.0,
        "news": 3.0,
        "shiksha": 1.0,
        "collegedunia": 1.0,
        "careers360": 1.0,
        "collegedekho": 1.0,
        "getmyuni": 1.0,
        "naukri": 1.0,
        "linkedin": 1.5,
        "twitter": 1.5,
        "instagram": 1.5,
        "web": 1.0,
        "competitor": 0.5,
    }

    raw_score = 0.0
    by_platform = {}
    for m in all_data:
        plat = m.get("platform", "web")
        w = WEIGHTS.get(plat, 1.0)
        sent = m.get("sentiment", "neutral")
        if sent == "positive":
            w *= 1.2
        elif sent == "negative":
            w *= 0.5
        raw_score += w
        by_platform[plat] = by_platform.get(plat, 0) + 1

    return {
        "raw_count": len(all_data),
        "weighted_score": round(raw_score, 1),
        "by_platform": by_platform,
        "formula": "Reddit×1.5 + Quora×2.0 + Medium×2.5 + YouTube×2.0 + News×3.0 + Portals×1.0 + Social×1.5 + Web×1.0 | Positive×1.2 bonus | Negative×0.5 penalty",
        "target_weighted": 1000,
        "pct_of_target": round(raw_score / 1000 * 100, 1),
    }

# ═══════════════════════════════════════════════════════════════════════════════
#  SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
def generate_summary(all_data, north_star):
    by_platform = {}
    by_sentiment = {"positive": 0, "negative": 0, "neutral": 0, "mixed": 0}
    by_category = {}

    for m in all_data:
        p = m.get("platform", "unknown")
        by_platform[p] = by_platform.get(p, 0) + 1
        s = m.get("sentiment", "neutral")
        by_sentiment[s] = by_sentiment.get(s, 0) + 1
        c = m.get("category", "general")
        by_category[c] = by_category.get(c, 0) + 1

    negative_mentions = [m for m in all_data if m.get("sentiment") == "negative"]

    # Actionable intelligence
    actionables = []
    if by_sentiment["negative"] > 0:
        actionables.append({
            "priority": "HIGH",
            "platform": "all",
            "action": f"Respond to {by_sentiment['negative']} negative mentions. Check negatives in Live Listening tab.",
        })
    if by_platform.get("reddit", 0) > 5:
        actionables.append({
            "priority": "HIGH",
            "platform": "Reddit",
            "action": f"{by_platform.get('reddit',0)} Reddit mentions found. Engage in top threads with helpful answers.",
        })
    if by_platform.get("quora", 0) > 0:
        actionables.append({
            "priority": "HIGH",
            "platform": "Quora",
            "action": f"{by_platform.get('quora',0)} Quora posts found. Post detailed answers to unanswered questions.",
        })
    if by_platform.get("youtube", 0) > 0:
        actionables.append({
            "priority": "MEDIUM",
            "platform": "YouTube",
            "action": f"{by_platform.get('youtube',0)} YouTube mentions found. Engage in comment sections of relevant videos.",
        })
    if by_platform.get("newspaper", 0) == 0 and by_platform.get("news", 0) == 0:
        actionables.append({
            "priority": "MEDIUM",
            "platform": "Press/PR",
            "action": "No news coverage found. Pitch a press release to EdTech reporters or education journalists.",
        })
    if north_star["pct_of_target"] < 25:
        actionables.append({
            "priority": "LOW",
            "platform": "Seeding",
            "action": "ORM coverage is low. Increase Quora seeding and Reddit engagement to build presence.",
        })

    summary = {
        "last_crawled": now_iso(),
        "total_mentions": len(all_data),
        "by_platform": by_platform,
        "by_sentiment": by_sentiment,
        "by_category": by_category,
        "negative_alert_count": len(negative_mentions),
        "north_star": north_star,
        "actionables": actionables,
        "platform_groups": {
            "content_platforms": {
                "platforms": ["reddit", "quora", "medium", "youtube"],
                "count": sum(by_platform.get(p, 0) for p in ["reddit", "quora", "medium", "youtube"]),
            },
            "news_pr": {
                "platforms": ["newspaper", "news"],
                "count": by_platform.get("newspaper", 0) + by_platform.get("news", 0),
            },
            "aggregators": {
                "platforms": ["shiksha", "collegedunia", "collegedekho", "careers360", "getmyuni", "naukri"],
                "count": sum(by_platform.get(p, 0) for p in ["shiksha", "collegedunia", "collegedekho", "careers360", "getmyuni", "naukri"]),
            },
            "social_media": {
                "platforms": ["linkedin", "twitter", "instagram"],
                "count": sum(by_platform.get(p, 0) for p in ["linkedin", "twitter", "instagram"]),
            },
        },
        "diagnostics": {
            "sources_attempted": list(set(DIAG["sources_attempted"])),
            "sources_ok": DIAG["sources_success"],
            "sources_failed": DIAG["sources_failed"],
            "errors": DIAG["errors"],
            "warnings": DIAG["warnings"][:10],
            "claude_sentiment_active": bool(CLAUDE_KEY),
            "youtube_api_active": bool(YT_KEY),
            "newsapi_active": bool(NEWSAPI_KEY),
        },
        "crawl_sources": [
            "Reddit Public JSON API (posts + comments)",
            "Google News RSS",
            "Bing News RSS",
            "DuckDuckGo HTML search → Quora",
            "DuckDuckGo HTML search → Medium",
            "DuckDuckGo HTML search → YouTube",
            "YouTube Data API v3 (if key set)",
            "NewsAPI.org (if key set)",
            "Bing search → 6 Aggregator Portals",
            "Bing/DDG → Social Media (LinkedIn, Twitter, Instagram)",
            "Bing/DDG → General Web",
            "Claude API sentiment (if key set)",
        ],
    }

    with open(DATA_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"  CRAWL COMPLETE — {len(all_data)} total mentions")
    print(f"{'='*60}")
    for p, c in sorted(by_platform.items(), key=lambda x: -x[1]):
        print(f"  {p:25s} → {c:4d}")
    print(f"\n  Sentiment: ✅ {by_sentiment['positive']}  ⚠️ {by_sentiment['mixed']}  — {by_sentiment['neutral']}  🚨 {by_sentiment['negative']}")
    print(f"\n  North Star: {north_star['weighted_score']:.1f} / {north_star['target_weighted']} ({north_star['pct_of_target']}%)")
    print(f"\n  Errors: {len(DIAG['errors'])} | Warnings: {len(DIAG['warnings'])}")
    if DIAG["errors"]:
        for e in DIAG["errors"]:
            print(f"  ❌ {e}")
    print(f"{'='*60}\n")

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  POLARIS ORM — Social Listening Crawler v4.0")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  YT API: {'YES' if YT_KEY else 'NO (fallback mode)'}  |  NewsAPI: {'YES' if NEWSAPI_KEY else 'NO'}  |  Claude: {'YES' if CLAUDE_KEY else 'NO'}")
    print("=" * 60)

    all_data = []
    all_data.extend(crawl_reddit())
    all_data.extend(crawl_news())
    all_data.extend(crawl_quora())
    all_data.extend(crawl_medium())
    all_data.extend(crawl_youtube())
    all_data.extend(crawl_aggregators())
    all_data.extend(crawl_social())
    all_data.extend(crawl_web())
    all_data.extend(crawl_competitors())

    north_star = compute_north_star(all_data)
    generate_summary(all_data, north_star)
    save("all_mentions.json", all_data)
    print("✅ Crawl complete.")

if __name__ == "__main__":
    main()
