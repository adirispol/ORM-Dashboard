"""
Polaris ORM Crawler v8.0
========================
Zero manual intervention. Runs every 30 min via GitHub Actions.

WHAT IT CRAWLS:
  1. Reddit        — Public JSON API. Real score, upvotes, comments count.
  2. News          — Google News RSS + Bing News RSS + NewsAPI (if key).
  3. Quora         — DuckDuckGo + Bing site:quora.com search. Estimated thread impressions.
  4. Medium        — DuckDuckGo + Bing site:medium.com. Estimated reads.
  5. YouTube       — YouTube Data API v3 (views/likes/comments). DDG fallback if no key.
                     Deep comment crawl: 6 videos/run, checkpoint-based, full crawl in ~5 days.
  6. Aggregators   — Shiksha, CollegeDunia, CollegeDekho, Careers360, GetMyUni, Naukri.
  7. Social        — DDG/Bing site:linkedin.com, site:twitter.com, site:instagram.com.
  8. General Web   — Broad web via DDG + Bing.
  9. Competitors   — 8 Bangalore/India BTech colleges tracked for comparison intel.

ENGAGEMENT ESTIMATES (where API not available):
  Quora:  thread_impressions = 1500 (question page), 800 (answer page)
          upvotes = 0 (no public API)
  Medium: reads = 500 (estimate), claps = 25 (estimate)
  Reddit: real score + real num_comments from Reddit JSON API
  YouTube: real views + likes + comment_count from YouTube API
  News:   publisher_reach per known outlet (TOI=35M, HT=18M, etc.)

SENTIMENT:
  - Keyword-based by default (fast, free)
  - Claude API (claude-haiku-4-5-20251001) if CLAUDE_API_KEY is set
    Batches of 20 texts per API call for efficiency

NORTH STAR (0-100 Brand Presence Score):
  Marketing-manager friendly score showing ORM health.
  A+ = 80-100, A = 65-79, B = 50-64, C = 35-49, D = 0-34

OUTPUTS (all in data/ folder):
  mentions.json     — all crawled mentions across all platforms
  summary.json      — aggregated stats, BPS score, actionables, diagnostics
  competitors.json  — competitor mentions + intel
  yt_checkpoint.json — YouTube comment crawl state (which videos done)
"""

import json, os, re, time, hashlib, urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════

BRAND_NAME  = "Polaris School of Technology"
BRAND_SHORT = "PST"

BRAND_QUERIES = [
    '"Polaris School of Technology"',
    '"Polaris School of Technology" review',
    '"Polaris School of Technology" placement',
    '"Polaris School of Technology" BTech',
    'PST Pune "industry integrated"',
]

# 8 Bangalore/India BTech competitors
COMPETITORS = {
    "Scaler School of Technology": "scaler_sot",
    "Newton School of Technology": "newton_sot",
    "PES University": "pes_univ",
    "RVCE Bangalore": "rvce",
    "BMS College of Engineering": "bms_coe",
    "Alliance University": "alliance_univ",
    "Jain University": "jain_univ",
    "CMR University": "cmr_univ",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# API Keys from GitHub Secrets
YT_KEY      = os.environ.get("YOUTUBE_API_KEY", "").strip()
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "").strip()
CLAUDE_KEY  = os.environ.get("CLAUDE_API_KEY", "").strip()

UA_DESK = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
UA_MOB  = ("Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36")

DIAG = {"attempted": [], "ok": [], "failed": [], "errors": [], "warnings": []}

# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════

def fetch(url, timeout=18, ua=None, extra_headers=None):
    try:
        h = {"User-Agent": ua or UA_DESK, "Accept-Language": "en-IN,en;q=0.9"}
        if extra_headers:
            h.update(extra_headers)
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            enc = r.headers.get_content_charset() or "utf-8"
            return r.read().decode(enc, errors="replace")
    except urllib.error.HTTPError as e:
        DIAG["warnings"].append(f"HTTP {e.code}: {url[:80]}")
        return None
    except Exception as e:
        DIAG["warnings"].append(f"{type(e).__name__}: {url[:80]}")
        return None

def uid(text):
    return hashlib.md5(str(text).encode()).hexdigest()[:12]

def clean(text, maxlen=400):
    text = re.sub(r"<[^>]+>", " ", str(text or ""))
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return (text[:maxlen].rsplit(" ", 1)[0] + "…") if len(text) > maxlen else text

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def ts_epoch(epoch):
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
    except:
        return now_iso()

def num_fmt(n):
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(n)

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except:
        return default if default is not None else {}

# ═══════════════════════════════════════════════════════════════════
#  SENTIMENT
# ═══════════════════════════════════════════════════════════════════

POS = ["great","amazing","excellent","best","good","love","awesome","fantastic",
       "wonderful","innovative","recommend","top","leading","quality","perfect",
       "impressive","outstanding","helpful","valuable","strong","proud","happy",
       "satisfied","incredible","superb","opportunity","placement","hands-on",
       "offer","hired","package","lpa","ctc","selected","practical","cutting-edge"]

NEG = ["bad","worst","terrible","poor","scam","fraud","waste","horrible","awful",
       "disappointing","overrated","avoid","fake","misleading","useless","expensive",
       "regret","complaint","problem","issue","beware","mediocre","subpar","warning",
       "not worth","money grab","no placement","zero job","no recruiters","cheated"]

def kw_sentiment(text):
    t = text.lower()
    p = sum(1 for w in POS if re.search(r"\b" + re.escape(w) + r"\b", t))
    n = sum(1 for w in NEG if re.search(r"\b" + re.escape(w) + r"\b", t))
    if p > n + 1: return "positive"
    if n > p + 1: return "negative"
    if p > 0 or n > 0: return "mixed"
    return "neutral"

# Cache to avoid re-calling Claude for same text
_sent_cache = {}

def batch_sentiment(texts):
    """Batch Claude sentiment or fallback to keyword."""
    results = []
    if not CLAUDE_KEY or not texts:
        return [kw_sentiment(t) for t in texts]

    # Check cache first
    uncached_indices = []
    uncached_texts = []
    for i, t in enumerate(texts):
        key = uid(t[:200])
        if key in _sent_cache:
            results.append(_sent_cache[key])
        else:
            results.append(None)
            uncached_indices.append(i)
            uncached_texts.append(t)

    if uncached_texts:
        # Process in batches of 20
        for batch_start in range(0, len(uncached_texts), 20):
            batch = uncached_texts[batch_start:batch_start+20]
            try:
                payload = json.dumps({
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "messages": [{
                        "role": "user",
                        "content": (
                            "Analyze sentiment of each text about Polaris School of Technology. "
                            "Reply ONLY with a JSON array. Each element must be exactly one of: "
                            "positive, negative, neutral, mixed.\n\n"
                            + json.dumps([t[:250] for t in batch])
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
                with urllib.request.urlopen(req, timeout=25) as r:
                    res = json.loads(r.read())
                    raw = res["content"][0]["text"].strip()
                    # Strip markdown fences if present
                    raw = re.sub(r"```[a-z]*", "", raw).strip()
                    sents = json.loads(raw)
                    valid = {"positive","negative","neutral","mixed"}
                    for i, s in enumerate(sents):
                        global_idx = uncached_indices[batch_start + i]
                        val = s if s in valid else kw_sentiment(uncached_texts[batch_start + i])
                        results[global_idx] = val
                        _sent_cache[uid(uncached_texts[batch_start + i][:200])] = val
                time.sleep(0.5)
            except Exception as e:
                DIAG["warnings"].append(f"Claude batch sentiment failed: {e}")
                for i in range(len(batch)):
                    global_idx = uncached_indices[batch_start + i]
                    results[global_idx] = kw_sentiment(uncached_texts[batch_start + i])

    # Fill any remaining Nones
    for i, r in enumerate(results):
        if r is None:
            results[i] = kw_sentiment(texts[i])
    return results

def get_sentiment(text):
    return batch_sentiment([text])[0]

# ═══════════════════════════════════════════════════════════════════
#  SEARCH ENGINE WRAPPERS (DDG + Bing — no keys needed)
# ═══════════════════════════════════════════════════════════════════

def ddg_search(query, site=None):
    q = f"{query} site:{site}" if site else query
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(q)}&kl=in-en"
    raw = fetch(url, extra_headers={"Accept": "text/html"})
    if not raw:
        return []
    results = []
    seen = set()
    urls = re.findall(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', raw, re.DOTALL)
    snippets = re.findall(r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>', raw, re.DOTALL)
    for i, (href, title) in enumerate(urls[:25]):
        if "duckduckgo.com/l/" in href:
            try:
                params = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                href = urllib.parse.unquote(params.get("uddg", [href])[0])
            except:
                pass
        if site and site not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        snippet = clean(snippets[i]) if i < len(snippets) else ""
        results.append({"url": href, "title": clean(title, 200), "snippet": snippet})
    return results

def bing_search(query, site=None):
    q = f"{query} site:{site}" if site else query
    url = f"https://www.bing.com/search?q={urllib.parse.quote(q)}&count=30&setlang=en-IN"
    raw = fetch(url, ua=UA_MOB)
    if not raw:
        return []
    results = []
    seen = set()
    matches = re.findall(r'<h2[^>]*><a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>', raw, re.DOTALL)
    snippets = re.findall(r'<p[^>]*class="[^"]*b_lineclamp[^"]*"[^>]*>(.*?)</p>', raw, re.DOTALL)
    for i, (href, title) in enumerate(matches[:25]):
        if "bing.com" in href or "microsoft.com" in href:
            continue
        if site and site not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        snippet = clean(snippets[i]) if i < len(snippets) else ""
        results.append({"url": href, "title": clean(title, 200), "snippet": snippet})
    return results

def multi_search(query, site=None, label="search"):
    DIAG["attempted"].append(f"{label}")
    seen_urls = set()
    results = []

    for r in ddg_search(query, site):
        if r["url"] not in seen_urls:
            seen_urls.add(r["url"])
            results.append(r)

    if len(results) < 5:
        time.sleep(1.5)
        for r in bing_search(query, site):
            if r["url"] not in seen_urls:
                seen_urls.add(r["url"])
                results.append(r)

    if results:
        DIAG["ok"].append(label)
        print(f"  ✓ [{label}] {len(results)} results")
    else:
        DIAG["warnings"].append(f"[{label}] 0 results for: {query[:50]}")
        print(f"  ⚠ [{label}] 0 results")
    return results

# ═══════════════════════════════════════════════════════════════════
#  CATEGORIZATION
# ═══════════════════════════════════════════════════════════════════

def categorize(text, url, platform):
    c = text.lower()
    u = url.lower()
    if platform == "quora":
        if "review" in c or "experience" in c: return "review"
        if " vs " in c or "compar" in c: return "comparison"
        if "placement" in c or "salary" in c or "ctc" in c or "lpa" in c: return "placement"
        if "admission" in c or "fee" in c or "eligib" in c or "cutoff" in c: return "admission"
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

# ═══════════════════════════════════════════════════════════════════
#  ENGAGEMENT ESTIMATES (where real API not available)
# ═══════════════════════════════════════════════════════════════════

PUBLISHER_REACH = {
    "times of india": 35_000_000, "the hindu": 12_000_000,
    "hindustan times": 18_000_000, "ndtv": 22_000_000,
    "india today": 25_000_000, "economic times": 20_000_000,
    "mint": 8_000_000, "business standard": 6_000_000,
    "deccan herald": 3_000_000, "pune mirror": 1_500_000,
    "maharashtra times": 4_000_000, "techcrunch": 10_000_000,
    "analytics india": 500_000, "yourstory": 2_000_000,
    "entrackr": 300_000,
}

def estimate_impressions(platform, url="", source="", category=""):
    """Return estimated impressions integer for a mention."""
    if platform == "reddit":
        return 0  # Calculated from score later
    if platform == "quora":
        if "question" in url.lower() or "q=" in url.lower():
            return 1500
        return 800
    if platform == "medium":
        return 500
    if platform in ("newspaper", "news"):
        s = source.lower()
        for pub, reach in PUBLISHER_REACH.items():
            if pub in s:
                return reach
        return 500_000  # default news reach
    if platform in ("shiksha","collegedunia","careers360","getmyuni","collegedekho","naukri"):
        return 2000
    if platform == "youtube":
        return 0  # real views from API
    if platform in ("linkedin","twitter","instagram"):
        return 1000
    return 500

# ═══════════════════════════════════════════════════════════════════
#  1. REDDIT — Public JSON API
# ═══════════════════════════════════════════════════════════════════

def crawl_reddit():
    print("\n🔴 REDDIT")
    mentions = []
    seen = set()

    searches = [
        ("Polaris School of Technology", ["relevance","new","top","comments"]),
        ("PST Pune technology BTech", ["relevance","new"]),
    ]
    for query, sorts in searches:
        for sort in sorts:
            url = (f"https://api.pushshift.io/reddit/search/submission"
                   f"q={urllib.parse.quote(query)}&sort={sort}&limit=100&t=all")
            raw = fetch(url, ua=UA_DESK + " bot")
            if not raw: continue
            try:
                children = json.loads(raw).get("data",{}).get("children",[])
            except: continue

            for c in children:
                d = c.get("data",{})
                pid = d.get("id","")
                if not pid or pid in seen: continue
                seen.add(pid)
                title = d.get("title","")
                body = d.get("selftext","")
                full = f"{title} {body}"
                if "polaris" not in full.lower(): continue

                subreddit = d.get("subreddit","")
                sl = subreddit.lower()
                cat = "discussion"
                if any(x in sl for x in ["college","jee","education","india","indian_academia","iit"]): cat = "education"
                elif any(x in sl for x in ["career","job","salary","placement","cscareer"]): cat = "career"
                elif "review" in sl or "advice" in sl: cat = "review"

                score = d.get("score", 0)
                num_comments = d.get("num_comments", 0)
                upvote_ratio = d.get("upvote_ratio", 0.5)
                # Reddit impressions estimate: score × 25 (typical upvote rate ~4%)
                impressions = max(score * 25, 100)

                mentions.append({
                    "id": uid(pid), "platform": "reddit",
                    "category": cat, "subcategory": f"r/{subreddit}",
                    "type": "post", "title": title, "snippet": clean(body),
                    "url": f"https://reddit.com{d.get('permalink','')}",
                    "author": d.get("author","[deleted]"),
                    "score": score, "upvote_ratio": upvote_ratio,
                    "comments": num_comments, "impressions": impressions,
                    "date": ts_epoch(d.get("created_utc",0)),
                    "sentiment": get_sentiment(full),
                })
            time.sleep(2)

    # Comments search
    url = (f"https://api.pushshift.io/reddit/search/submission"
           f'q={urllib.parse.quote(chr(34)+"Polaris School of Technology"+chr(34))}'
           f"&type=comment&sort=new&limit=100&t=all")
    raw = fetch(url)
    if raw:
        try:
            children = json.loads(raw).get("data",{}).get("children",[])
        except:
            children = []
        for c in children:
            d = c.get("data",{})
            cid = d.get("id","")
            if not cid or cid in seen: continue
            seen.add(cid)
            body = d.get("body","")
            if "polaris" not in body.lower(): continue
            score = d.get("score",0)
            mentions.append({
                "id": uid(cid), "platform": "reddit",
                "category": "comment", "subcategory": f"r/{d.get('subreddit','')}",
                "type": "comment", "title": f"Comment in r/{d.get('subreddit','')}",
                "snippet": clean(body),
                "url": f"https://reddit.com{d.get('permalink','')}",
                "author": d.get("author","[deleted]"),
                "score": score, "upvote_ratio": 0, "comments": 0,
                "impressions": max(score * 20, 50),
                "date": ts_epoch(d.get("created_utc",0)),
                "sentiment": get_sentiment(body),
            })
        time.sleep(2)

    DIAG["ok"].append("reddit")
    print(f"  ✓ {len(mentions)} Reddit posts+comments")
    return mentions

# ═══════════════════════════════════════════════════════════════════
#  2. NEWS — Google News RSS + Bing News RSS + NewsAPI
# ═══════════════════════════════════════════════════════════════════

def crawl_news():
    print("\n📰 NEWS")
    mentions = []
    seen = set()

    # Google News RSS
    for q in ['"Polaris School of Technology"', '"Polaris School" Pune', 'PST Pune BTech']:
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=en-IN&gl=IN&ceid=IN:en"
        raw = fetch(url)
        if not raw: continue
        try: root = ET.fromstring(raw)
        except: continue
        for item in root.findall(".//item"):
            link = item.findtext("link","") or item.findtext("guid","")
            if link in seen: continue
            seen.add(link)
            title = clean(item.findtext("title",""), 200)
            desc = clean(item.findtext("description",""))
            source = item.findtext("source","") or ""
            impressions = estimate_impressions("newspaper", source=source)
            mentions.append({
                "id": uid(link), "platform": "newspaper",
                "category": "news", "subcategory": source or "google_news",
                "type": "article", "title": title, "snippet": desc, "url": link,
                "author": source, "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": impressions,
                "date": item.findtext("pubDate", now_iso()),
                "sentiment": get_sentiment(f"{title} {desc}"),
            })
        time.sleep(1)

    # Bing News RSS
    before = len(mentions)
    for q in ['"Polaris School of Technology"', "Polaris School Technology Pune"]:
        url = f"https://www.bing.com/news/search?q={urllib.parse.quote(q)}&format=rss"
        raw = fetch(url)
        if not raw: continue
        try: root = ET.fromstring(raw)
        except: continue
        for item in root.findall(".//item"):
            link = item.findtext("link","")
            if link in seen: continue
            seen.add(link)
            title = clean(item.findtext("title",""), 200)
            desc = clean(item.findtext("description",""))
            mentions.append({
                "id": uid(link), "platform": "newspaper",
                "category": "news", "subcategory": "bing_news",
                "type": "article", "title": title, "snippet": desc, "url": link,
                "author": "Bing News", "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": 500_000,
                "date": item.findtext("pubDate", now_iso()),
                "sentiment": get_sentiment(f"{title} {desc}"),
            })
        time.sleep(1)
    print(f"  ✓ Bing News: {len(mentions)-before} articles")

    # NewsAPI (if key)
    if NEWSAPI_KEY:
        before2 = len(mentions)
        url = (f"https://newsapi.org/v2/everything?"
               f"q={urllib.parse.quote(chr(34)+'Polaris School of Technology'+chr(34))}"
               f"&language=en&sortBy=publishedAt&pageSize=50&apiKey={NEWSAPI_KEY}")
        raw = fetch(url)
        if raw:
            try:
                for a in json.loads(raw).get("articles",[]):
                    u = a.get("url","")
                    if u in seen: continue
                    seen.add(u)
                    title = a.get("title","")
                    desc = a.get("description","") or a.get("content","")
                    source = a.get("source",{}).get("name","")
                    mentions.append({
                        "id": uid(u), "platform": "newspaper",
                        "category": "newsapi", "subcategory": source,
                        "type": "article", "title": clean(title,200), "snippet": clean(desc),
                        "url": u, "author": a.get("author",""),
                        "score": 0, "upvote_ratio": 0, "comments": 0,
                        "impressions": estimate_impressions("newspaper", source=source),
                        "date": a.get("publishedAt", now_iso()),
                        "sentiment": get_sentiment(f"{title} {desc}"),
                    })
            except Exception as e:
                DIAG["errors"].append(f"NewsAPI: {e}")
        print(f"  ✓ NewsAPI: {len(mentions)-before2} articles")

    DIAG["ok"].append("news")
    print(f"  ✓ {len(mentions)} total news articles")
    return mentions

# ═══════════════════════════════════════════════════════════════════
#  3. QUORA — DDG + Bing site:quora.com
# ═══════════════════════════════════════════════════════════════════

def crawl_quora():
    print("\n❓ QUORA")
    mentions = []
    seen = set()

    queries = [
        '"Polaris School of Technology"',
        "Polaris School of Technology review experience",
        "Polaris School of Technology placement salary",
        "Polaris School of Technology vs Scaler",
        "PST Pune BTech admission fee",
    ]
    for q in queries:
        for r in multi_search(q, site="quora.com", label="quora"):
            if r["url"] in seen: continue
            seen.add(r["url"])
            combined = f"{r['title']} {r['snippet']}"
            cat = categorize(combined, r["url"], "quora")
            is_question = "/question/" in r["url"]
            impressions = 1500 if is_question else 800

            mentions.append({
                "id": uid(r["url"]), "platform": "quora",
                "category": cat, "subcategory": "quora",
                "type": "question" if is_question else "answer",
                "title": r["title"], "snippet": r["snippet"],
                "url": r["url"], "author": "",
                "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": impressions,
                "thread_impressions": impressions,
                "date": now_iso(),
                "sentiment": get_sentiment(combined),
            })
        time.sleep(3)

    DIAG["ok"].append("quora")
    print(f"  ✓ {len(mentions)} Quora mentions")
    return mentions

# ═══════════════════════════════════════════════════════════════════
#  4. MEDIUM — DDG + Bing site:medium.com
# ═══════════════════════════════════════════════════════════════════

def crawl_medium():
    print("\n📝 MEDIUM")
    mentions = []
    seen = set()

    queries = [
        '"Polaris School of Technology"',
        "Polaris School Technology review experience BTech",
        "Polaris School Technology placement career",
    ]
    for q in queries:
        for r in multi_search(q, site="medium.com", label="medium"):
            if r["url"] in seen: continue
            seen.add(r["url"])
            combined = f"{r['title']} {r['snippet']}"
            mentions.append({
                "id": uid(r["url"]), "platform": "medium",
                "category": categorize(combined, r["url"], "medium"),
                "subcategory": "medium",
                "type": "article", "title": r["title"], "snippet": r["snippet"],
                "url": r["url"], "author": "",
                "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": 500, "reads": 500, "claps": 25,
                "date": now_iso(),
                "sentiment": get_sentiment(combined),
            })
        time.sleep(3)

    DIAG["ok"].append("medium")
    print(f"  ✓ {len(mentions)} Medium articles")
    return mentions

# ═══════════════════════════════════════════════════════════════════
#  5. YOUTUBE — API v3 (real stats) + Comment Crawl Checkpoint
# ═══════════════════════════════════════════════════════════════════

def crawl_youtube():
    print("\n▶️  YOUTUBE")
    mentions = []
    seen_vids = set()

    if YT_KEY:
        # ─── 5A. Search for videos via YouTube Data API ─────────────
        yt_queries = [
            "Polaris School of Technology",
            "PST Pune BTech review campus",
        ]
        video_ids = []
        for q in yt_queries:
            url = (f"https://www.googleapis.com/youtube/v3/search?"
                   f"q={urllib.parse.quote(q)}&type=video&part=snippet"
                   f"&maxResults=50&key={YT_KEY}&relevanceLanguage=en")
            raw = fetch(url)
            if not raw: continue
            try:
                items = json.loads(raw).get("items",[])
                for item in items:
                    vid_id = item.get("id",{}).get("videoId","")
                    snip = item.get("snippet",{})
                    title = snip.get("title","")
                    desc = snip.get("description","")
                    full = f"{title} {desc}"
                    if "polaris" not in full.lower() and "pst" not in full.lower():
                        continue
                    if vid_id in seen_vids: continue
                    seen_vids.add(vid_id)
                    video_ids.append(vid_id)
                    mentions.append({
                        "id": uid(vid_id), "platform": "youtube",
                        "category": categorize(full,"","youtube"),
                        "subcategory": snip.get("channelTitle",""),
                        "type": "video", "title": title, "snippet": clean(desc),
                        "url": f"https://youtube.com/watch?v={vid_id}",
                        "author": snip.get("channelTitle",""),
                        "score": 0, "upvote_ratio": 0, "comments": 0,
                        "impressions": 0, "views": 0, "likes": 0,
                        "date": snip.get("publishedAt", now_iso()),
                        "sentiment": get_sentiment(full),
                        "video_id": vid_id,
                    })
            except Exception as e:
                DIAG["errors"].append(f"YouTube search: {e}")
            time.sleep(1)

        # ─── 5B. Fetch real video statistics ─────────────────────────
        if video_ids:
            # Batch up to 50 per request
            for i in range(0, len(video_ids), 50):
                batch_ids = ",".join(video_ids[i:i+50])
                url = (f"https://www.googleapis.com/youtube/v3/videos?"
                       f"id={batch_ids}&part=statistics&key={YT_KEY}")
                raw = fetch(url)
                if not raw: continue
                try:
                    items = json.loads(raw).get("items",[])
                    stats_map = {}
                    for item in items:
                        vid_id = item.get("id","")
                        stats = item.get("statistics",{})
                        stats_map[vid_id] = {
                            "views": int(stats.get("viewCount",0)),
                            "likes": int(stats.get("likeCount",0)),
                            "comments": int(stats.get("commentCount",0)),
                        }
                    # Update mentions with real stats
                    for m in mentions:
                        if m.get("video_id") in stats_map:
                            s = stats_map[m["video_id"]]
                            m["views"] = s["views"]
                            m["likes"] = s["likes"]
                            m["comments"] = s["comments"]
                            m["impressions"] = s["views"]
                            m["score"] = s["likes"]
                except Exception as e:
                    DIAG["errors"].append(f"YouTube stats: {e}")
                time.sleep(1)

        # ─── 5C. YouTube Deep Comment Crawl (checkpoint-based) ───────
        yt_crawl_comments(video_ids)

    else:
        # Fallback: DDG/Bing site:youtube.com
        print("  ⚠ No YOUTUBE_API_KEY — using DDG/Bing fallback")
        DIAG["warnings"].append("YouTube API key not set — using search fallback")
        for q in ['"Polaris School of Technology" review', "Polaris School campus placement"]:
            for r in multi_search(q, site="youtube.com", label="youtube_ddg"):
                if r["url"] in seen_vids: continue
                seen_vids.add(r["url"])
                combined = f"{r['title']} {r['snippet']}"
                mentions.append({
                    "id": uid(r["url"]), "platform": "youtube",
                    "category": categorize(combined, r["url"], "youtube"),
                    "subcategory": "youtube", "type": "video",
                    "title": r["title"], "snippet": r["snippet"],
                    "url": r["url"], "author": "",
                    "score": 0, "upvote_ratio": 0, "comments": 0,
                    "impressions": 0, "views": 0, "likes": 0,
                    "date": now_iso(),
                    "sentiment": get_sentiment(combined),
                })
            time.sleep(3)

    DIAG["ok"].append("youtube")
    print(f"  ✓ {len(mentions)} YouTube mentions")
    return mentions

def yt_crawl_comments(video_ids):
    """
    Checkpoint-based YouTube comment crawl.
    Processes 6 videos per GitHub Actions run.
    Full crawl completes in ~5-6 days depending on total video count.
    Stores Polaris-mentioning comments to data/yt_comments.json.
    """
    if not YT_KEY or not video_ids:
        return

    cp_path = DATA_DIR / "yt_checkpoint.json"
    cp = load_json(cp_path, {
        "done": [],
        "queue": [],
        "comments": [],
        "last_run": None,
        "total_crawled": 0,
        "total_comments": 0,
    })

    # Merge new video IDs into queue
    all_known = set(cp.get("done",[])) | set(cp.get("queue",[]))
    for vid in video_ids:
        if vid not in all_known:
            cp["queue"].append(vid)

    # Process up to 6 videos this run
    to_process = cp["queue"][:6]
    cp["queue"] = cp["queue"][6:]

    print(f"  🎥 YouTube comments: crawling {len(to_process)} videos | {len(cp['queue'])} in queue | {len(cp['done'])} done")

    new_comments = []
    for vid_id in to_process:
        next_page = None
        pages_fetched = 0
        while True:
            url = (f"https://www.googleapis.com/youtube/v3/commentThreads?"
                   f"videoId={vid_id}&part=snippet&maxResults=100&key={YT_KEY}"
                   f"&textFormat=plainText&order=relevance")
            if next_page:
                url += f"&pageToken={next_page}"
            raw = fetch(url)
            if not raw: break
            try:
                data = json.loads(raw)
            except: break

            items = data.get("items",[])
            for item in items:
                top = item.get("snippet",{}).get("topLevelComment",{}).get("snippet",{})
                text = top.get("textOriginal","") or top.get("textDisplay","")
                if not text: continue
                # Only store comments mentioning Polaris
                if "polaris" not in text.lower() and "pst" not in text.lower():
                    continue
                new_comments.append({
                    "video_id": vid_id,
                    "comment": clean(text, 500),
                    "author": top.get("authorDisplayName",""),
                    "likes": top.get("likeCount",0),
                    "date": top.get("publishedAt", now_iso()),
                    "sentiment": get_sentiment(text),
                })

            next_page = data.get("nextPageToken")
            pages_fetched += 1
            if not next_page or pages_fetched >= 5:  # Max 500 comments per video per run
                break
            time.sleep(0.5)

        cp["done"].append(vid_id)
        time.sleep(1)

    # Update checkpoint
    existing_comments = cp.get("comments",[])
    existing_ids = {c["video_id"]+c["comment"][:20] for c in existing_comments}
    for c in new_comments:
        key = c["video_id"] + c["comment"][:20]
        if key not in existing_ids:
            existing_comments.append(c)
            existing_ids.add(key)

    cp["comments"] = existing_comments[-2000:]  # Keep last 2000
    cp["last_run"] = now_iso()
    cp["total_crawled"] = len(cp["done"])
    cp["total_comments"] = len(cp["comments"])
    save_json(cp_path, cp)

    print(f"  ✓ YouTube comments: {len(new_comments)} new Polaris mentions | {len(existing_comments)} total stored")

# ═══════════════════════════════════════════════════════════════════
#  6. AGGREGATORS — Shiksha, CollegeDunia, Careers360, etc.
# ═══════════════════════════════════════════════════════════════════

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
    queries = ['"Polaris School of Technology"', "Polaris School Technology review placement"]

    for name, site in portals.items():
        portal_count = 0
        for q in queries:
            for r in multi_search(q, site=site, label=f"agg:{name}"):
                if r["url"] in seen: continue
                seen.add(r["url"])
                combined = f"{r['title']} {r['snippet']}"
                all_mentions.append({
                    "id": uid(r["url"]), "platform": name,
                    "category": categorize(combined, r["url"], name),
                    "subcategory": name, "type": "listing",
                    "title": r["title"], "snippet": r["snippet"],
                    "url": r["url"], "author": name,
                    "score": 0, "upvote_ratio": 0, "comments": 0,
                    "impressions": 2000,
                    "date": now_iso(),
                    "sentiment": get_sentiment(combined),
                })
                portal_count += 1
            time.sleep(2)

    DIAG["ok"].append("aggregators")
    print(f"  ✓ {len(all_mentions)} aggregator mentions")
    return all_mentions

# ═══════════════════════════════════════════════════════════════════
#  7. SOCIAL — LinkedIn, Twitter, Instagram (indexed only)
# ═══════════════════════════════════════════════════════════════════

def crawl_social():
    print("\n📱 SOCIAL")
    mentions = []
    seen = set()

    socials = {"linkedin": "linkedin.com", "twitter": "twitter.com", "instagram": "instagram.com"}
    q = '"Polaris School of Technology"'

    for name, site in socials.items():
        for r in multi_search(q, site=site, label=f"social:{name}"):
            if r["url"] in seen: continue
            seen.add(r["url"])
            combined = f"{r['title']} {r['snippet']}"
            mentions.append({
                "id": uid(r["url"]), "platform": name,
                "category": "social", "subcategory": name,
                "type": "post", "title": r["title"], "snippet": r["snippet"],
                "url": r["url"], "author": "",
                "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": 1000,
                "date": now_iso(),
                "sentiment": get_sentiment(combined),
            })
        time.sleep(3)

    DIAG["ok"].append("social")
    print(f"  ✓ {len(mentions)} social mentions")
    return mentions

# ═══════════════════════════════════════════════════════════════════
#  8. GENERAL WEB
# ═══════════════════════════════════════════════════════════════════

def crawl_web():
    print("\n🌐 GENERAL WEB")
    mentions = []
    seen = set()
    skip_domains = ["reddit.com","quora.com","medium.com","youtube.com",
                    "linkedin.com","twitter.com","instagram.com",
                    "shiksha.com","collegedunia.com","careers360.com",
                    "collegedekho.com","getmyuni.com","naukri.com"]

    for q in [
        '"Polaris School of Technology" -site:reddit.com -site:quora.com',
        "Polaris School Technology Pune review admission 2024 2025",
        "Polaris PST BTech placement package",
    ]:
        for r in multi_search(q, label="web"):
            if r["url"] in seen: continue
            if any(d in r["url"] for d in skip_domains): continue
            seen.add(r["url"])
            combined = f"{r['title']} {r['snippet']}"
            mentions.append({
                "id": uid(r["url"]), "platform": "web",
                "category": "mention", "subcategory": "web",
                "type": "webpage", "title": r["title"], "snippet": r["snippet"],
                "url": r["url"], "author": "",
                "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": 500,
                "date": now_iso(),
                "sentiment": get_sentiment(combined),
            })
        time.sleep(3)

    DIAG["ok"].append("web")
    print(f"  ✓ {len(mentions)} web mentions")
    return mentions

# ═══════════════════════════════════════════════════════════════════
#  9. COMPETITORS — 8 Bangalore BTech colleges
# ═══════════════════════════════════════════════════════════════════

def crawl_competitors():
    print("\n🏆 COMPETITORS")
    comp_data = {}

    for comp_name, comp_key in COMPETITORS.items():
        print(f"  → {comp_name}")
        comp_mentions = []
        seen = set()

        # Search for competitor solo (to understand their ORM presence)
        for q in [f'"{comp_name}"', f'"{comp_name}" review placement BTech']:
            for r in multi_search(q, label=f"comp:{comp_key}"):
                if r["url"] in seen: continue
                seen.add(r["url"])
                combined = f"{r['title']} {r['snippet']}"
                comp_mentions.append({
                    "id": uid(r["url"]), "competitor": comp_name,
                    "platform": _detect_platform(r["url"]),
                    "title": r["title"], "snippet": r["snippet"],
                    "url": r["url"],
                    "sentiment": get_sentiment(combined),
                    "date": now_iso(),
                    "mentions_polaris": "polaris" in combined.lower(),
                })
            time.sleep(2)

        by_plat = {}
        by_sent = {"positive":0,"negative":0,"neutral":0,"mixed":0}
        for m in comp_mentions:
            p = m["platform"]
            by_plat[p] = by_plat.get(p,0) + 1
            by_sent[m["sentiment"]] = by_sent.get(m["sentiment"],0) + 1

        comp_data[comp_key] = {
            "name": comp_name,
            "key": comp_key,
            "total_mentions": len(comp_mentions),
            "by_platform": by_plat,
            "by_sentiment": by_sent,
            "sentiment_score": (by_sent["positive"] - by_sent["negative"]) / max(len(comp_mentions),1),
            "mentions": comp_mentions[:20],  # Store top 20
        }
        print(f"    {len(comp_mentions)} mentions | +{by_sent['positive']} -{by_sent['negative']}")

    save_json(DATA_DIR / "competitors.json", {
        "last_crawled": now_iso(),
        "competitors": comp_data,
    })
    DIAG["ok"].append("competitors")
    print(f"  ✓ Competitor data saved")
    return []  # Don't add competitor data to main mentions

def _detect_platform(url):
    u = url.lower()
    for p in ["reddit","quora","medium","youtube","linkedin","twitter","instagram",
              "shiksha","collegedunia","careers360","collegedekho","getmyuni","naukri"]:
        if p in u: return p
    return "web"

# ═══════════════════════════════════════════════════════════════════
#  NORTH STAR — Brand Presence Score (0-100)
# ═══════════════════════════════════════════════════════════════════

def compute_bps(all_mentions):
    """
    Brand Presence Score (BPS) — 0 to 100.
    Designed for a marketing manager. Think of it as:
      - 0-34  = D: Brand barely exists online. Urgent action needed.
      - 35-49 = C: Growing but patchy. Need consistent seeding.
      - 50-64 = B: Decent presence. Some platforms weak.
      - 65-79 = A: Strong presence. Minor gaps.
      - 80-100 = A+: Excellent. Multi-platform, positive narrative.

    Formula factors:
      1. Platform coverage (are you on all 8 key platforms?)
      2. Volume per platform (weighted by trust/intent level)
      3. Sentiment modifier (positive boosts, negative hurts)
      4. Engagement quality (higher upvotes/views = more reach)
    """
    PLATFORM_WEIGHTS = {
        "newspaper": 3.0,   # Highest — editorial credibility
        "quora": 2.5,       # High intent, Google-ranked
        "medium": 2.0,      # SEO value, long-form
        "youtube": 2.0,     # Video reach
        "reddit": 1.5,      # Peer trust
        "linkedin": 1.5,    # Professional signal
        "twitter": 1.2,
        "instagram": 1.0,
        "shiksha": 1.2,     # Education portal
        "collegedunia": 1.2,
        "careers360": 1.2,
        "collegedekho": 1.0,
        "getmyuni": 1.0,
        "naukri": 0.8,
        "web": 0.8,
    }

    TARGET_SCORE = 1200  # What "100" maps to

    raw = 0.0
    by_platform = {}
    total_impressions = 0

    for m in all_mentions:
        plat = m.get("platform","web")
        w = PLATFORM_WEIGHTS.get(plat, 0.8)
        sent = m.get("sentiment","neutral")

        # Sentiment modifier
        if sent == "positive": w *= 1.3
        elif sent == "negative": w *= 0.3
        elif sent == "mixed": w *= 0.9
        # neutral = 1.0 (no change)

        # Engagement bonus
        impressions = m.get("impressions", 0)
        if impressions > 100_000: w *= 2.0
        elif impressions > 10_000: w *= 1.5
        elif impressions > 1_000: w *= 1.2

        raw += w
        by_platform[plat] = by_platform.get(plat, 0) + 1
        total_impressions += impressions

    # Coverage bonus: more platforms = more bonus
    key_platforms = {"newspaper","quora","medium","youtube","reddit","shiksha","collegedunia","careers360"}
    active_platforms = set(by_platform.keys()) & key_platforms
    coverage_ratio = len(active_platforms) / len(key_platforms)
    raw *= (0.5 + 0.5 * coverage_ratio)  # Max 1.5x bonus for full coverage

    bps = min(100, round(raw / TARGET_SCORE * 100, 1))

    # Grade
    if bps >= 80: grade = "A+"
    elif bps >= 65: grade = "A"
    elif bps >= 50: grade = "B"
    elif bps >= 35: grade = "C"
    else: grade = "D"

    return {
        "bps": bps, "grade": grade,
        "raw_score": round(raw, 1),
        "target": TARGET_SCORE,
        "total_mentions": len(all_mentions),
        "total_impressions": total_impressions,
        "total_impressions_fmt": num_fmt(total_impressions),
        "by_platform": by_platform,
        "coverage_platforms": len(active_platforms),
        "coverage_total": len(key_platforms),
        "formula": "Weighted by platform trust × sentiment × engagement | Coverage bonus for multi-platform presence",
        "grade_thresholds": {"A+": 80, "A": 65, "B": 50, "C": 35, "D": 0},
        "interpretation": {
            "A+": "Excellent ORM. Multi-platform positive narrative. Keep maintaining.",
            "A":  "Strong presence. Minor gaps on some platforms.",
            "B":  "Decent presence but patchy. Need consistent seeding.",
            "C":  "Growing but not enough. Increase Quora + Reddit activity.",
            "D":  "Brand barely visible online. Urgent action needed.",
        }.get(grade, ""),
    }

# ═══════════════════════════════════════════════════════════════════
#  ACTIONABLES ENGINE
# ═══════════════════════════════════════════════════════════════════

def generate_actionables(all_mentions, bps_data, comp_data=None):
    """
    Auto-generate prioritized actionables from crawl data.
    Returns list of {priority, platform, action, why, content_idea}.
    """
    by_platform = bps_data.get("by_platform",{})
    total = bps_data.get("total_mentions", 0)
    grade = bps_data.get("grade","D")
    actions = []

    # ── Negative mentions (always HIGH priority)
    neg = [m for m in all_mentions if m.get("sentiment")=="negative"]
    if neg:
        actions.append({
            "priority": "URGENT",
            "platform": "All",
            "action": f"Respond to {len(neg)} negative mention(s) found online",
            "why": "Negative content left unaddressed damages admission inquiries",
            "content_idea": "Write a factual, empathetic response. Acknowledge concern, state facts, invite DM",
            "count": len(neg),
        })

    # ── Reddit active threads
    reddit_count = by_platform.get("reddit", 0)
    if reddit_count >= 3:
        actions.append({
            "priority": "HIGH",
            "platform": "Reddit",
            "action": f"Engage in {reddit_count} Reddit thread(s) mentioning Polaris",
            "why": "Reddit threads rank on Google for years. Unanswered = bad impression",
            "content_idea": "Post detailed, helpful answer as a student/alumni. No overt promotion. Link to Polaris website naturally",
            "count": reddit_count,
        })

    # ── Quora coverage
    quora_count = by_platform.get("quora", 0)
    if quora_count >= 5:
        actions.append({
            "priority": "HIGH",
            "platform": "Quora",
            "action": f"{quora_count} Quora questions found. Answer unanswered ones first",
            "why": "Quora answers rank on Google. Each answer is passive SEO for admission keywords",
            "content_idea": "Write 300-word answer: personal story → curriculum → placement data → call to action",
            "count": quora_count,
        })
    elif quora_count == 0:
        actions.append({
            "priority": "HIGH",
            "platform": "Quora",
            "action": "No Quora presence found. Seed 3 questions and answer them",
            "why": "Quora is the #1 source for BTech decision-making in India",
            "content_idea": "'Is Polaris School of Technology good for BTech?' / 'What is PST Pune placement record?'",
            "count": 0,
        })

    # ── YouTube comments intelligence
    yt_cp = load_json(DATA_DIR / "yt_checkpoint.json", {})
    yt_comments = yt_cp.get("comments", [])
    yt_neg_comments = [c for c in yt_comments if c.get("sentiment")=="negative"]
    if yt_neg_comments:
        actions.append({
            "priority": "HIGH",
            "platform": "YouTube",
            "action": f"{len(yt_neg_comments)} negative comments found in YouTube videos",
            "why": "Comments visible to prospective students watching campus/review videos",
            "content_idea": "Reply with facts + positive framing. Pin official response on Polaris channel videos",
            "count": len(yt_neg_comments),
        })

    # ── News/PR gap
    news_count = by_platform.get("newspaper",0) + by_platform.get("news",0)
    if news_count == 0:
        actions.append({
            "priority": "HIGH",
            "platform": "News/PR",
            "action": "Zero news coverage found. Pitch a press release immediately",
            "why": "News backlinks are the fastest way to boost LLM citations (ChatGPT, Perplexity)",
            "content_idea": "Pitch: 'Polaris School of Technology launches [new program/partnership/placement record]' to YourStory, AnalyticsIndiaMag, Education Times",
            "count": 0,
        })

    # ── Medium articles
    medium_count = by_platform.get("medium",0)
    if medium_count == 0:
        actions.append({
            "priority": "MEDIUM",
            "platform": "Medium",
            "action": "No Medium articles found. Publish 2 this month",
            "why": "Medium articles index fast, rank for long-tail queries, and LLMs cite them",
            "content_idea": "'My BTech journey at Polaris School of Technology' / 'How Polaris prepares you for AI roles'",
            "count": 0,
        })

    # ── LLM/AEO actions
    actions.append({
        "priority": "MEDIUM",
        "platform": "LLM/AEO",
        "action": "Create Wikipedia page for Polaris School of Technology",
        "why": "ChatGPT, Perplexity, Gemini pull from Wikipedia as primary citation",
        "content_idea": "Draft: Founded year, programs (BTech Applied AI, PM&E, Cloud), AICTE recognition, campus, notable placements",
        "count": 0,
    })
    actions.append({
        "priority": "MEDIUM",
        "platform": "LLM/AEO",
        "action": "Add Polaris to Wikidata and Crunchbase",
        "why": "Wikidata feeds Google Knowledge Panel. Crunchbase feeds LLM training data",
        "content_idea": "Add: institution name, website, location, programs, year founded",
        "count": 0,
    })

    # ── BPS grade-based actions
    if grade in ("C","D"):
        actions.append({
            "priority": "MEDIUM",
            "platform": "ORM",
            "action": f"BPS Grade {grade} — Increase content seeding across 3 platforms simultaneously",
            "why": "Low ORM coverage means prospective students find nothing or find competitors",
            "content_idea": "Run 30-day blitz: 3 Quora answers/week + 1 Reddit thread/week + 1 Medium article every 2 weeks",
            "count": 0,
        })

    # ── Competitor intel actions
    if comp_data:
        competitors_obj = comp_data.get("competitors", {})
        top_comp = max(competitors_obj.values(), key=lambda x: x.get("total_mentions",0), default=None)
        if top_comp and top_comp["total_mentions"] > total:
            actions.append({
                "priority": "MEDIUM",
                "platform": "Competitive",
                "action": f"{top_comp['name']} has more online mentions. Target their comparison keywords",
                "why": "Students Googling 'Polaris vs Scaler' or 'PST vs Newton School' need to find you",
                "content_idea": "Publish Quora answers + Medium post: 'Polaris School of Technology vs [Competitor] — honest comparison'",
                "count": top_comp["total_mentions"],
            })

    return actions

# ═══════════════════════════════════════════════════════════════════
#  SUMMARY
# ═══════════════════════════════════════════════════════════════════

def generate_summary(all_mentions, bps_data, actionables):
    by_platform = {}
    by_sentiment = {"positive":0,"negative":0,"neutral":0,"mixed":0}
    by_category = {}
    total_impressions = 0

    for m in all_mentions:
        p = m.get("platform","unknown")
        by_platform[p] = by_platform.get(p,0) + 1
        s = m.get("sentiment","neutral")
        by_sentiment[s] = by_sentiment.get(s,0) + 1
        c = m.get("category","general")
        by_category[c] = by_category.get(c,0) + 1
        total_impressions += m.get("impressions",0)

    yt_cp = load_json(DATA_DIR / "yt_checkpoint.json", {})

    summary = {
        "last_crawled": now_iso(),
        "total_mentions": len(all_mentions),
        "total_impressions": total_impressions,
        "total_impressions_fmt": num_fmt(total_impressions),
        "by_platform": by_platform,
        "by_sentiment": by_sentiment,
        "by_category": by_category,
        "negative_alert_count": by_sentiment.get("negative",0),
        "bps": bps_data,
        "actionables": actionables,
        "yt_crawl_status": {
            "videos_done": yt_cp.get("total_crawled",0),
            "videos_queued": len(yt_cp.get("queue",[])),
            "polaris_comments_found": yt_cp.get("total_comments",0),
            "last_run": yt_cp.get("last_run","Never"),
        },
        "platform_groups": {
            "content": {
                "label": "Content Platforms",
                "platforms": ["reddit","quora","medium","youtube"],
                "count": sum(by_platform.get(p,0) for p in ["reddit","quora","medium","youtube"]),
            },
            "news": {
                "label": "News & PR",
                "platforms": ["newspaper","news"],
                "count": by_platform.get("newspaper",0)+by_platform.get("news",0),
            },
            "aggregators": {
                "label": "College Portals",
                "platforms": ["shiksha","collegedunia","collegedekho","careers360","getmyuni","naukri"],
                "count": sum(by_platform.get(p,0) for p in ["shiksha","collegedunia","collegedekho","careers360","getmyuni","naukri"]),
            },
            "social": {
                "label": "Social Media",
                "platforms": ["linkedin","twitter","instagram"],
                "count": sum(by_platform.get(p,0) for p in ["linkedin","twitter","instagram"]),
            },
        },
        "diagnostics": {
            "sources_attempted": list(set(DIAG["attempted"])),
            "sources_ok": list(set(DIAG["ok"])),
            "sources_failed": DIAG["failed"],
            "errors": DIAG["errors"][:20],
            "warnings": DIAG["warnings"][:20],
            "claude_sentiment_active": bool(CLAUDE_KEY),
            "youtube_api_active": bool(YT_KEY),
            "newsapi_active": bool(NEWSAPI_KEY),
        },
    }

    save_json(DATA_DIR / "summary.json", summary)

    # Print report
    print(f"\n{'='*62}")
    print(f"  POLARIS ORM CRAWL COMPLETE")
    print(f"  BPS: {bps_data['bps']}/100 (Grade {bps_data['grade']})  |  {len(all_mentions)} mentions  |  {num_fmt(total_impressions)} impressions")
    print(f"{'='*62}")
    for p, c in sorted(by_platform.items(), key=lambda x: -x[1]):
        print(f"  {p:25s} → {c:4d}")
    print(f"\n  Sentiment: ✅ {by_sentiment['positive']}  ⚠ {by_sentiment['mixed']}  — {by_sentiment['neutral']}  🚨 {by_sentiment['negative']}")
    print(f"\n  Actionables: {len(actionables)} generated")
    if DIAG["errors"]:
        print(f"\n  Errors ({len(DIAG['errors'])}):")
        for e in DIAG["errors"][:5]:
            print(f"    ✗ {e}")
    print(f"{'='*62}\n")

# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 62)
    print("  POLARIS ORM CRAWLER v8.0")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  YouTube API: {'✓' if YT_KEY else '✗ fallback'}  "
          f"NewsAPI: {'✓' if NEWSAPI_KEY else '✗'}  "
          f"Claude Sentiment: {'✓' if CLAUDE_KEY else '✗ keyword'}")
    print("=" * 62)

    all_mentions = []
    all_mentions.extend(crawl_reddit())
    all_mentions.extend(crawl_news())
    all_mentions.extend(crawl_quora())
    all_mentions.extend(crawl_medium())
    all_mentions.extend(crawl_youtube())
    all_mentions.extend(crawl_aggregators())
    all_mentions.extend(crawl_social())
    all_mentions.extend(crawl_web())
    crawl_competitors()

    # Apply batch Claude sentiment if key available (re-score all at end)
    if CLAUDE_KEY and all_mentions:
        print(f"\n🤖 Claude API sentiment — scoring {len(all_mentions)} mentions...")
        texts = [f"{m.get('title','')} {m.get('snippet','')}" for m in all_mentions]
        sentiments = batch_sentiment(texts)
        for m, s in zip(all_mentions, sentiments):
            m["sentiment"] = s
        print(f"  ✓ Claude sentiment done")

    # Deduplicate by ID
    seen_ids = set()
    deduped = []
    for m in all_mentions:
        if m["id"] not in seen_ids:
            seen_ids.add(m["id"])
            deduped.append(m)
    all_mentions = deduped

    save_json(DATA_DIR / "mentions.json", {
        "last_crawled": now_iso(),
        "total": len(all_mentions),
        "mentions": all_mentions,
    })

    bps_data = compute_bps(all_mentions)
    comp_data = load_json(DATA_DIR / "competitors.json", {})
    actionables = generate_actionables(all_mentions, bps_data, comp_data)
    generate_summary(all_mentions, bps_data, actionables)

    print("✅ All done.")

if __name__ == "__main__":
    main()
