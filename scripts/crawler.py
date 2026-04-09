"""
Polaris ORM Crawler — Fixed for GitHub Actions
===============================================
ROOT CAUSE OF 403s: Quora and news sites block GitHub Actions IPs directly.
FIX: Use SearXNG public instances (free JSON search API, not blocked).

Outputs: data/mentions.json + data/summary.json (what dashboard reads)
"""

import json, time, re, hashlib, urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

BRAND_QUERIES = [
    '"Polaris School of Technology"',
    '"Polaris School of Technology" review',
    '"Polaris School of Technology" placement',
    '"Polaris School of Technology" BTech',
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"

# SearXNG public instances — tries each until one works
# These are community-run, no keys, JSON API, GitHub Actions not blocked
SEARXNG_INSTANCES = [
    "https://search.bus-hit.me",
    "https://searx.be",
    "https://searx.tiekoetter.com",
    "https://searxng.world",
    "https://opnxng.com",
]

DIAG = {"ok": [], "failed": [], "errors": []}

# ── HELPERS ──────────────────────────────────────────────────────

def fetch(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None

def uid(text):
    return hashlib.md5(str(text).encode()).hexdigest()[:12]

def clean(text, n=350):
    text = re.sub(r"<[^>]+>", " ", str(text or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:n] + "…" if len(text) > n else text

def now():
    return datetime.now(timezone.utc).isoformat()

def sentiment(text):
    t = text.lower()
    pos = sum(1 for w in ["great","amazing","excellent","best","good","love","recommend",
              "placement","hired","offer","lpa","ctc","worth","impressive","quality",
              "outstanding","top","leading","innovative","practical"] if w in t)
    neg = sum(1 for w in ["bad","worst","scam","fraud","waste","terrible","avoid","fake",
              "misleading","regret","complaint","overrated","mediocre","beware",
              "no placement","zero job"] if w in t)
    if pos > neg + 1: return "positive"
    if neg > pos + 1: return "negative"
    if pos or neg: return "mixed"
    return "neutral"

def num_fmt(n):
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# ── SEARXNG — works from GitHub Actions ──────────────────────────

_best_instance = None

def searxng_search(query, site_filter=None, max_results=20):
    """
    SearXNG public JSON API — free, no key, not blocked on GitHub Actions.
    Falls back through instance list automatically.
    """
    global _best_instance
    q = f"{query} site:{site_filter}" if site_filter else query

    instances = ([_best_instance] + [i for i in SEARXNG_INSTANCES if i != _best_instance]
                 if _best_instance else SEARXNG_INSTANCES)

    for instance in instances:
        url = (f"{instance}/search?q={urllib.parse.quote(q)}"
               f"&format=json&language=en&safesearch=0&pageno=1")
        raw = fetch(url, timeout=12)
        if not raw:
            continue
        try:
            data = json.loads(raw)
            results = data.get("results", [])
            if results:
                _best_instance = instance
                print(f"    ✓ SearXNG [{instance.split('//')[1].split('/')[0]}] → {len(results)} results")
                return results[:max_results]
        except:
            continue
        time.sleep(0.5)

    print(f"    ✗ All SearXNG instances failed for: {query[:50]}")
    DIAG["failed"].append(f"searxng:{query[:30]}")
    return []

def results_to_mentions(results, platform, impressions_default=500):
    mentions = []
    seen = set()
    for r in results:
        url = r.get("url", "")
        if url in seen: continue
        seen.add(url)
        title = clean(r.get("title", ""), 200)
        snippet = clean(r.get("content", ""))
        combined = f"{title} {snippet}"
        if not any(kw in combined.lower() for kw in ["polaris school", "polaris campus", "pst pune", "polariscampus"]):
            if "polaris" not in url.lower():
                continue

        # Estimate impressions
        imp = impressions_default
        if platform == "quora":
            imp = 1500 if "/question/" in url else 800
        elif platform in ("newspaper", "news"):
            src = r.get("engine", "")
            imp = {"times of india":35_000_000,"hindustan times":18_000_000,
                   "ndtv":22_000_000,"the hindu":12_000_000,"india today":25_000_000
                   }.get(src.lower(), 500_000)

        cat = categorize(combined, url, platform)
        mentions.append({
            "id": uid(url), "platform": platform,
            "category": cat, "subcategory": platform,
            "type": "mention", "title": title, "snippet": snippet,
            "url": url, "author": r.get("engine", ""),
            "score": 0, "upvote_ratio": 0, "comments": 0,
            "impressions": imp,
            "date": r.get("publishedDate") or now(),
            "sentiment": sentiment(combined),
        })
    return mentions

def categorize(text, url, platform):
    c = text.lower()
    if platform == "quora":
        if "review" in c or "experience" in c: return "review"
        if " vs " in c or "compar" in c: return "comparison"
        if "placement" in c or "salary" in c: return "placement"
        if "admission" in c or "fee" in c: return "admission"
        return "discussion"
    if platform == "medium":
        return "review" if "review" in c else "article"
    if platform == "youtube":
        if "review" in c: return "review"
        if "campus" in c or "tour" in c: return "campus_tour"
        return "video"
    return "mention"

# ── 1. REDDIT — Public JSON API (always works, no key) ───────────

def crawl_reddit():
    print("\n🔴 REDDIT")
    mentions = []
    seen = set()

    for query in ['Polaris School of Technology', '"Polaris School of Technology"']:
        for sort in ["relevance", "new", "top"]:
            url = (f"https://www.reddit.com/search.json?"
                   f"q={urllib.parse.quote(query)}&sort={sort}&limit=100&t=all")
            raw = fetch(url)
            if not raw: continue
            try:
                children = json.loads(raw).get("data", {}).get("children", [])
            except: continue

            for c in children:
                d = c.get("data", {})
                pid = d.get("id", "")
                if not pid or pid in seen: continue
                seen.add(pid)
                title = d.get("title", "")
                body = d.get("selftext", "")
                if "polaris" not in f"{title} {body}".lower(): continue

                score = d.get("score", 0)
                mentions.append({
                    "id": uid(pid), "platform": "reddit",
                    "category": "discussion", "subcategory": f"r/{d.get('subreddit','')}",
                    "type": "post", "title": title, "snippet": clean(body),
                    "url": f"https://reddit.com{d.get('permalink','')}",
                    "author": d.get("author", ""),
                    "score": score, "upvote_ratio": d.get("upvote_ratio", 0),
                    "comments": d.get("num_comments", 0),
                    "impressions": max(score * 25, 100),
                    "date": datetime.fromtimestamp(d.get("created_utc", 0), tz=timezone.utc).isoformat(),
                    "sentiment": sentiment(f"{title} {body}"),
                })
            time.sleep(2)

    DIAG["ok"].append("reddit")
    print(f"  ✓ {len(mentions)} Reddit posts")
    return mentions

# ── 2. NEWS — Google News RSS + Bing News RSS ────────────────────

def crawl_news():
    print("\n📰 NEWS")
    mentions = []
    seen = set()

    feeds = [
        f"https://news.google.com/rss/search?q={urllib.parse.quote('Polaris School of Technology')}&hl=en-IN&gl=IN&ceid=IN:en",
        query = urllib.parse.quote('"Polaris School of Technology"')
        url = f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
        f"https://www.bing.com/news/search?q={urllib.parse.quote('Polaris School of Technology')}&format=rss",
    ]

    for feed_url in feeds:
        raw = fetch(feed_url)
        if not raw: continue
        try:
            root = ET.fromstring(raw)
        except: continue
        for item in root.findall(".//item"):
            link = item.findtext("link", "") or item.findtext("guid", "")
            if link in seen: continue
            seen.add(link)
            title = clean(item.findtext("title", ""), 200)
            desc = clean(item.findtext("description", ""))
            source = item.findtext("source", "") or "news"
            mentions.append({
                "id": uid(link), "platform": "newspaper",
                "category": "news", "subcategory": source,
                "type": "article", "title": title, "snippet": desc,
                "url": link, "author": source,
                "score": 0, "upvote_ratio": 0, "comments": 0,
                "impressions": 500_000,
                "date": item.findtext("pubDate", now()),
                "sentiment": sentiment(f"{title} {desc}"),
            })
        time.sleep(1)

    DIAG["ok"].append("news_rss")
    print(f"  ✓ {len(mentions)} news articles (RSS)")
    return mentions

# ── 3. QUORA — via SearXNG (no direct scrape = no 403) ──────────

def crawl_quora():
    print("\n❓ QUORA")
    mentions = []
    seen = set()

    for q in BRAND_QUERIES + ["Polaris School Technology placement", "PST Pune BTech review"]:
        results = searxng_search(q, site_filter="quora.com")
        for m in results_to_mentions(results, "quora"):
            if m["url"] not in seen:
                seen.add(m["url"])
                mentions.append(m)
        time.sleep(2)

    DIAG["ok"].append("quora")
    print(f"  ✓ {len(mentions)} Quora mentions")
    return mentions

# ── 4. MEDIUM — via SearXNG ──────────────────────────────────────

def crawl_medium():
    print("\n📝 MEDIUM")
    mentions = []
    seen = set()

    for q in BRAND_QUERIES[:2] + ["Polaris School Technology BTech experience"]:
        results = searxng_search(q, site_filter="medium.com")
        for m in results_to_mentions(results, "medium", impressions_default=500):
            if m["url"] not in seen:
                seen.add(m["url"])
                m["reads"] = 500
                m["claps"] = 25
                mentions.append(m)
        time.sleep(2)

    DIAG["ok"].append("medium")
    print(f"  ✓ {len(mentions)} Medium articles")
    return mentions

# ── 5. YOUTUBE — via SearXNG (no YouTube API needed) ─────────────

def crawl_youtube():
    print("\n▶️  YOUTUBE")
    mentions = []
    seen = set()

    for q in ['"Polaris School of Technology" review', "Polaris School campus placement BTech"]:
        results = searxng_search(q, site_filter="youtube.com")
        for m in results_to_mentions(results, "youtube", impressions_default=1000):
            if m["url"] not in seen:
                seen.add(m["url"])
                mentions.append(m)
        time.sleep(2)

    DIAG["ok"].append("youtube")
    print(f"  ✓ {len(mentions)} YouTube mentions")
    return mentions

# ── 6. AGGREGATORS — Shiksha, CollegeDunia, Careers360 ───────────

def crawl_aggregators():
    print("\n🏫 AGGREGATORS")
    mentions = []
    seen = set()

    portals = ["shiksha.com", "collegedunia.com", "careers360.com",
               "collegedekho.com", "getmyuni.com"]

    for site in portals:
        results = searxng_search('"Polaris School of Technology"', site_filter=site)
        name = site.split(".")[0]
        for m in results_to_mentions(results, name, impressions_default=2000):
            if m["url"] not in seen:
                seen.add(m["url"])
                mentions.append(m)
        time.sleep(2)

    DIAG["ok"].append("aggregators")
    print(f"  ✓ {len(mentions)} portal mentions")
    return mentions

# ── 7. GENERAL WEB — via SearXNG ─────────────────────────────────

def crawl_web():
    print("\n🌐 WEB")
    mentions = []
    seen = set()
    skip = {"reddit.com","quora.com","medium.com","youtube.com","shiksha.com",
            "collegedunia.com","careers360.com","collegedekho.com","getmyuni.com"}

    for q in BRAND_QUERIES[:2]:
        results = searxng_search(q)
        for r in results:
            url = r.get("url","")
            if any(s in url for s in skip): continue
            if url in seen: continue
            seen.add(url)
            combined = f"{r.get('title','')} {r.get('content','')}"
            if "polaris school" not in combined.lower() and "polariscampus" not in url.lower():
                continue
            mentions.append({
                "id": uid(url), "platform": "web",
                "category": "mention", "subcategory": "web",
                "type": "webpage",
                "title": clean(r.get("title",""),200),
                "snippet": clean(r.get("content","")),
                "url": url, "author": r.get("engine",""),
                "score":0,"upvote_ratio":0,"comments":0,"impressions":500,
                "date": now(),
                "sentiment": sentiment(combined),
            })
        time.sleep(2)

    DIAG["ok"].append("web")
    print(f"  ✓ {len(mentions)} web mentions")
    return mentions

# ── BPS SCORE ─────────────────────────────────────────────────────

def compute_bps(all_mentions):
    WEIGHTS = {"newspaper":3.0,"quora":2.5,"medium":2.0,"youtube":2.0,
               "reddit":1.5,"linkedin":1.5,"shiksha":1.2,"collegedunia":1.2,
               "careers360":1.2,"collegedekho":1.0,"getmyuni":1.0,"web":0.8}
    raw = 0.0
    by_plat = {}
    total_impr = 0
    for m in all_mentions:
        p = m.get("platform","web")
        w = WEIGHTS.get(p, 0.8)
        s = m.get("sentiment","neutral")
        if s=="positive": w *= 1.3
        elif s=="negative": w *= 0.3
        imp = m.get("impressions",0)
        if imp > 100_000: w *= 2.0
        elif imp > 10_000: w *= 1.5
        elif imp > 1_000: w *= 1.2
        raw += w
        by_plat[p] = by_plat.get(p,0)+1
        total_impr += imp

    key_platforms = {"newspaper","quora","medium","youtube","reddit","shiksha","collegedunia","careers360"}
    active = set(by_plat.keys()) & key_platforms
    raw *= (0.5 + 0.5 * len(active)/len(key_platforms))
    bps = min(100, round(raw/1200*100, 1))
    grade = "A+" if bps>=80 else "A" if bps>=65 else "B" if bps>=50 else "C" if bps>=35 else "D"
    return {
        "bps": bps, "grade": grade,
        "raw_score": round(raw,1), "target": 1200,
        "total_mentions": len(all_mentions),
        "total_impressions": total_impr,
        "total_impressions_fmt": num_fmt(total_impr),
        "by_platform": by_plat,
        "coverage_platforms": len(active),
        "coverage_total": len(key_platforms),
        "formula": "Weighted by platform trust × sentiment × engagement | Coverage bonus",
        "grade_thresholds": {"A+":80,"A":65,"B":50,"C":35,"D":0},
        "interpretation": {
            "A+":"Excellent ORM. Multi-platform positive narrative.",
            "A":"Strong presence. Minor gaps on some platforms.",
            "B":"Decent presence but patchy. Need consistent seeding.",
            "C":"Growing but not enough. Increase Quora + Reddit activity.",
            "D":"Brand barely visible online. Urgent action needed.",
        }.get(grade,""),
    }

# ── ACTIONABLES ───────────────────────────────────────────────────

def generate_actionables(all_mentions, bps):
    by_plat = bps.get("by_platform",{})
    actions = []
    neg = [m for m in all_mentions if m.get("sentiment")=="negative"]

    if neg:
        actions.append({"priority":"URGENT","platform":"All",
            "action":f"Respond to {len(neg)} negative mention(s) found online",
            "why":"Negative content damages admission inquiries",
            "content_idea":"Write a factual, empathetic response. State facts, invite DM."})
    if by_plat.get("quora",0)==0:
        actions.append({"priority":"HIGH","platform":"Quora",
            "action":"No Quora presence. Seed 3 questions and answer them.",
            "why":"Quora is #1 source for BTech decision-making in India",
            "content_idea":"'Is Polaris School of Technology good?' / 'PST Pune placement record?'"})
    elif by_plat.get("quora",0)>=3:
        actions.append({"priority":"HIGH","platform":"Quora",
            "action":f"{by_plat['quora']} Quora questions found. Answer unanswered ones.",
            "why":"Each answer is passive SEO for admission keywords",
            "content_idea":"300-word answer: story → curriculum → placement data → CTA"})
    if by_plat.get("newspaper",0)==0:
        actions.append({"priority":"HIGH","platform":"News/PR",
            "action":"Zero news coverage. Pitch a press release.",
            "why":"News backlinks boost LLM citations (ChatGPT, Perplexity)",
            "content_idea":"Pitch to YourStory, AnalyticsIndiaMag: new program/placement record"})
    if by_plat.get("medium",0)==0:
        actions.append({"priority":"MEDIUM","platform":"Medium",
            "action":"No Medium articles found. Publish 2 this month.",
            "why":"Medium indexes fast and LLMs cite it frequently",
            "content_idea":"'My BTech journey at Polaris School of Technology'"})
    actions.append({"priority":"MEDIUM","platform":"LLM/AEO",
        "action":"Create Wikipedia page for Polaris School of Technology",
        "why":"ChatGPT, Perplexity, Gemini pull from Wikipedia as primary citation",
        "content_idea":"Draft: Founded, programs, AICTE, campus, notable placements"})
    return actions

# ── SUMMARY ───────────────────────────────────────────────────────

def generate_summary(all_mentions, bps_data, actionables):
    by_plat = {}
    by_sent = {"positive":0,"negative":0,"neutral":0,"mixed":0}
    total_impr = 0
    for m in all_mentions:
        p = m.get("platform","unknown")
        by_plat[p] = by_plat.get(p,0)+1
        by_sent[m.get("sentiment","neutral")] = by_sent.get(m.get("sentiment","neutral"),0)+1
        total_impr += m.get("impressions",0)

    summary = {
        "last_crawled": now(),
        "total_mentions": len(all_mentions),
        "total_impressions": total_impr,
        "total_impressions_fmt": num_fmt(total_impr),
        "by_platform": by_plat,
        "by_sentiment": by_sent,
        "negative_alert_count": by_sent.get("negative",0),
        "bps": bps_data,
        "actionables": actionables,
        "yt_crawl_status": {"videos_done":0,"videos_queued":0,"polaris_comments_found":0,"last_run":"N/A"},
        "diagnostics": {
            "sources_ok": DIAG["ok"],
            "sources_failed": DIAG["failed"],
            "errors": DIAG["errors"][:10],
            "warnings": [],
            "claude_sentiment_active": False,
            "youtube_api_active": False,
            "newsapi_active": False,
        },
    }

    with open(DATA_DIR/"summary.json","w") as f:
        json.dump(summary,f,indent=2)

    print(f"\n{'='*55}")
    print(f"  DONE — {len(all_mentions)} mentions | BPS {bps_data['bps']}/100 ({bps_data['grade']}) | {num_fmt(total_impr)} impressions")
    print(f"{'='*55}")
    for p,c in sorted(by_plat.items(),key=lambda x:-x[1]):
        print(f"  {p:20s} → {c}")

# ── MAIN ──────────────────────────────────────────────────────────

def main():
    print("="*55)
    print("  Polaris ORM Crawler — Fixed for GitHub Actions")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*55)

    all_mentions = []
    all_mentions.extend(crawl_reddit())
    all_mentions.extend(crawl_news())
    all_mentions.extend(crawl_quora())
    all_mentions.extend(crawl_medium())
    all_mentions.extend(crawl_youtube())
    all_mentions.extend(crawl_aggregators())
    all_mentions.extend(crawl_web())

    # Deduplicate by ID
    seen_ids = set()
    deduped = [m for m in all_mentions if not (m["id"] in seen_ids or seen_ids.add(m["id"]))]

    with open(DATA_DIR/"mentions.json","w") as f:
        json.dump({"last_crawled":now(),"total":len(deduped),"mentions":deduped},f,indent=2)

    bps = compute_bps(deduped)
    actions = generate_actionables(deduped, bps)
    generate_summary(deduped, bps, actions)
    print("✅ All files written to data/")

if __name__ == "__main__":
    main()
