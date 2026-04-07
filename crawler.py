"""
Polaris ORM — Social Listening Crawler
========================================
Runs via GitHub Actions (free, every 6 hours).
Crawls ALL public sources for "Polaris School of Technology" mentions.

Sources (fully automatic, no manual input):
  1. Reddit — Public JSON API (no key needed)
  2. Google News — RSS feed
  3. Quora — Public page crawling via Google
  4. Medium — Public page crawling via Google  
  5. YouTube — Public page crawling via Google
  6. Aggregators — Shiksha, CollegeDunia, CollegeDekho, Careers360, GetMyUni
  7. Social indexed — LinkedIn, Twitter, Facebook, Instagram (Google-indexed only)
  8. General web — Everything else

Each source writes to data/<platform>.json
A combined summary.json is generated at the end.
"""

import json
import os
import re
import time
import hashlib
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

# ─── CONFIG ───────────────────────────────────────────────────────
BRAND_NAME = "Polaris School of Technology"
BRAND_QUERIES = [
    '"Polaris School of Technology"',
    '"Polaris School" technology',
    'PST Pune technology college',
]
SHORT_QUERIES = [
    '"Polaris School of Technology"',
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


# ─── HELPERS ──────────────────────────────────────────────────────

def fetch(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    ⚠ Fetch failed: {url[:100]}… → {e}")
        return None


def uid(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]


def clean(text, maxlen=350):
    text = re.sub(r'<[^>]+>', '', str(text))
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:maxlen].rsplit(' ', 1)[0] + '…' if len(text) > maxlen else text


def sentiment(text):
    t = text.lower()
    pos = ["great","amazing","excellent","best","good","love","awesome","fantastic",
           "wonderful","innovative","recommend","top","leading","quality","perfect",
           "impressive","outstanding","brilliant","helpful","valuable","worth",
           "strong","proud","happy","satisfied","incredible","superb","opportunity",
           "career","placement","industry","hands-on","practical","cutting-edge"]
    neg = ["bad","worst","terrible","poor","scam","fraud","waste","horrible","awful",
           "disappointing","overrated","avoid","fake","misleading","useless","expensive",
           "regret","complaint","problem","issue","beware","mediocre","subpar","warning"]
    p = sum(1 for w in pos if re.search(r'\b'+w+r'\b', t))
    n = sum(1 for w in neg if re.search(r'\b'+w+r'\b', t))
    if p > n + 1: return "positive"
    if n > p + 1: return "negative"
    if p > 0 or n > 0: return "mixed"
    return "neutral"


def save(filename, mentions):
    out = {
        "last_crawled": datetime.now(timezone.utc).isoformat(),
        "source": filename.replace('.json',''),
        "total": len(mentions),
        "mentions": mentions,
    }
    path = DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"  ✅ {len(mentions):3d} mentions → {path}")
    return mentions


def ts_from_epoch(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════
#  1. REDDIT — Public JSON API
# ═══════════════════════════════════════════════════════════════════

def crawl_reddit():
    print("\n🔴 REDDIT")
    mentions = []
    seen = set()

    for q in BRAND_QUERIES:
        for sort in ["relevance", "new", "top", "comments"]:
            url = (f"https://www.reddit.com/search.json?"
                   f"q={urllib.parse.quote(q)}&sort={sort}&limit=100&t=all")
            raw = fetch(url)
            if not raw: continue
            try:
                children = json.loads(raw).get("data",{}).get("children",[])
            except: continue

            for c in children:
                d = c.get("data",{})
                pid = d.get("id","")
                if pid in seen: continue
                seen.add(pid)

                title = d.get("title","")
                body = d.get("selftext","")
                full = f"{title} {body}"

                if not any(bq.replace('"','').lower() in full.lower()
                           for bq in BRAND_QUERIES): continue

                subreddit = d.get("subreddit","")
                # Auto-categorise
                cat = "discussion"
                sub_lower = subreddit.lower()
                if any(x in sub_lower for x in ["indian_academia","college","jee","neet","education"]):
                    cat = "education"
                elif any(x in sub_lower for x in ["career","job","salary","placement"]):
                    cat = "career"
                elif any(x in sub_lower for x in ["review","ask","advice"]):
                    cat = "review"

                mentions.append({
                    "id": uid(pid),
                    "platform": "reddit",
                    "category": cat,
                    "subcategory": f"r/{subreddit}",
                    "type": "post",
                    "title": title,
                    "snippet": clean(body),
                    "url": f"https://reddit.com{d.get('permalink','')}",
                    "author": d.get("author","[deleted]"),
                    "score": d.get("score",0),
                    "comments": d.get("num_comments",0),
                    "date": ts_from_epoch(d.get("created_utc",0)),
                    "sentiment": sentiment(full),
                })
            time.sleep(2)

    # Also search comments
    for q in SHORT_QUERIES:
        url = (f"https://www.reddit.com/search.json?"
               f"q={urllib.parse.quote(q)}&type=comment&sort=new&limit=100&t=all")
        raw = fetch(url)
        if not raw: continue
        try:
            children = json.loads(raw).get("data",{}).get("children",[])
        except: continue
        for c in children:
            d = c.get("data",{})
            cid = d.get("id","")
            if cid in seen: continue
            seen.add(cid)
            body = d.get("body","")
            if "polaris" not in body.lower(): continue
            mentions.append({
                "id": uid(cid),
                "platform": "reddit",
                "category": "comment",
                "subcategory": f"r/{d.get('subreddit','')}",
                "type": "comment",
                "title": f"Comment in r/{d.get('subreddit','')}",
                "snippet": clean(body),
                "url": f"https://reddit.com{d.get('permalink','')}",
                "author": d.get("author","[deleted]"),
                "score": d.get("score",0),
                "comments": 0,
                "date": ts_from_epoch(d.get("created_utc",0)),
                "sentiment": sentiment(body),
            })
        time.sleep(2)

    return save("reddit.json", mentions)


# ═══════════════════════════════════════════════════════════════════
#  2. GOOGLE NEWS RSS
# ═══════════════════════════════════════════════════════════════════

def crawl_news():
    print("\n📰 GOOGLE NEWS")
    mentions = []
    seen = set()

    for q in SHORT_QUERIES:
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=en-IN&gl=IN&ceid=IN:en"
        raw = fetch(url)
        if not raw: continue
        try:
            root = ET.fromstring(raw)
        except: continue

        for item in root.findall(".//item"):
            link = item.findtext("link","")
            if link in seen: continue
            seen.add(link)

            title = clean(item.findtext("title",""), 200)
            desc = clean(item.findtext("description",""))
            source = item.findtext("source","")
            pub = item.findtext("pubDate","")

            # Categorise by source
            cat = "newspaper"
            src_lower = source.lower()
            if any(x in src_lower for x in ["times","hindu","express","ndtv","india today"]):
                cat = "national_media"
            elif any(x in src_lower for x in ["pune","maharashtra","local"]):
                cat = "local_media"
            elif any(x in src_lower for x in ["tech","digit","gadget","analytics"]):
                cat = "tech_media"
            else:
                cat = "press_release"

            mentions.append({
                "id": uid(link),
                "platform": "newspaper",
                "category": cat,
                "subcategory": source,
                "type": "article",
                "title": title,
                "snippet": desc,
                "url": link,
                "author": source,
                "date": pub,
                "sentiment": sentiment(f"{title} {desc}"),
            })

    return save("news.json", mentions)


# ═══════════════════════════════════════════════════════════════════
#  3–8. GOOGLE SEARCH BASED CRAWLERS
#  For Quora, Medium, YouTube, Aggregators, Social, General Web
# ═══════════════════════════════════════════════════════════════════

def google_crawl(site_filter, platform, default_cat="general", sub_name=None):
    """
    Uses Google search to find indexed pages mentioning Polaris on target site.
    Returns list of mention dicts.
    """
    mentions = []
    seen = set()

    for q in SHORT_QUERIES:
        search = f"{q} {site_filter}"
        encoded = urllib.parse.quote(search)
        # Fetch Google search results page
        url = f"https://www.google.com/search?q={encoded}&num=40&hl=en"
        raw = fetch(url)
        if not raw:
            time.sleep(3)
            continue

        # Extract result blocks using regex patterns on Google HTML
        # Pattern 1: <a href="/url?q=..." (standard results)
        urls_found = re.findall(r'/url\?q=(https?://[^&"]+)', raw)
        # Pattern 2: direct href matches
        urls_found += re.findall(r'href="(https?://(?:www\.)?'
                                  + re.escape(site_filter.replace('site:','').split()[0].replace('*',''))
                                  + r'[^"]*)"', raw)

        # Extract snippets (Google wraps them in various span classes)
        snippets = re.findall(
            r'<(?:span|div)[^>]*class="[^"]*(?:VwiC3b|IsZvec|s3v9rd|hgKElc)[^"]*"[^>]*>(.*?)</(?:span|div)>',
            raw, re.DOTALL
        )

        # Extract titles from <h3> tags
        titles = re.findall(r'<h3[^>]*>(.*?)</h3>', raw, re.DOTALL)

        for i, result_url in enumerate(urls_found):
            result_url = urllib.parse.unquote(result_url).split('&')[0]
            if result_url in seen: continue
            if 'google.com' in result_url: continue
            seen.add(result_url)

            # Get title and snippet
            title = clean(titles[i], 200) if i < len(titles) else ""
            if not title:
                # Derive from URL
                title = result_url.split('/')[-1].replace('-',' ').replace('_',' ').title()[:150]
            snippet = clean(snippets[i]) if i < len(snippets) else ""

            # Auto-categorise based on URL patterns
            cat = default_cat
            url_lower = result_url.lower()
            title_lower = title.lower()
            combined = f"{title_lower} {snippet.lower()}"

            if platform == "quora":
                if "review" in combined or "experience" in combined:
                    cat = "review"
                elif "vs" in combined or "compar" in combined:
                    cat = "comparison"
                elif "salary" in combined or "placement" in combined or "career" in combined:
                    cat = "placement"
                elif "admission" in combined or "fee" in combined or "eligib" in combined:
                    cat = "admission"
                else:
                    cat = "discussion"

            elif platform == "medium":
                if "review" in combined: cat = "review"
                elif "guide" in combined or "how" in combined: cat = "guide"
                elif "opinion" in combined or "thought" in combined: cat = "opinion"
                else: cat = "article"

            elif platform == "youtube":
                if "review" in combined: cat = "review"
                elif "tour" in combined or "campus" in combined: cat = "campus_tour"
                elif "placement" in combined: cat = "placement"
                elif "vlog" in combined or "day" in combined: cat = "student_vlog"
                else: cat = "video"

            mentions.append({
                "id": uid(result_url),
                "platform": sub_name or platform,
                "category": cat,
                "subcategory": default_cat if sub_name else cat,
                "type": "mention",
                "title": title,
                "snippet": snippet,
                "url": result_url,
                "author": "",
                "date": now_iso(),
                "sentiment": sentiment(f"{title} {snippet}"),
            })

        time.sleep(4)  # Be respectful to Google

    return mentions


def crawl_quora():
    print("\n❓ QUORA")
    m = google_crawl("site:quora.com", "quora", "discussion")
    return save("quora.json", m)


def crawl_medium():
    print("\n📝 MEDIUM")
    m = google_crawl("site:medium.com", "medium", "article")
    return save("medium.json", m)


def crawl_youtube():
    print("\n▶️  YOUTUBE")
    m = google_crawl("site:youtube.com", "youtube", "video")
    return save("youtube.json", m)


def crawl_aggregators():
    print("\n🏫 AGGREGATOR PORTALS")
    all_m = []
    portals = {
        "shiksha":      "site:shiksha.com",
        "collegedunia": "site:collegedunia.com",
        "collegedekho": "site:collegedekho.com",
        "careers360":   "site:careers360.com",
        "getmyuni":     "site:getmyuni.com",
        "naukri":       "site:naukri.com",
    }
    for name, filt in portals.items():
        print(f"  → {name}")
        m = google_crawl(filt, name, "aggregator", sub_name=name)
        # Tag with portal-specific subcategories
        for item in m:
            url = item["url"].lower()
            if "review" in url: item["subcategory"] = "review"
            elif "placement" in url: item["subcategory"] = "placement"
            elif "admission" in url: item["subcategory"] = "admission"
            elif "course" in url: item["subcategory"] = "courses"
            elif "fee" in url: item["subcategory"] = "fees"
            elif "cutoff" in url or "cut-off" in url: item["subcategory"] = "cutoff"
            elif "ranking" in url: item["subcategory"] = "ranking"
            else: item["subcategory"] = "listing"
        all_m.extend(m)
        time.sleep(3)
    return save("aggregators.json", all_m)


def crawl_social():
    print("\n📱 SOCIAL MEDIA (Google-indexed)")
    all_m = []
    socials = {
        "linkedin":  "site:linkedin.com",
        "twitter":   "site:twitter.com OR site:x.com",
        "facebook":  "site:facebook.com",
        "instagram": "site:instagram.com",
    }
    for name, filt in socials.items():
        print(f"  → {name}")
        m = google_crawl(filt, name, "social", sub_name=name)
        for item in m:
            url = item["url"].lower()
            if "/posts/" in url or "/post/" in url: item["subcategory"] = "post"
            elif "/company/" in url or "/school/" in url: item["subcategory"] = "official_page"
            elif "/in/" in url: item["subcategory"] = "profile_mention"
            elif "/reel" in url or "/p/" in url: item["subcategory"] = "content"
            else: item["subcategory"] = "mention"
        all_m.extend(m)
        time.sleep(4)
    return save("social.json", all_m)


def crawl_web():
    print("\n🌐 GENERAL WEB")
    exclude = ("-site:reddit.com -site:quora.com -site:medium.com "
               "-site:youtube.com -site:linkedin.com -site:twitter.com "
               "-site:facebook.com -site:instagram.com -site:shiksha.com "
               "-site:collegedunia.com -site:collegedekho.com -site:careers360.com "
               "-site:getmyuni.com -site:x.com -site:naukri.com")
    m = google_crawl(exclude, "web", "general")
    for item in m:
        url = item["url"].lower()
        if any(x in url for x in ["blog","article","post"]): item["category"] = "blog"
        elif any(x in url for x in ["forum","discuss","thread"]): item["category"] = "forum"
        elif any(x in url for x in ["news","press","media"]): item["category"] = "news"
        elif any(x in url for x in ["review","rating"]): item["category"] = "review"
    return save("web.json", m)


# ═══════════════════════════════════════════════════════════════════
#  SUMMARY GENERATOR
# ═══════════════════════════════════════════════════════════════════

def generate_summary(all_data):
    print("\n📊 GENERATING SUMMARY")
    by_platform = {}
    by_sentiment = {"positive":0, "negative":0, "neutral":0, "mixed":0}
    by_category = {}

    for m in all_data:
        p = m.get("platform","unknown")
        by_platform[p] = by_platform.get(p, 0) + 1

        s = m.get("sentiment","neutral")
        by_sentiment[s] = by_sentiment.get(s, 0) + 1

        c = m.get("category","general")
        by_category[c] = by_category.get(c, 0) + 1

    # Platform groups for dashboard
    platform_groups = {
        "content_platforms": {
            "platforms": ["reddit","quora","medium","youtube"],
            "count": sum(by_platform.get(p,0) for p in ["reddit","quora","medium","youtube"]),
        },
        "news_pr": {
            "platforms": ["newspaper"],
            "count": by_platform.get("newspaper", 0),
        },
        "aggregators": {
            "platforms": ["shiksha","collegedunia","collegedekho","careers360","getmyuni","naukri"],
            "count": sum(by_platform.get(p,0) for p in ["shiksha","collegedunia","collegedekho","careers360","getmyuni","naukri"]),
        },
        "social_media": {
            "platforms": ["linkedin","twitter","facebook","instagram"],
            "count": sum(by_platform.get(p,0) for p in ["linkedin","twitter","facebook","instagram"]),
        },
        "general_web": {
            "platforms": ["web"],
            "count": by_platform.get("web", 0),
        },
    }

    # Negative mentions list for alerts
    negative_mentions = [m for m in all_data if m.get("sentiment") == "negative"]

    summary = {
        "last_crawled": now_iso(),
        "total_mentions": len(all_data),
        "by_platform": by_platform,
        "by_sentiment": by_sentiment,
        "by_category": by_category,
        "platform_groups": platform_groups,
        "negative_alert_count": len(negative_mentions),
        "crawl_sources": [
            "Reddit Public JSON API",
            "Google News RSS",
            "Google Search → Quora",
            "Google Search → Medium",
            "Google Search → YouTube",
            "Google Search → Aggregators (6 portals)",
            "Google Search → Social Media (4 platforms)",
            "Google Search → General Web",
        ],
    }

    with open(DATA_DIR / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*55}")
    print(f"  CRAWL COMPLETE — {len(all_data)} total mentions")
    print(f"{'='*55}")
    for p, c in sorted(by_platform.items(), key=lambda x: -x[1]):
        print(f"  {p:20s} → {c:4d}")
    print(f"\n  Sentiment: ✅ {by_sentiment['positive']}  "
          f"⚠️ {by_sentiment['mixed']}  "
          f"— {by_sentiment['neutral']}  "
          f"🚨 {by_sentiment['negative']}")
    print(f"{'='*55}\n")


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  POLARIS ORM — Social Listening Crawler v2.0")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
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

    generate_summary(all_data)
    save("all_mentions.json", all_data)


if __name__ == "__main__":
    main()
