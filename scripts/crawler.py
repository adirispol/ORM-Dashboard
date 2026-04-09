# Polaris ORM Crawler v13 (No Google API, No 403)

import json, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from pathlib import Path
from bs4 import BeautifulSoup

BRAND = "Polaris School of Technology"

QUERIES = [
    "Polaris School of Technology",
    "Polaris vs Scaler",
    "PST Pune BTech"
    "Polaris",
    "PST",
    "Polaris Campus",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print("ERROR:", e)
        return None

def now():
    return datetime.now(timezone.utc).isoformat()

# ================= REDDIT =================
def reddit_search(query):
    url = f"https://www.reddit.com/search.json?q={urllib.parse.quote(query)}&limit=5"
    raw = fetch(url)
    if not raw:
        return []

    results = []
    try:
        data = json.loads(raw)
        posts = data["data"]["children"]

        for p in posts:
            d = p["data"]
            results.append({
                "title": d["title"],
                "content": d.get("selftext", ""),
                "url": "https://reddit.com" + d["permalink"],
                "platform": "reddit",
                "date": now()
            })
    except:
        pass

    return results

# ================= QUORA =================
def quora_search(query):
    url = f"https://www.quora.com/search?q={urllib.parse.quote(query)}"
    html = fetch(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []

    for a in soup.find_all("a", href=True):
        link = a["href"]
        if "/question/" in link:
            results.append({
                "title": a.text.strip(),
                "url": "https://www.quora.com" + link,
                "platform": "quora",
                "date": now()
            })

    return results[:5]

# ================= NEWS (RSS) =================
def news_search(query):
    url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}"
    xml = fetch(url)
    if not xml:
        return []

    soup = BeautifulSoup(xml, "xml")
    results = []

    for item in soup.find_all("item")[:5]:
        results.append({
            "title": item.title.text,
            "url": item.link.text,
            "platform": "news",
            "date": now()
        })

    return results

# ================= MAIN =================
def run():
    print("🚀 Running ORM Crawler v13")

    all_data = []

    for q in QUERIES:
        print("🔎", q)

        all_data += reddit_search(q)
        all_data += quora_search(q)
        all_data += news_search(q)

        time.sleep(2)

    # Save
    with open(DATA_DIR / "output.json", "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"✅ DONE: {len(all_data)} records")


if __name__ == "__main__":
    run()
