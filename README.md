# Polaris ORM Intelligence Dashboard

**Auto-crawls Reddit, Quora, YouTube, News, Medium, LinkedIn, Twitter, CollegeDunia, Shiksha & 10+ more sources every ~2.5 hours. Zero manual work after setup.**

Live at: `https://adirispol.github.io/ORM-Dashboard/`

---

## How It Works

```
GitHub Actions (every ~2.5h)
        ↓
  crawler.py runs
        ↓
  data/*.json saved to repo
        ↓
  Dashboard reads JSON files live
```

No server. No backend. No database. Everything runs free on GitHub.

---

## SETUP — Do This Once (15 minutes total)

### Step 1 — Fork or upload files to your repo

Your repo must have this structure:
```
ORM-Dashboard/
├── index.html           ← the dashboard (this file)
├── scripts/
│   └── crawler.py       ← the crawler
├── .github/
│   └── workflows/
│       └── crawl.yml    ← GitHub Actions schedule
├── data/
│   └── summary.json     ← placeholder (gets replaced by crawler)
└── README.md
```

Upload all files to your GitHub repo. Keep `data/summary.json` as the placeholder — the crawler will overwrite it.

---

### Step 2 — Enable GitHub Pages

1. Go to your repo on GitHub
2. Click **Settings** (top menu)
3. Click **Pages** (left sidebar)
4. Under **Source** → select **Deploy from a branch**
5. Branch: **main** · Folder: **/ (root)**
6. Click **Save**
7. Wait 1–2 minutes → your dashboard is live at `https://YOUR-USERNAME.github.io/ORM-Dashboard/`

---

### Step 3 — Enable GitHub Actions (crawler)

1. Go to your repo → click **Actions** tab
2. If you see "Workflows aren't running", click **"I understand my workflows, go ahead and enable them"**
3. Click **Polaris ORM Crawler** in the left sidebar
4. Click **Run workflow** → **Run workflow** (green button)
5. Watch it run — takes 3–5 minutes
6. When complete, check the **data/** folder in your repo — it now has reddit.json, quora.json, etc.
7. Refresh your dashboard — data appears immediately

**The crawler now runs automatically every ~2.5 hours forever.**

---

### Step 4 — Connect the dashboard to your repo

1. Open your live dashboard at `https://YOUR-USERNAME.github.io/ORM-Dashboard/`
2. Click **⚙ Settings** (top right)
3. Under **Crawler Data Source**, enter: `YOUR-USERNAME/ORM-Dashboard`
4. Click **Save & Apply**
5. Data loads immediately

---

### Step 5 — Add optional API keys (for better data)

The crawler works 100% free without any keys. These improve quality:

| Key | What it improves | Cost | Where to get it |
|---|---|---|---|
| YouTube API v3 | Full YouTube video + comment search | Free | console.cloud.google.com |
| NewsAPI | 70,000+ news sources vs just Google News RSS | Free | newsapi.org/register |
| SerpAPI | Better Google search results quality | $50/mo (50 free/day trial) | serpapi.com |
| Apify (Quora) | Full Quora coverage (vs ~30% via Google index) | ~$6/month | apify.com |
| Claude API | AI sentiment instead of keyword-based | ~₹500/mo | console.anthropic.com |

**To add keys properly (so the crawler uses them):**

1. Go to your repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** for each:
   - Name: `YOUTUBE_API_KEY` → Value: your key
   - Name: `NEWSAPI_KEY` → Value: your key
   - Name: `SERPAPI_KEY` → Value: your key
3. Also paste them in **⚙ Settings** on the dashboard (for status display)

---

### Step 6 — Connect your Google Sheets (My Content tab)

For your manually logged content (Reddit posts you made, Quora answers you wrote, etc.):

1. Open your Google Sheet
2. **File → Share → Publish to web → select the tab → Comma-separated values (.csv) → Publish**
3. Repeat for each tab you want to show
4. Copy your **Sheet ID** from the URL: `docs.google.com/spreadsheets/d/**[THIS PART]**/edit`
5. Find the **GID** in the tab URL: `#gid=**[THIS NUMBER]**` (0 = first tab)
6. In dashboard **⚙ Settings → My Content**, paste Sheet ID + GID for each platform
7. Click **Save & Apply**

---

## Crawler Sources (all automatic, no keys needed)

| Source | Method | Frequency |
|---|---|---|
| Reddit | Public JSON API (no key) | Every crawl |
| Google News | RSS feed | Every crawl |
| Quora | Google-indexed pages | Every crawl |
| Medium | Google-indexed pages | Every crawl |
| YouTube | Google-indexed pages | Every crawl |
| Shiksha, CollegeDunia, Careers360, GetMyUni, CollegeDekho, Naukri | Google search | Every crawl |
| LinkedIn, Twitter, Facebook, Instagram | Google-indexed only | Every crawl |
| General Web | Google search | Every crawl |

---

## Crawl Schedule

Runs at approximately: **12 AM, 2:30 AM, 5 AM, 7:30 AM, 10 AM, 12:30 PM, 3 PM, 5:30 PM, 8 PM, 10:30 PM IST** (every ~2.5 hours)

GitHub Actions free plan: 2,000 minutes/month. Each crawl takes ~5 minutes = ~300 runs/month = 1,500 minutes. Well within the free limit.

---

## Manual Trigger

To run the crawler immediately (e.g. after adding new API keys):
1. Go to repo → **Actions** → **Polaris ORM Crawler**
2. Click **Run workflow** → **Run workflow**

---

## File Structure After First Crawl

```
data/
├── summary.json       ← totals + sentiment breakdown (dashboard reads this first)
├── reddit.json        ← all Reddit mentions
├── quora.json         ← all Quora mentions
├── medium.json        ← all Medium mentions
├── youtube.json       ← all YouTube mentions
├── news.json          ← all news/PR mentions
├── aggregators.json   ← Shiksha, CollegeDunia, etc.
├── social.json        ← LinkedIn, Twitter, Instagram, Facebook
├── web.json           ← general web mentions
└── all_mentions.json  ← combined file (used for Live Listening tab)
```

---

## Troubleshooting

**Dashboard shows "No data yet"**
→ Check ⚙ Settings → repo path is correct (username/repo-name)
→ Check that crawler has run: go to Actions tab and look for green checkmarks

**Crawler failing in Actions**
→ Go to Actions → click the failed run → read the error log
→ Most common cause: repo permissions. Go to Settings → Actions → General → Workflow permissions → "Read and write permissions" → Save

**Google Sheets data not showing**
→ Make sure you published the tab as CSV (not "Web page")
→ Check Sheet ID and GID are correct
→ GID for first tab is always 0

**Data is stale (crawl dot is amber/red)**
→ Check Actions tab — crawler may have failed
→ Click Run workflow to trigger manually

---

*Polaris School of Technology · ORM Intelligence System v3.0 · April 2026*

