name: Polaris ORM Crawler

on:
  schedule:
    - cron: '*/30 * * * *' # Runs every 30 minutes
  workflow_dispatch:
  push:
    branches: [main]

permissions:
  contents: write

jobs:
  crawl:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install pandas requests beautifulsoup4

      - name: Run Crawler
        run: python scripts/crawler.py  # Fixed path to look in scripts folder
        env:
          YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
          NEWSAPI_KEY: ${{ secrets.NEWSAPI_KEY }}

      - name: Commit data
        run: |
          git config --global user.name 'github-actions'
          git config --global user.email 'github-actions@github.com'
          git add data/
          git commit -m "Automated data update" || exit 0
          git push
