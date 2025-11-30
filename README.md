# Scrapers by ManzerX

Scrapers for gathering information about certain topics or people.

## Drimble Vuurwerk Scraper

Location: `article_scrapers/drimble_vw_scraper.py`

Description:

- Scrapes Drimble (https://drimble.nl) search results for the keyword `vuurwerk` and gathers matching articles.
- By default it seeds from the search results (`max_pages` pages) and optionally follows internal article links (BFS) to discover more pages.
- For each article that contains the keyword the scraper writes:
  - a row in `output_scrapers/drimble_vuurwerk.csv` (CSV columns include keyword-related fields), and
  - a per-article JSON file in `output_scrapers/articles/` (one JSON per article, named `{index}_{short-title}_{sha}.json`).

Features:

- Robust HTML extraction with BeautifulSoup (title, date, author, tags, main image, full text).
- Keyword-focused data: occurrences, surrounding contexts, sentences containing the keyword, nearby numbers and date-like patterns.
- Optional NLP named-entity extraction via spaCy (Dutch model `nl_core_news_sm`) — extracted entities are stored in the JSON and serialized into the CSV. The script runs without spaCy installed but will print a warning and skip entities.
- Configurable crawling: `follow_links`, `max_link_depth`, `max_links_per_article`, and global `max_total_articles`.

How to run (PowerShell):

```powershell
# install basic dependencies
pip install -r article_scrapers/requirements.txt

# (optional) install the Dutch spaCy model for better entity extraction
python -m spacy download nl_core_news_sm

& C:/Users/marco/AppData/Local/Microsoft/WindowsApps/python3.13.exe c:/Users/marco/OneDrive/Desktop/scrapers/article_scrapers/drimble_vw_scraper.py
```

Configuration:

- Edit the call at the bottom of `drimble_vw_scraper.py` or import and call `scrape_vuurwerk_articles(...)` with custom args for `max_pages`, `follow_links`, `max_link_depth`, `save_json_all`, etc.

Notes:

- The scraper aims to be polite: it uses a short sleep between requests and has a configurable page limit. Always respect the target site's robots.txt and terms of use.
- If you want improvements (article-only URL filtering, batched spaCy processing for speed, or normalized date/number extraction), open an issue or ask for changes.

## Data Politie Scraper (experimental)

Location: `article_scrapers/data-politie-scraper.py`

Description:

- Prototype scraper that attempts to find the keyword `vuurwerk` on `data.politie.nl`.
- The site is a single-page application (SPA) that serves table-style results, so this scraper currently uses multiple strategies:
  - a gentle HTML crawler starting from the navigation page, following internal links,
  - a CKAN API probe (if the dataset exposes a CKAN API), and
  - a fallback that parses the dataset HTML page for direct resource links and streams small amounts of resource content to scan for matches.

Status:

- Experimental / Not finished: the scraper may not find results by default because the site loads table content dynamically via JavaScript and/or uses API endpoints that the crawler does not automatically discover. The script includes heuristics and fallbacks but requires further refinement to reliably reproduce the search results you see in the browser.

Work needed / Next steps:

- Capture the site's XHR/API request used by the SPA when you search (DevTools → Network → XHR) and I can call that API directly.
- Alternatively, add headless-browser rendering (Playwright or Selenium) to load the SPA and scrape the rendered tables.
- Improve CKAN/datastore handling to scan large dataset resources efficiently (streaming and sampling), and add better filtering for table-only results.

How to run (PowerShell):

```powershell
& C:/Users/marco/AppData/Local/Microsoft/WindowsApps/python3.13.exe c:/Users/marco/OneDrive/Desktop/scrapers/article_scrapers/data-politie-scraper.py
```

Notes:

- The script is intentionally conservative (polite delays, small download limits).
