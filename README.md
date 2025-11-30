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

