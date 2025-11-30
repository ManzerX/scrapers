import csv
import os
import time
import re
import json
import hashlib
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from collections import deque

BASE_URL = "https://drimble.nl"
SEARCH_PATH = "/zoeken.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DrimbleVuurwerkScraper/1.0; +https://example.com)"
}

# Attempt to load spaCy NLP model for Dutch (optional)
_SPACY_NLP = None
try:
    import spacy
    try:
        # prefer a language-specific model; this requires the model to be installed separately
        _SPACY_NLP = spacy.load("nl_core_news_sm")
    except Exception:
        # fallback: try to load any default model name
        try:
            _SPACY_NLP = spacy.load("nl")
        except Exception:
            _SPACY_NLP = None
            print("[WARN] spaCy model not found. Install 'nl_core_news_sm' with: python -m spacy download nl_core_news_sm")
except Exception:
    _SPACY_NLP = None

# --- HULPFUNCTIES -----------------------------------------------------------

def get_soup(url, params=None, sleep=1.0):
    """Haalt een pagina op en geeft een BeautifulSoup-object terug."""
    time.sleep(sleep)  # beleefd crawlen
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def find_search_results(soup, query):
    """
    Zoekt in de zoekresultatenpagina naar Drimble-artikellinks.

    Omdat ik de HTML van de zoekresultaten hier niet kan inspecteren,
    gebruik ik een generieke strategie:
    - pak alle <a>-tags
    - filter op links binnen drimble.nl
    - filter op titel die het zoekwoord bevat
    """
    results = []

    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)

        # alleen Drimble-nieuws, geen externe sites
        if not full_url.startswith(BASE_URL):
            continue

        title = a.get_text(strip=True)
        if not title:
            continue

        if query.lower() not in title.lower():
            continue

        results.append({"title": title, "url": full_url})

    # dedup op URL
    seen = set()
    unique = []
    for r in results:
        if r["url"] in seen:
            continue
        seen.add(r["url"])
        unique.append(r)

    return unique


def extract_article_data(url, keyword):
    """
    Haalt data uit een losse artikelpagina:
    - titel (h1)
    - datum (best effort)
    - volledige tekst (best effort)
    - checkt of keyword in de tekst staat
    """
    try:
        soup = get_soup(url)
    except Exception as e:
        print(f"FOUT bij ophalen artikel {url}: {e}")
        return None

    # Titel
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # Datum: probeer een paar veelvoorkomende patronen
    date_text = ""
    # <time> element
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        date_text = time_tag.get("datetime")
    elif time_tag:
        date_text = time_tag.get_text(strip=True)

    if not date_text:
        # fallback op eventueel span/div met 'datum' in class
        date_candidate = soup.find(
            lambda tag: tag.name in ("span", "div", "p")
            and tag.get("class")
            and any("datum" in cls.lower() or "date" in cls.lower() for cls in tag.get("class"))
        )
        if date_candidate:
            date_text = date_candidate.get_text(strip=True)

    # Auteur: probeer meta, link rel=author of een author-class
    author = ""
    author_meta = soup.find("meta", attrs={"name": "author"}) or soup.find("meta", attrs={"property": "author"})
    if author_meta and author_meta.get("content"):
        author = author_meta.get("content").strip()
    else:
        link_author = soup.find("link", rel="author")
        if link_author and link_author.get("href"):
            author = link_author.get("href").strip()
        else:
            author_candidate = soup.find(lambda tag: tag.name in ("span", "div", "p") and tag.get("class") and any("author" in cls.lower() for cls in tag.get("class")))
            if author_candidate:
                author = author_candidate.get_text(strip=True)

    # Tags/keywords: meta keywords or tag list
    tags = []
    keywords_meta = soup.find("meta", attrs={"name": "keywords"})
    if keywords_meta and keywords_meta.get("content"):
        tags = [t.strip() for t in keywords_meta.get("content").split(",") if t.strip()]
    else:
        # zoek naar tag elements
        tag_nodes = soup.find_all(lambda tag: tag.name in ("a", "span") and tag.get("class") and any("tag" in cls.lower() or "keyword" in cls.lower() for cls in tag.get("class")))
        for tn in tag_nodes:
            ttxt = tn.get_text(strip=True)
            if ttxt:
                tags.append(ttxt)

    # Hoofdtekst: pak <article> of een generiek content-blok
    article_node = soup.find("article")
    if not article_node:
        article_node = soup.find("div", {"class": lambda c: c and "article" in c.lower()}) \
                       or soup.find("div", {"id": lambda i: i and "content" in i.lower()})

    if article_node:
        full_text = article_node.get_text(" ", strip=True)
    else:
        # als fallback de hele pagina (kan ruis geven)
        full_text = soup.get_text(" ", strip=True)

    contains_keyword = keyword.lower() in full_text.lower()

    # Klein fragment maken rondom het keyword
    snippet = ""
    lower_text = full_text.lower()
    idx = lower_text.find(keyword.lower())
    if idx != -1:
        start = max(0, idx - 80)
        end = min(len(full_text), idx + 80)
        snippet = full_text[start:end].strip()

    # Hoofdafbeelding: og:image of eerste <img> in artikel
    main_image = ""
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"):
        main_image = og_img.get("content").strip()
    else:
        if article_node:
            img = article_node.find("img")
            if img and img.get("src"):
                main_image = urljoin(url, img.get("src"))
        else:
            img = soup.find("img")
            if img and img.get("src"):
                main_image = urljoin(url, img.get("src"))

    # Woordentelling
    word_count = len(full_text.split()) if full_text else 0

    # Hulp: extra informatie rond het keyword
    def _extract_keyword_info(text, keyword):
        k = keyword.lower()
        contexts = []
        sentences = []
        numbers = []
        dates = []

        # simple sentence split
        sent_split = re.split(r'(?<=[.!?])\s+', text)
        for s in sent_split:
            if k in s.lower():
                sentences.append(s.strip())

        # contexts (windowed snippets)
        for m in re.finditer(re.escape(keyword), text, flags=re.IGNORECASE):
            start = max(0, m.start() - 120)
            end = min(len(text), m.end() + 120)
            contexts.append(text[start:end].strip())

            # numbers near keyword (30 chars window)
            window_start = max(0, m.start() - 30)
            window_end = min(len(text), m.end() + 30)
            window = text[window_start:window_end]
            for num in re.findall(r"\b\d+[\d.,]*\b", window):
                numbers.append(num)

        # dates (very simple heuristics)
        for d in re.findall(r"\b\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}\b", text):
            dates.append(d)
        for d in re.findall(r"\b\d{4}\b", text):
            dates.append(d)

        # dedup while preserving order
        def _uniq(seq):
            seen = set(); out = []
            for x in seq:
                if x not in seen:
                    seen.add(x); out.append(x)
            return out

        return {
            "sentences_with_keyword": _uniq(sentences),
            "keyword_contexts": _uniq(contexts),
            "numbers_near_keyword": _uniq(numbers),
            "dates_in_text": _uniq(dates),
            "occurrences": len(contexts),
        }

    # Zoek interne article-links (basis: links die naar dezelfde host wijzen)
    internal_links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        full = urljoin(url, href)
        # alleen interne links
        if full.startswith(BASE_URL) and full != url:
            internal_links.append(full)

    # keyword-related info
    kw_info = _extract_keyword_info(full_text, keyword)

    # Entities via spaCy (if available)
    entities = {}
    if _SPACY_NLP and full_text:
        try:
            doc = _SPACY_NLP(full_text)
            for ent in doc.ents:
                entities.setdefault(ent.label_, []).append(ent.text)
            # deduplicate while preserving order
            for k, v in list(entities.items()):
                seen = set(); out = []
                for x in v:
                    if x not in seen:
                        seen.add(x); out.append(x)
                entities[k] = out
        except Exception as e:
            print(f"[WARN] spaCy entity extraction failed for {url}: {e}")

    return {
        "title": title,
        "url": url,
        "date": date_text,
        "author": author,
        "tags": tags,
        "main_image": main_image,
        "word_count": word_count,
        "internal_links": list(dict.fromkeys(internal_links)),
        "entities": entities,
        "keyword_occurrences": kw_info["occurrences"],
        "keyword_contexts": kw_info["keyword_contexts"],
        "keyword_sentences": kw_info["sentences_with_keyword"],
        "numbers_near_keyword": kw_info["numbers_near_keyword"],
        "dates_in_text": kw_info["dates_in_text"],
        "contains_keyword": contains_keyword,
        "snippet": snippet,
        "full_text": full_text,
    }


def search_drimble_for_keyword(keyword, max_pages=1):
    """
    Zoekt op Drimble naar een keyword en haalt artikelen op.

    LET OP:
    - De query-parameter ('q') en pagina-parameter ('page') zijn aannames.
      Controleer deze in de browser (Netwerk-tab in DevTools) en pas ze zo nodig aan.
    """
    all_results = []

    for page in range(1, max_pages + 1):
        params = {
            "q": keyword,     # mogelijk moet dit iets als 'zoekwoord' of 'query' zijn
            "page": page,     # mogelijk 'p', 'pagina', etc.
        }

        print(f"[INFO] Haal zoekresultaten op pagina {page}...")
        try:
            soup = get_soup(BASE_URL + SEARCH_PATH, params=params)
        except Exception as e:
            print(f"FOUT bij ophalen zoekpagina {page}: {e}")
            break

        page_results = find_search_results(soup, keyword)
        if not page_results:
            # waarschijnlijk geen resultaten (meer)
            print("[INFO] Geen resultaten meer gevonden.")
            break

        print(f"[INFO] Gevonden {len(page_results)} potentiële artikelen op pagina {page}.")
        all_results.extend(page_results)

    # dedup
    seen = set()
    unique_results = []
    for r in all_results:
        if r["url"] in seen:
            continue
        seen.add(r["url"])
        unique_results.append(r)

    return unique_results


def scrape_vuurwerk_articles(output_csv=None, max_pages=1, follow_links=True, max_link_depth=1, max_links_per_article=5, max_total_articles=500, save_json=True, save_json_all=False, json_subdir="articles"):
    """
    Scrapes vuurwerk articles from Drimble.
    
    Args:
        output_csv: Path to output CSV file. If None, saves to output_scrapers/drimble_vuurwerk.csv
        max_pages: Number of pages to scrape
    """
    if output_csv is None:
        # Construct path relative to project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_csv = os.path.join(project_root, "output_scrapers", "drimble_vuurwerk.csv")
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_csv)
    os.makedirs(output_dir, exist_ok=True)
    # JSON output directory (subfolder inside output_scrapers)
    json_dir = os.path.join(output_dir, json_subdir)
    if save_json:
        os.makedirs(json_dir, exist_ok=True)
    
    keyword = "vuurwerk"

    # Begin met zoekresultaten als startpunt
    search_results = search_drimble_for_keyword(keyword, max_pages=max_pages)
    start_urls = [r["url"] for r in search_results]
    print(f"[INFO] Totaal {len(start_urls)} unieke zoekresultaten gevonden.")

    rows = []
    processed = set()
    queue = deque()

    # enqueue start urls with depth 0
    for u in start_urls:
        queue.append((u, 0))

    while queue and len(processed) < max_total_articles:
        url, depth = queue.popleft()
        if url in processed:
            continue
        print(f"[INFO] Verwerk artikel (depth={depth}): {url}")
        article_data = extract_article_data(url, keyword)
        processed.add(url)
        if not article_data:
            continue

        if not article_data["contains_keyword"]:
            print("   -> keyword niet in tekst, overslaan")
            # still optionally follow links even if keyword not found
        else:
            rows.append(article_data)

        # Save per-article JSON if desired
        if save_json and (article_data.get("contains_keyword") or save_json_all):
            # create a stable filename: index + short sha1
            sha = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
            idx = len(processed)
            # sanitize title into short slug
            title = article_data.get("title") or "article"
            slug = re.sub(r"[^0-9a-zA-Z_-]", "_", title)[:40]
            filename = f"{idx:04d}_{slug}_{sha}.json"
            filepath = os.path.join(json_dir, filename)
            try:
                with open(filepath, "w", encoding="utf-8") as jf:
                    json.dump(article_data, jf, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"   -> kon JSON niet schrijven voor {url}: {e}")

        # follow internal links if requested and depth limit not reached
        if follow_links and depth < max_link_depth:
            links = article_data.get("internal_links", [])[:max_links_per_article]
            for l in links:
                if l not in processed:
                    queue.append((l, depth + 1))

    # Naar CSV schrijven (tags worden als ;-gescheiden string opgeslagen)
    fieldnames = [
        "title",
        "url",
        "date",
        "author",
        "tags",
        "main_image",
        "word_count",
        "snippet",
        "full_text",
        "entities",
        "keyword_occurrences",
        "keyword_contexts",
        "keyword_sentences",
        "numbers_near_keyword",
        "dates_in_text",
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "title": r["title"],
                "url": r["url"],
                "date": r["date"],
                "author": r.get("author", ""),
                "tags": ";".join(r.get("tags", [])),
                "main_image": r.get("main_image", ""),
                "word_count": r.get("word_count", 0),
                "snippet": r.get("snippet", ""),
                "full_text": r.get("full_text", ""),
                "entities": json.dumps(r.get("entities", {}), ensure_ascii=False),
                "keyword_occurrences": r.get("keyword_occurrences", 0),
                "keyword_contexts": json.dumps(r.get("keyword_contexts", []), ensure_ascii=False),
                "keyword_sentences": json.dumps(r.get("keyword_sentences", []), ensure_ascii=False),
                "numbers_near_keyword": json.dumps(r.get("numbers_near_keyword", []), ensure_ascii=False),
                "dates_in_text": json.dumps(r.get("dates_in_text", []), ensure_ascii=False),
            })

    print(f"[KLAAR] {len(rows)} artikelen met '{keyword}' opgeslagen in {output_csv}")


if __name__ == "__main__":
    # max_pages=1 om beleefd te blijven.
    # Verhoog dit als je zeker weet dat het mag en netjes throttle’t.
    scrape_vuurwerk_articles(max_pages=1)
