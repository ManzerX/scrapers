import csv
import os
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://drimble.nl"
SEARCH_PATH = "/zoeken.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DrimbleVuurwerkScraper/1.0; +https://example.com)"
}

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

    return {
        "title": title,
        "url": url,
        "date": date_text,
        "author": author,
        "tags": tags,
        "main_image": main_image,
        "word_count": word_count,
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


def scrape_vuurwerk_articles(output_csv=None, max_pages=1):
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
    
    keyword = "vuurwerk"
    search_results = search_drimble_for_keyword(keyword, max_pages=max_pages)

    print(f"[INFO] Totaal {len(search_results)} unieke zoekresultaten gevonden.")

    rows = []
    for i, res in enumerate(search_results, start=1):
        print(f"[INFO] ({i}/{len(search_results)}) Verwerk artikel: {res['url']}")
        article_data = extract_article_data(res["url"], keyword)
        if not article_data:
            continue

        if not article_data["contains_keyword"]:
            # optioneel: filter artikelen waar 'vuurwerk' niet echt in de tekst staat
            print("   -> keyword niet in tekst, overslaan")
            continue

        rows.append(article_data)

    # Naar CSV schrijven (tags worden als ;-gescheiden string opgeslagen)
    fieldnames = ["title", "url", "date", "author", "tags", "main_image", "word_count", "snippet", "full_text"]
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
            })

    print(f"[KLAAR] {len(rows)} artikelen met '{keyword}' opgeslagen in {output_csv}")


if __name__ == "__main__":
    # max_pages=1 om beleefd te blijven.
    # Verhoog dit als je zeker weet dat het mag en netjes throttle’t.
    scrape_vuurwerk_articles(max_pages=1)
