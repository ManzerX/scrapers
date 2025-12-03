import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd


# =========================
# CONFIGURATIE
# =========================

BASE_URL = "https://old.reddit.com"

HEADERS = {
    # ZET HIER EEN EIGEN USER-AGENT IN
    "User-Agent": "Mozilla/5.0 (compatible; old-reddit-vuurwerkScraper/1.0; +https://example.com)"
}

SUBREDDITS = [
    "theNetherlands",
    "Netherlands",
    "VuurwerkNL",
    "vuurwerk",  # als hij niet bestaat, krijg je gewoon geen resultaten
]

SEARCH_TERMS = [
    "vuurwerk",
    "\"illegaal vuurwerk\"",
    "\"illegal vuurwerk\"",
    "knalvuurwerk",
    "\"cobra 6\"",
    "nitraat",
    "nitraten",
    "\"zelfgemaakt vuurwerk\"",
    "siervuurwerk",
    "\"legaal vuurwerk\"",
]

INCIDENT_KEYWORDS = [
    "ongeluk", "incident", "gewond", "gewonden",
    "brand", "schade", "politie", "arrestatie",
    "ziekenhuis", "huis afgebrand", "ruiten kapot",
]

ILLEGAL_KEYWORDS = [
    "illegaal vuurwerk", "legaal vuurwerk",
    "cobra 6", "cobra6", "nitraat", "nitraten",
    "strijker", "bunkerknaller", "polenknaller",
]

LEGAL_KEYWORDS = [
    "legaal vuurwerk", "siervuurwerk",
    "categorie 1", "categorie 2", "cat.1", "cat.2",
]

# TIJDSRANGE VOOR FILTER:
# Disabled voor nu - scrape alle posts ongeacht datum
# START_DATETIME = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
# END_DATETIME = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
START_DATETIME = None
END_DATETIME = None

# Max aantal pagina's per zoekopdracht (om het veilig/overzichtelijk te houden)
MAX_PAGES_PER_QUERY = 3

# Delay tussen requests (in seconden) – respecteer de server!
REQUEST_DELAY = 2.0


# =========================
# HULPFUNCTIES
# =========================

def utc_timestamp(dt: datetime) -> int:
    return int(dt.timestamp())


# Time range timestamps (disabled for now)
START_TS = utc_timestamp(START_DATETIME) if START_DATETIME else None
END_TS = utc_timestamp(END_DATETIME) if END_DATETIME else None


def contains_any(text: str, keywords: List[str]) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def get_soup(url: str) -> Optional[BeautifulSoup]:
    """Haalt HTML op en geeft BeautifulSoup object terug, of None bij fout."""
    try:
        print(f"[REQ] {url}")
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"[WARN] Status code {resp.status_code} voor URL: {url}")
            return None
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Fout bij requests.get voor {url}: {e}")
        return None


def parse_reddit_time(time_tag) -> Optional[datetime]:
    """
    old.reddit gebruikt <time datetime="2023-12-31T21:23:45+00:00">.
    We parsen dit naar een datetime (UTC).
    """
    if not time_tag:
        return None
    dt_str = time_tag.get("datetime")
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        # Zorg dat hij tzinfo heeft
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        print(f"[WARN] Kon datetime niet parsen: {dt_str} ({e})")
        return None


def is_in_time_range(dt: Optional[datetime]) -> bool:
    # Time range filter disabled for now — accept all timestamps
    if START_TS is None or END_TS is None:
        return True
    if dt is None:
        return False
    ts = utc_timestamp(dt)
    return START_TS <= ts < END_TS


def extract_text_from_md(md_div) -> str:
    """Pak de tekst uit een markdown body (div.md) zo schoon mogelijk."""
    if not md_div:
        return ""
    # Simpel: alle tekst in één string
    return md_div.get_text(" ", strip=True)


# =========================
# SCRAPE POSTS (ZOEKRESULTATEN)
# =========================

def search_subreddit_posts(subreddit: str, query: str = None) -> List[Dict[str, Any]]:
    """
    Crawl de /new/ pagina's van een subreddit en filter lokaal op SEARCH_TERMS.
    De 'query'-parameter wordt genegeerd (alle zoekwoorden zitten in SEARCH_TERMS).
    """
    records: List[Dict[str, Any]] = []

    # Begin bij /new/ (nieuwste posts eerst)
    url = f"{BASE_URL}/r/{subreddit}/new/"

    page_count = 0

    while url and page_count < MAX_PAGES_PER_QUERY:
        time.sleep(REQUEST_DELAY)

        soup = get_soup(url)
        if soup is None:
            break

        page_count += 1
        print(f"[INFO] r/{subreddit} – pagina {page_count} (new)")

        # Hier werkt 'thing' WEL (normale listing)
        things = soup.find_all("div", class_="thing", attrs={"data-type": "link"})
        print(f"[INFO] Gevonden {len(things)} posts op deze pagina")

        for thing in things:
            post_id_full = thing.get("data-fullname")  # bv. t3_xxxxxx
            post_id = post_id_full.split("_")[-1] if post_id_full else None

            title_tag = thing.find("a", class_="title")
            title = title_tag.get_text(strip=True) if title_tag else ""

            # Filter op je zoekwoorden in de titel
            if not contains_any(title, SEARCH_TERMS):
                continue

            permalink = thing.get("data-permalink")
            if not permalink and title_tag:
                permalink = title_tag.get("href")
            if permalink and permalink.startswith("/"):
                permalink = BASE_URL + permalink

            time_tag = thing.find("time")
            created_dt = parse_reddit_time(time_tag)

            if not is_in_time_range(created_dt):
                # Als je straks tijdsfilter aanzet, werkt dit weer
                pass  # nu staat je filter uit, dus deze regel doet niks

            score_tag = thing.find("div", class_="score")
            try:
                score = int(score_tag.get("title")) if score_tag and score_tag.get("title") else None
            except ValueError:
                score = None

            comments_tag = thing.find("a", class_="comments")
            num_comments = 0
            if comments_tag:
                text = comments_tag.get_text(strip=True)
                parts = text.split()
                for p in parts:
                    if p.isdigit():
                        num_comments = int(p)
                        break

            combined_text_for_flags = title  # body komt later

            record = {
                "type": "post",
                "id": post_id,
                "subreddit": subreddit,
                "title": title,
                "selftext": None,
                "created_utc": utc_timestamp(created_dt) if created_dt else None,
                "created_datetime_utc": created_dt.isoformat() if created_dt else None,
                "score": score,
                "num_comments": num_comments,
                "permalink": permalink,
                "mentions_incident": contains_any(combined_text_for_flags, INCIDENT_KEYWORDS),
                "mentions_illegal": contains_any(combined_text_for_flags, ILLEGAL_KEYWORDS),
                "mentions_legal": contains_any(combined_text_for_flags, LEGAL_KEYWORDS),
            }

            records.append(record)

        # Volgende pagina via "next-button"
        next_button = soup.find("span", class_="next-button")
        if next_button:
            next_link = next_button.find("a")
            url = next_link.get("href") if next_link else None
        else:
            url = None

    print(f"[INFO] Totaal posts voor r/{subreddit} (na keyword-filter): {len(records)}")
    return records


# =========================
# SCRAPE POST-DETAILS (BODY + COMMENTS)
# =========================

def fetch_post_body_and_comments(post_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Op basis van een post-record (met permalink) de echte post-body + alle comments scrapen.
    Returned een lijst: [post_update_record, comment_records...]
    """
    results: List[Dict[str, Any]] = []
    permalink = post_record.get("permalink")
    if not permalink:
        return results

    time.sleep(REQUEST_DELAY)

    soup = get_soup(permalink)
    if soup is None:
        return results

    # 1) Body van de post
    # Op old.reddit staat het hoofdbericht meestal in een div.thing met data-type="link"
    link_thing = soup.find("div", class_="thing", attrs={"data-type": "link"})
    if link_thing:
        md_div = link_thing.find("div", class_="expando")
        if md_div:
            # in expando kan usertext-body zitten
            body_div = md_div.find("div", class_="usertext-body")
            if body_div:
                md_inner = body_div.find("div", class_="md")
                body_text = extract_text_from_md(md_inner)
            else:
                body_text = ""
        else:
            body_text = ""
    else:
        body_text = ""

    # Combineer titel + body voor de flags
    combined_text = (post_record.get("title") or "") + " " + (body_text or "")

    post_updated = post_record.copy()
    post_updated["selftext"] = body_text
    post_updated["mentions_incident"] = contains_any(combined_text, INCIDENT_KEYWORDS)
    post_updated["mentions_illegal"] = contains_any(combined_text, ILLEGAL_KEYWORDS)
    post_updated["mentions_legal"] = contains_any(combined_text, LEGAL_KEYWORDS)

    results.append(post_updated)

    # 2) Comments
    # Comments zijn divs met class="thing" en data-type="comment"
    comment_things = soup.find_all("div", class_="thing", attrs={"data-type": "comment"})
    print(f"[INFO] Comments gevonden voor post {post_record.get('id')}: {len(comment_things)}")

    for c in comment_things:
        comment_id_full = c.get("data-fullname")  # t1_xxxx
        comment_id = comment_id_full.split("_")[-1] if comment_id_full else None
        parent_id = c.get("data-parent")
        link_id = c.get("data-link-id")

        entry = c.find("div", class_="entry")
        if not entry:
            continue

        author_tag = entry.find("a", class_="author")
        author = author_tag.get_text(strip=True) if author_tag else "[unknown]"

        time_tag = entry.find("time")
        created_dt = parse_reddit_time(time_tag)
        if not is_in_time_range(created_dt):
            continue

        body_div = entry.find("div", class_="usertext-body")
        md_div = body_div.find("div", class_="md") if body_div else None
        body_text = extract_text_from_md(md_div)

        score_tag = c.find("span", class_="score unvoted")
        score = None
        if score_tag:
            # tekst is vaak "123 points" of "1 point"
            score_text = score_tag.get_text(strip=True)
            for p in score_text.split():
                if p.isdigit():
                    score = int(p)
                    break

        combined = body_text

        comment_record = {
            "type": "comment",
            "id": comment_id,
            "parent_id": parent_id,
            "link_id": link_id,
            "submission_id": post_record.get("id"),
            "subreddit": post_record.get("subreddit"),
            "body": body_text,
            "author": author,
            "created_utc": utc_timestamp(created_dt) if created_dt else None,
            "created_datetime_utc": created_dt.isoformat() if created_dt else None,
            "score": score,
            "permalink": permalink,
            "mentions_incident": contains_any(combined, INCIDENT_KEYWORDS),
            "mentions_illegal": contains_any(combined, ILLEGAL_KEYWORDS),
            "mentions_legal": contains_any(combined, LEGAL_KEYWORDS),
        }

        results.append(comment_record)

    return results


# =========================
# MAIN LOGICA
# =========================

def run_scraper() -> pd.DataFrame:
    all_records: List[Dict[str, Any]] = []

    print(f"[INFO] Scraper gestart")
    # print(f"[INFO] Tijdsrange: {START_DATETIME.isoformat()} t/m {END_DATETIME.isoformat()} (UTC)")

    # 1) Posts vinden via zoekresultaten
    basic_posts: List[Dict[str, Any]] = []
    for subreddit in SUBREDDITS:
        posts = search_subreddit_posts(subreddit)
        basic_posts.extend(posts)


    print(f"[INFO] Totaal posts (voor deduplicatie): {len(basic_posts)}")

    # 2) Dedupliceren op post-id
    posts_by_id = {}
    for p in basic_posts:
        pid = p.get("id")
        if not pid:
            continue
        if pid not in posts_by_id:
            posts_by_id[pid] = p

    unique_posts = list(posts_by_id.values())
    print(f"[INFO] Unieke posts na deduplicatie: {len(unique_posts)}")

    # 3) Voor elke unieke post: body + comments ophalen
    for post in unique_posts:
        recs = fetch_post_body_and_comments(post)
        all_records.extend(recs)

    # 4) DataFrame maken
    df = pd.DataFrame(all_records)
    return df


def main():
    df = run_scraper()
    print(f"[INFO] Totaal aantal records (posts + comments): {len(df)}")
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_filename = f"reddit_vuurwerk_html_{timestamp_str}.csv"
    json_filename = f"reddit_vuurwerk_html_{timestamp_str}.json"

    df.to_csv(csv_filename, index=False)
    df.to_json(json_filename, orient="records", force_ascii=False, indent=2)

    print(f"[INFO] Data opgeslagen in:")
    print(f"  - {csv_filename}")
    print(f"  - {json_filename}")


if __name__ == "__main__":
    main()
