# version 1.0
import os
import time
import json
from datetime import datetime, timezone
from typing import List, Dict, Any

import praw
import pandas as pd



# Subreddits om te doorzoeken (zonder r/)
SUBREDDITS = [
    "thenetherlands",
    "Netherlands",
    "vuurwerk",  # alleen als deze bestaat; anders wordt hij gewoon overgeslagen
]

# Zoektermen gericht op vuurwerk + incidenten
SEARCH_TERMS = [
    "vuurwerk",
    "\"illegaal vuurwerk\"",
    "\"illegal vuurwerk\"",
    "knalvuurwerk",
    "\"cobra 6\"",
    "nitraten",
    "\"zelfgemaakt vuurwerk\"",
    "siervuurwerk",
    "\"legaal vuurwerk\"",
]

# Optioneel: incident-gerelateerde woorden (helpt later bij analyse)
INCIDENT_KEYWORDS = [
    "ongeluk", "incident", "gewond", "gewonden",
    "brand", "schade", "politie", "arrestatie",
    "ziekenhuis", "huis afgebrand", "ruiten kapot",
]

ILLEGAL_KEYWORDS = [
    "illegaal vuurwerk", "illegal vuurwerk",
    "cobra 6", "cobra6", "nitraat", "nitraten",
    "strijker", "bunkerknaller", "polenknaller",
]

LEGAL_KEYWORDS = [
    "legaal vuurwerk", "siervuurwerk",
    "categorie 1", "categorie 2", "cat.1", "cat.2",
]

# Tijdsperiode: disabled voor nu - zoeken over alle posts
# START_DATETIME = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
# END_DATETIME = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
START_DATETIME = None
END_DATETIME = None


# =========================
# HULPFUNCTIES
# =========================

def utc_timestamp(dt: datetime) -> int:
    """Converteer datetime (met tzinfo=UTC) naar unix timestamp (int)."""
    return int(dt.timestamp())


def contains_any(text: str, keywords: List[str]) -> bool:
    """Check of een van de gegeven keywords in de tekst voorkomt (case-insensitive)."""
    if not text:
        return False
    lower = text.lower()
    return any(kw.lower() in lower for kw in keywords)



# reddit scraper class
class RedditFireworksScraper:
    def __init__(self):
        # Reddit API credentials ophalen uit omgevingsvariabelen
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT")

        if not all([client_id, client_secret, user_agent]):
            raise RuntimeError(
                "Zorg dat REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET en "
                "REDDIT_USER_AGENT als environment variables zijn ingesteld."
            )

        # PRAW-instantie
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )

        # Voor tijdfilter (disabled voor nu)
        self.start_ts = utc_timestamp(START_DATETIME) if START_DATETIME else None
        self.end_ts = utc_timestamp(END_DATETIME) if END_DATETIME else None

    def search_subreddit_posts(self, subreddit_name: str, query: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Zoek posts in een subreddit op basis van query en filter op tijd."""
        print(f"[INFO] Zoeken in r/{subreddit_name} met query: {query}")
        posts_data = []

        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            # Reddit search is beperkt; we halen zoveel mogelijk op en filteren zelf op datum
            for submission in subreddit.search(query=query, sort="new", limit=limit):
                created_utc = int(submission.created_utc)

                # Filter op onze tijdsrange (disabled voor nu)
                if self.start_ts and self.end_ts:
                    if not (self.start_ts <= created_utc < self.end_ts):
                        continue

                post_dict = {
                    "type": "post",
                    "id": submission.id,
                    "subreddit": subreddit_name,
                    "title": submission.title,
                    "selftext": submission.selftext,
                    "created_utc": created_utc,
                    "created_datetime_utc": datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat(),
                    "author": str(submission.author) if submission.author else "[deleted]",
                    "score": submission.score,
                    "num_comments": submission.num_comments,
                    "permalink": f"https://www.reddit.com{submission.permalink}",
                    "url": submission.url,
                    # Voor jouw onderzoeksvraag alvast wat flags:
                    "mentions_incident": contains_any(submission.title + " " + submission.selftext, INCIDENT_KEYWORDS),
                    "mentions_illegal": contains_any(submission.title + " " + submission.selftext, ILLEGAL_KEYWORDS),
                    "mentions_legal": contains_any(submission.title + " " + submission.selftext, LEGAL_KEYWORDS),
                }

                posts_data.append(post_dict)

            print(f"[INFO] Gevonden posts in r/{subreddit_name} voor query '{query}': {len(posts_data)}")

        except Exception as e:
            print(f"[ERROR] Fout bij zoeken in r/{subreddit_name}: {e}")

        return posts_data

    def fetch_comments_for_post(self, submission_id: str, subreddit_name: str) -> List[Dict[str, Any]]:
        """Haal alle comments op van een specifieke post en filter op tijd."""
        comments_data = []
        try:
            submission = self.reddit.submission(id=submission_id)
            submission.comments.replace_more(limit=None)

            for comment in submission.comments.list():
                created_utc = int(comment.created_utc)

                # Filter op onze tijdsrange (disabled voor nu)
                if self.start_ts and self.end_ts:
                    if not (self.start_ts <= created_utc < self.end_ts):
                        continue

                body = comment.body or ""

                comment_dict = {
                    "type": "comment",
                    "id": comment.id,
                    "parent_id": comment.parent_id,
                    "link_id": comment.link_id,
                    "submission_id": submission_id,
                    "subreddit": subreddit_name,
                    "body": body,
                    "created_utc": created_utc,
                    "created_datetime_utc": datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat(),
                    "author": str(comment.author) if comment.author else "[deleted]",
                    "score": comment.score,
                    "permalink": f"https://www.reddit.com{comment.permalink}",
                    "mentions_incident": contains_any(body, INCIDENT_KEYWORDS),
                    "mentions_illegal": contains_any(body, ILLEGAL_KEYWORDS),
                    "mentions_legal": contains_any(body, LEGAL_KEYWORDS),
                }
                comments_data.append(comment_dict)

            print(f"[INFO] Gevonden comments voor post {submission_id}: {len(comments_data)}")

        except Exception as e:
            print(f"[ERROR] Fout bij ophalen comments voor {submission_id}: {e}")

        return comments_data

    def run(self) -> pd.DataFrame:
        """Hoofdlogica: zoek posts en trek vervolgens alle comments binnen die binnen de tijdsrange vallen."""
        all_records: List[Dict[str, Any]] = []

        for subreddit in SUBREDDITS:
            for term in SEARCH_TERMS:
                # Kleine pauze om API-limieten te respecteren
                time.sleep(1)
                posts = self.search_subreddit_posts(subreddit, term)
                all_records.extend(posts)

                # Voor elke unieke post: comments ophalen
                unique_post_ids = {p["id"] for p in posts}
                for post_id in unique_post_ids:
                    time.sleep(1)
                    comments = self.fetch_comments_for_post(post_id, subreddit)
                    all_records.extend(comments)

        # Dataframe maken voor verdere analyse
        df = pd.DataFrame(all_records)
        return df

# main 
def main():
    print("[INFO] Start Reddit vuurwerk scraper")
    if START_DATETIME and END_DATETIME:
        print(f"[INFO] Periode: {START_DATETIME.isoformat()} t/m {END_DATETIME.isoformat()} (UTC)")
    else:
        print("[INFO] Periode: geen filter (alle posts)")

    scraper = RedditFireworksScraper()
    df = scraper.run()

    # Resultaten opslaan
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"reddit_vuurwerk_{timestamp_str}.csv"
    json_filename = f"reddit_vuurwerk_{timestamp_str}.json"

    df.to_csv(csv_filename, index=False)
    df.to_json(json_filename, orient="records", force_ascii=False, indent=2)

    print(f"[INFO] Aantal records totaal (posts + comments): {len(df)}")
    print(f"[INFO] Data opgeslagen in: {csv_filename} en {json_filename}")

# auto start main
if __name__ == "__main__":
    main()
