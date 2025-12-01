# Version:1 1.0
import os
import time
import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_DIR = "data"
PDF_DIR = os.path.join(BASE_DIR, "pdf")
RAW_DIR = os.path.join(BASE_DIR, "raw")

os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ScraperHelper/1.0; +https://example.com)"
}

def fetch(url, *, sleep=1.0):
    """Veilige wrapper om een pagina op te halen."""
    print(f"[FETCH] {url}")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(sleep)
    return resp

def download_file(url, out_path):
    """Download een bestand (PDF/CSV/…) naar disk."""
    if os.path.exists(out_path):
        print(f"[SKIP] bestaat al: {out_path}")
        return
    resp = fetch(url)
    with open(out_path, "wb") as f:
        f.write(resp.content)
    print(f"[OK] opgeslagen: {out_path}")

def pdf_to_tables(pdf_path):
    """Lees alle tabellen uit een PDF en geef één grote DataFrame terug."""
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            page_tables = page.extract_tables()
            if not page_tables:
                continue
            for t in page_tables:
                if len(t) < 2:
                    continue
                df = pd.DataFrame(t[1:], columns=t[0])
                df["__page__"] = page_no
                tables.append(df)
    if not tables:
        print(f"[WARN] Geen tabellen gevonden in {pdf_path}")
        return None
    big = pd.concat(tables, ignore_index=True)
    return big
