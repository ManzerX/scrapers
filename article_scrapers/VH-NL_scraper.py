# Version: 1.0
import scraper-helper as sh
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import pandas as pd
VEILIGHEID_PAGES = [
    "https://www.veiligheid.nl/themas/veilig-productgebruik/cijferrapportage/ongevallen-met-vuurwerk-jaarwisseling-2023-2024",
    "https://www.veiligheid.nl/themas/veilig-productgebruik/cijferrapportage/ongevallen-met-vuurwerk-jaarwisseling-2024-2025",
]

def find_vuurwerk_pdfs():
    pdf_urls = []
    for page_url in VEILIGHEID_PAGES:
        resp = sh.fetch(page_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf") and "vuurwerk" in href.lower():
                full_url = urljoin(page_url, href)
                pdf_urls.append(full_url)
                print(f"[FOUND] {full_url}")
    return list(dict.fromkeys(pdf_urls))  # unieke lijst

def scrape_veiligheidnl():
    pdf_urls = find_vuurwerk_pdfs()
    all_tables = []

    for url in pdf_urls:
        filename = url.split("/")[-1].split("?")[0]
        pdf_path = os.path.join(PDF_DIR, filename)
        download_file(url, pdf_path)

        df = pdf_to_tables(pdf_path)
        if df is not None:
            df["__source__"] = "VeiligheidNL"
            df["__file__"] = filename
            all_tables.append(df)

    if all_tables:
        big = pd.concat(all_tables, ignore_index=True)
        out_csv = os.path.join(BASE_DIR, "veiligheidnl_vuurwerk_tabellen.csv")
        big.to_csv(out_csv, index=False)
        print(f"[OK] Gecombineerde tabellen → {out_csv}")

if __name__ == "__main__":
    scrape_veiligheidnl()
