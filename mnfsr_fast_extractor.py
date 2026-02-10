import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import camelot
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://mnfsr.gov.pk/Publications"

DOWNLOAD_FOLDER = "MNFSR_PDFs"
TABLE_FOLDER = "MNFSR_Extracted_Tables"
MASTER_CSV = "MNFSR_MASTER_DATASET.csv"

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(TABLE_FOLDER, exist_ok=True)


# ==========================================
# SCRAPE PDF LINKS
# ==========================================

def get_all_pdf_links():
    print("🔍 Scraping MNFSR Publications Page...")

    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    pdf_links = []

    for link in soup.find_all("a"):
        href = link.get("href")
        if href and ".pdf" in href.lower():
            if href.startswith("/"):
                href = "https://mnfsr.gov.pk" + href
            pdf_links.append(href)

    print(f"✅ Total PDFs Found: {len(pdf_links)}")
    return list(set(pdf_links))


# ==========================================
# DOWNLOAD PDFs
# ==========================================

def download_pdf(url):
    filename = url.split("/")[-1]
    filepath = os.path.join(DOWNLOAD_FOLDER, filename)

    if os.path.exists(filepath):
        print("⚠ Already Downloaded:", filename)
        return filepath

    print("⬇ Downloading:", filename)

    r = requests.get(url)
    with open(filepath, "wb") as f:
        f.write(r.content)

    return filepath


# ==========================================
# CLEAN TABLE
# ==========================================

def clean_table(df):
    df = df.dropna(how="all")
    df = df.reset_index(drop=True)
    return df


# ==========================================
# FAST TABLE EXTRACTION
# ==========================================

def extract_tables(pdf_file):
    pdf_name = os.path.basename(pdf_file)
    print(f"\n📌 Extracting Tables: {pdf_name}")

    try:
        # 🚀 Only scan first 15 pages (FAST)
        tables = camelot.read_pdf(pdf_file, pages="1-15")

        if tables.n == 0:
            print("⚠ No tables found (first 15 pages). Skipping...")
            return []

        extracted = []

        for i, table in enumerate(tables):
            df = clean_table(table.df)

            if len(df) < 2:
                continue

            df["Source_PDF"] = pdf_name

            out_file = os.path.join(
                TABLE_FOLDER,
                f"{pdf_name}_table_{i+1}.csv"
            )
            df.to_csv(out_file, index=False)

            extracted.append(df)

        print(f"✅ Extracted {len(extracted)} tables from {pdf_name}")
        return extracted

    except Exception as e:
        print("❌ Error:", e)
        return []


# ==========================================
# FULL PIPELINE (MULTI THREAD)
# ==========================================

def run_pipeline():
    pdf_links = get_all_pdf_links()

    # Download PDFs fast
    print("\n⬇ Downloading PDFs...")
    with ThreadPoolExecutor(max_workers=6) as executor:
        pdf_files = list(executor.map(download_pdf, pdf_links))

    print("\n🚀 Extracting tables (FAST MODE)...")

    all_tables = []

    for pdf in pdf_files:
        tables = extract_tables(pdf)
        all_tables.extend(tables)

    # Combine Master CSV
    if all_tables:
        master_df = pd.concat(all_tables, ignore_index=True)
        master_df.to_csv(MASTER_CSV, index=False)

        print("\n🎉 MASTER DATASET CREATED!")
        print("✅ Saved:", MASTER_CSV)

    else:
        print("\n❌ No tables extracted.")


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":
    run_pipeline()
