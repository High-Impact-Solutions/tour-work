import os
import re
import warnings
import requests
import pandas as pd

from bs4 import BeautifulSoup

import camelot
from pdfminer.high_level import extract_text

# ===============================
# SETTINGS
# ===============================

MNFSR_URL = "https://mnfsr.gov.pk/Publications"

PDF_FOLDER = "MNFSR_PDFs"
TABLE_FOLDER = "Extracted_Tables"
MASTER_FILE = "MNFSR_MASTER_DATASET.csv"

MAX_PAGES = 15   # only first 15 pages for speed

warnings.filterwarnings("ignore")


# ===============================
# STEP 1: DOWNLOAD PDFs
# ===============================

def download_all_pdfs():

    print("\n🔍 Scraping MNFSR Publications Page...")

    os.makedirs(PDF_FOLDER, exist_ok=True)

    response = requests.get(MNFSR_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    pdf_links = []

    for link in soup.find_all("a", href=True):
        href = link["href"]

        if ".pdf" in href.lower():
            full_url = href if href.startswith("http") else "https://mnfsr.gov.pk" + href
            pdf_links.append(full_url)

    print(f"✅ Total PDFs Found: {len(pdf_links)}")

    for url in pdf_links:

        filename = url.split("/")[-1]
        filename = filename.replace("%20", "_")

        filepath = os.path.join(PDF_FOLDER, filename)

        if os.path.exists(filepath):
            print(f"⚠ Already Downloaded: {filename}")
            continue

        print(f"⬇ Downloading: {filename}")

        pdf_data = requests.get(url).content

        with open(filepath, "wb") as f:
            f.write(pdf_data)

    print("\n✅ All PDFs Downloaded Successfully!")


# ===============================
# STEP 2: CHECK TEXT PDF OR SCANNED
# ===============================

def is_text_pdf(pdf_path):
    try:
        text = extract_text(pdf_path, maxpages=1)
        return bool(text.strip())
    except:
        return False


# ===============================
# STEP 3: CLEAN TABLE HEADERS
# ===============================

def clean_dataframe(df):

    # Remove empty columns
    df = df.dropna(axis=1, how="all")

    # Remove duplicate header rows
    df = df[df.iloc[:, 0] != df.columns[0]]

    # Rename columns properly
    df.columns = [f"Column_{i+1}" for i in range(df.shape[1])]

    # Drop empty rows
    df = df.dropna(how="all")

    return df


# ===============================
# STEP 4: EXTRACT TABLES (FAST)
# ===============================

def extract_tables_from_pdf(pdf_path):

    filename = os.path.basename(pdf_path)

    print(f"\n📌 Processing: {filename}")

    if not is_text_pdf(pdf_path):
        print("🖼️ Scanned PDF detected → OCR needed later (skipping)")
        return []

    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=f"1-{MAX_PAGES}",
            flavor="stream"
        )

        if tables.n == 0:
            print("⚠ No tables found.")
            return []

        extracted = []

        for i, table in enumerate(tables):

            df = table.df
            df = clean_dataframe(df)

            if df.empty or len(df) < 2:
                continue

            df["Source_PDF"] = filename
            df["Table_Number"] = i + 1

            extracted.append(df)

        print(f"✅ Tables Extracted: {len(extracted)}")
        return extracted

    except Exception as e:
        print("❌ Error:", e)
        return []


# ===============================
# STEP 5: BUILD MASTER DATASET
# ===============================

def build_master_dataset():

    os.makedirs(TABLE_FOLDER, exist_ok=True)

    all_tables = []

    pdf_files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]

    print(f"\n📂 Total PDFs to Process: {len(pdf_files)}")

    for pdf in pdf_files:

        path = os.path.join(PDF_FOLDER, pdf)

        tables = extract_tables_from_pdf(path)

        all_tables.extend(tables)

    if not all_tables:
        print("\n❌ No tables extracted from any PDF.")
        return

    print("\n📌 Combining into MASTER CSV...")

    master_df = pd.concat(all_tables, ignore_index=True)

    master_df.to_csv(MASTER_FILE, index=False)

    print("\n🎉 DONE!")
    print(f"✅ Master Tableau Dataset Saved: {MASTER_FILE}")


# ===============================
# RUN FULL PIPELINE
# ===============================

if __name__ == "__main__":

    download_all_pdfs()
    build_master_dataset()
