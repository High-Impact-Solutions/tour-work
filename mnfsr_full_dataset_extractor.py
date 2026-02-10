import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import camelot

# ==========================================
# CONFIG
# ==========================================

BASE_URL = "https://mnfsr.gov.pk/Publications"

DOWNLOAD_FOLDER = "MNFSR_PDFs"
TABLE_FOLDER = "MNFSR_Extracted_Tables"

MASTER_CSV = "MNFSR_MASTER_DATASET.csv"

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(TABLE_FOLDER, exist_ok=True)

# ==========================================
# 1. SCRAPE ALL PDF LINKS
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
# 2. DOWNLOAD PDF FILES
# ==========================================

def download_pdfs(pdf_links):
    downloaded_files = []

    for url in pdf_links:
        filename = url.split("/")[-1]
        filepath = os.path.join(DOWNLOAD_FOLDER, filename)

        if os.path.exists(filepath):
            print("⚠ Already Downloaded:", filename)
            downloaded_files.append(filepath)
            continue

        print("⬇ Downloading:", filename)

        r = requests.get(url)

        with open(filepath, "wb") as f:
            f.write(r.content)

        downloaded_files.append(filepath)

    return downloaded_files


# ==========================================
# 3. CLEAN TABLE FUNCTION
# ==========================================

def clean_table(df):
    # Remove empty rows
    df = df.dropna(how="all")

    # Remove duplicate header rows
    df = df[df.iloc[:, 0] != df.columns[0]]

    # Reset index
    df = df.reset_index(drop=True)

    return df


# ==========================================
# 4. EXTRACT TABLES FROM PDF
# ==========================================

def extract_tables_from_pdf(pdf_file):
    print("\n📌 Extracting Tables From:", pdf_file)

    try:
        tables = camelot.read_pdf(pdf_file, pages="all")

        if tables.n == 0:
            print("⚠ No tables found.")
            return []

        extracted = []

        for i, table in enumerate(tables):
            df = table.df
            df = clean_table(df)

            if len(df) < 2:
                continue

            extracted.append(df)

        return extracted

    except Exception as e:
        print("❌ Error Extracting:", e)
        return []


# ==========================================
# 5. FULL PIPELINE
# ==========================================

def run_full_pipeline():
    all_rows = []

    # Step 1: Get all PDFs
    pdf_links = get_all_pdf_links()

    # Step 2: Download them
    pdf_files = download_pdfs(pdf_links)

    # Step 3: Extract tables + build master dataset
    for pdf in pdf_files:

        tables = extract_tables_from_pdf(pdf)

        if not tables:
            continue

        pdf_name = os.path.basename(pdf)

        for idx, df in enumerate(tables):

            # Add source column
            df["Source_PDF"] = pdf_name

            # Save individual table CSV
            table_file = os.path.join(
                TABLE_FOLDER,
                f"{pdf_name}_table_{idx+1}.csv"
            )
            df.to_csv(table_file, index=False)

            all_rows.append(df)

    # Step 4: Combine into Master CSV
    if all_rows:
        master_df = pd.concat(all_rows, ignore_index=True)
        master_df.to_csv(MASTER_CSV, index=False)

        print("\n🎉 MASTER DATASET READY!")
        print("✅ Saved:", MASTER_CSV)

    else:
        print("\n❌ No tables extracted from any PDF.")


# ==========================================
# RUN SCRIPT
# ==========================================

if __name__ == "__main__":
    run_full_pipeline()
