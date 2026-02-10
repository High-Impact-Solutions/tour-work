import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import pdfplumber

# ===============================
# SETTINGS
# ===============================

BASE_URL = "https://mnfsr.gov.pk"
PUBLICATIONS_URL = BASE_URL + "/Publications"

PDF_FOLDER = "MNFSR_PDFs"
CSV_FOLDER = "MNFSR_CSVs"

MASTER_CSV = "MNFSR_ALL_Publications_Master.csv"

os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)


# ===============================
# STEP 1: SCRAPE PUBLICATIONS
# ===============================

def scrape_publications():
    print("🔍 Scraping MNFSR Publications Page...")

    response = requests.get(PUBLICATIONS_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")

    if not table:
        print("❌ No table found!")
        return []

    rows = table.find_all("tr")[1:]

    data = []

    for row in rows:
        cols = row.find_all("td")

        if len(cols) < 4:
            continue

        title = cols[1].get_text(strip=True)
        date = cols[2].get_text(strip=True)

        link_tag = cols[3].find("a")
        pdf_link = None

        if link_tag and link_tag.get("href"):
            pdf_link = urljoin(BASE_URL, link_tag["href"])

        data.append({
            "Title": title,
            "Date": date,
            "PDF_Link": pdf_link
        })

    print(f"✅ Found {len(data)} Publications")
    return data


# ===============================
# STEP 2: DOWNLOAD PDFs
# ===============================

def download_pdfs(publications):
    print("\n📥 Downloading PDFs...")

    for pub in publications:
        link = pub["PDF_Link"]

        if not link:
            continue

        filename = link.split("/")[-1]
        filepath = os.path.join(PDF_FOLDER, filename)

        if os.path.exists(filepath):
            print("✔ Already downloaded:", filename)
            continue

        try:
            r = requests.get(link)
            with open(filepath, "wb") as f:
                f.write(r.content)

            print("✅ Downloaded:", filename)

        except Exception as e:
            print("❌ Failed:", link, e)


# ===============================
# STEP 3: EXTRACT TABLES FROM PDFs
# ===============================

def extract_tables_from_pdf(pdf_path):
    extracted_tables = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):

                table = page.extract_table()

                if table:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    extracted_tables.append(df)

    except Exception as e:
        print("❌ PDF Extraction Error:", e)

    return extracted_tables


# ===============================
# STEP 4: CONVERT ALL PDFs → CSV
# ===============================

def convert_all_pdfs_to_csv():
    print("\n📊 Extracting Tables from PDFs...")

    master_rows = []

    pdf_files = os.listdir(PDF_FOLDER)

    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_FOLDER, pdf_file)

        print("\n📌 Processing:", pdf_file)

        tables = extract_tables_from_pdf(pdf_path)

        if not tables:
            print("⚠ No tables found inside:", pdf_file)
            continue

        for i, df in enumerate(tables):
            clean_name = pdf_file.replace(".pdf", "")

            csv_name = f"{clean_name}_Table{i+1}.csv"
            csv_path = os.path.join(CSV_FOLDER, csv_name)

            # Save individual CSV
            df.to_csv(csv_path, index=False)

            print("✅ Saved CSV:", csv_name)

            # Add into master dataset
            df["Source_PDF"] = pdf_file
            df["Table_Number"] = i + 1

            master_rows.append(df)

    # Combine Master CSV
    if master_rows:
        master_df = pd.concat(master_rows, ignore_index=True)
        master_df.to_csv(MASTER_CSV, index=False)

        print("\n🎉 MASTER DATASET READY:", MASTER_CSV)

    else:
        print("\n❌ No tables extracted from any PDF.")


# ===============================
# RUN FULL PIPELINE
# ===============================

def run_full_extractor():
    publications = scrape_publications()

    # Save publication metadata
    pd.DataFrame(publications).to_csv("MNFSR_Publications_List.csv", index=False)

    download_pdfs(publications)

    convert_all_pdfs_to_csv()


if __name__ == "__main__":
    run_full_extractor()
