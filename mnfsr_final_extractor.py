import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

import camelot
from pdf2image import convert_from_path
import pytesseract

from concurrent.futures import ThreadPoolExecutor

# ==============================
# SETTINGS
# ==============================

URL = "https://mnfsr.gov.pk/Publications"

PDF_FOLDER = "MNFSR_PDFs"
TABLE_FOLDER = "MNFSR_Tables"
MASTER_FILE = "MNFSR_MASTER_TABLEAU.csv"

MAX_PAGES_OCR = 5   # OCR only first 5 pages for speed
THREADS = 4         # Multi-thread processing

# ==============================
# STEP 1: Download PDFs
# ==============================

def download_pdfs():

    print("\n🔍 Scraping MNFSR Publications Page...")

    os.makedirs(PDF_FOLDER, exist_ok=True)

    html = requests.get(URL).text
    soup = BeautifulSoup(html, "html.parser")

    pdf_links = []

    for link in soup.find_all("a"):
        href = link.get("href")

        if href and ".pdf" in href.lower():
            if not href.startswith("http"):
                href = "https://mnfsr.gov.pk" + href

            pdf_links.append(href)

    print("✅ Total PDFs Found:", len(pdf_links))

    for pdf_url in pdf_links:
        name = pdf_url.split("/")[-1]
        path = os.path.join(PDF_FOLDER, name)

        if os.path.exists(path):
            continue

        print("⬇ Downloading:", name)
        data = requests.get(pdf_url).content

        with open(path, "wb") as f:
            f.write(data)

    print("✅ All PDFs Ready!\n")


# ==============================
# STEP 2: Detect Scanned PDF
# ==============================

def is_scanned(pdf_path):

    try:
        tables = camelot.read_pdf(pdf_path, pages="1")
        return len(tables) == 0
    except:
        return True


# ==============================
# STEP 3: OCR Extract (FAST MODE)
# ==============================

def ocr_extract(pdf_path):

    print("🖼 OCR Running (Fast Mode)...")

    images = convert_from_path(pdf_path, first_page=1, last_page=MAX_PAGES_OCR)

    text = ""

    for img in images:
        text += pytesseract.image_to_string(img)

    return text


# ==============================
# STEP 4: Extract Tables
# ==============================

def extract_tables(pdf_path):

    filename = os.path.basename(pdf_path)

    print(f"\n📌 Processing: {filename}")

    all_rows = []

    # --- SCANNED PDF → OCR ---
    if is_scanned(pdf_path):

        print("🖼 Scanned PDF Detected → OCR Extracting...")

        try:
            text = ocr_extract(pdf_path)

            all_rows.append({
                "Source_File": filename,
                "Page": "OCR",
                "Table_Data": text[:2000]
            })

        except Exception as e:
            print("❌ OCR Failed:", e)

        return all_rows

    # --- TEXT PDF → Camelot ---
    try:
        tables = camelot.read_pdf(pdf_path, pages="1-end")

        print("✅ Tables Found:", len(tables))

        for i, table in enumerate(tables):
            df = table.df
            df.columns = df.iloc[0]
            df = df[1:]

            df["Source_File"] = filename
            df["Table_Number"] = i + 1

            all_rows.append(df)

    except Exception as e:
        print("❌ Camelot Error:", e)

    return all_rows


# ==============================
# STEP 5: Multi-thread Processing
# ==============================

def process_all_pdfs():

    os.makedirs(TABLE_FOLDER, exist_ok=True)

    pdf_files = [
        os.path.join(PDF_FOLDER, f)
        for f in os.listdir(PDF_FOLDER)
        if f.endswith(".pdf")
    ]

    print("📂 Total PDFs:", len(pdf_files))

    master_data = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        results = executor.map(extract_tables, pdf_files)

        for result in results:
            for item in result:

                if isinstance(item, pd.DataFrame):
                    master_data.append(item)

                elif isinstance(item, dict):
                    master_data.append(pd.DataFrame([item]))

    # Combine all
    print("\n📌 Combining into Master Dataset...")

    if master_data:
        final_df = pd.concat(master_data, ignore_index=True)
        final_df.to_csv(MASTER_FILE, index=False)

        print("🎉 DONE!")
        print("✅ Tableau Master CSV Saved:", MASTER_FILE)

    else:
        print("⚠ No data extracted.")


# ==============================
# RUN FULL PIPELINE
# ==============================

if __name__ == "__main__":

    download_pdfs()
    process_all_pdfs()
