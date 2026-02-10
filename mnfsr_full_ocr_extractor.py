import os

POPPLER_PATH = r"C:\poppler\Library\bin"

import os
import requests
import pandas as pd
import warnings

from bs4 import BeautifulSoup

import camelot
from pdfminer.high_level import extract_text

from pdf2image import convert_from_path
import pytesseract

warnings.filterwarnings("ignore")

# ===============================
# SETTINGS
# ===============================

MNFSR_URL = "https://mnfsr.gov.pk/Publications"

PDF_FOLDER = "MNFSR_PDFs"
MASTER_FILE = "MNFSR_MASTER_DATASET.csv"

MAX_PAGES = 5   # OCR is slow, keep small first

# ✅ Set your Tesseract path (Windows)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


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

        filename = url.split("/")[-1].replace("%20", "_")
        filepath = os.path.join(PDF_FOLDER, filename)

        if os.path.exists(filepath):
            continue

        print(f"⬇ Downloading: {filename}")
        pdf_data = requests.get(url).content

        with open(filepath, "wb") as f:
            f.write(pdf_data)

    print("✅ All PDFs Downloaded!")


# ===============================
# STEP 2: CHECK TEXT OR SCANNED
# ===============================

def is_text_pdf(pdf_path):
    try:
        text = extract_text(pdf_path, maxpages=1)
        return bool(text.strip())
    except:
        return False


# ===============================
# STEP 3: CLEAN TABLES
# ===============================

def clean_dataframe(df):

    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")

    df.columns = [f"Column_{i+1}" for i in range(df.shape[1])]

    return df


# ===============================
# STEP 4A: EXTRACT NORMAL TABLES
# ===============================

def extract_camelot_tables(pdf_path):

    tables_list = []

    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=f"1-{MAX_PAGES}",
            flavor="stream"
        )

        for i, table in enumerate(tables):
            df = clean_dataframe(table.df)

            if df.empty:
                continue

            df["Source_PDF"] = os.path.basename(pdf_path)
            df["Method"] = "Camelot"

            tables_list.append(df)

    except:
        pass

    return tables_list

def extract_ocr_tables(pdf_path):

    print("🖼 Scanned PDF → OCR Running...")

    tables_list = []

    try:
        images = convert_from_path(
            pdf_path,
            poppler_path=POPPLER_PATH,   # ✅ VERY IMPORTANT
            first_page=1,
            last_page=MAX_PAGES
        )

        for page_num, img in enumerate(images):

            print(f"   🔍 OCR Page {page_num+1}")

            text = pytesseract.image_to_string(img)

            lines = [line.strip() for line in text.split("\n") if line.strip()]

            if len(lines) < 5:
                continue

            rows = []
            for line in lines:
                parts = line.split()
                if len(parts) >= 2:
                    rows.append(parts)

            if not rows:
                continue

            df = pd.DataFrame(rows)
            df = clean_dataframe(df)

            df["Source_PDF"] = os.path.basename(pdf_path)
            df["Method"] = "OCR"

            tables_list.append(df)

    except Exception as e:
        print("❌ OCR Failed:", e)

    return tables_list


# ===============================
# STEP 4B: OCR TABLE EXTRACTION
# ===============================

def extract_ocr_tables(pdf_path):

    print("🖼 Scanned PDF → OCR Running...")

    tables_list = []

    try:
        images = convert_from_path(pdf_path, first_page=1, last_page=MAX_PAGES)

        for page_num, img in enumerate(images):

            print(f"   🔍 OCR Page {page_num+1}")

            text = pytesseract.image_to_string(img)

            lines = [line.strip() for line in text.split("\n") if line.strip()]

            if len(lines) < 5:
                continue

            # Convert OCR lines into row format
            rows = []
            for line in lines:
                parts = line.split()
                if len(parts) >= 2:
                    rows.append(parts)

            if not rows:
                continue

            df = pd.DataFrame(rows)
            df = clean_dataframe(df)

            df["Source_PDF"] = os.path.basename(pdf_path)
            df["Method"] = "OCR"

            tables_list.append(df)

    except Exception as e:
        print("❌ OCR Failed:", e)

    return tables_list


# ===============================
# STEP 5: MASTER PIPELINE
# ===============================

def run_full_extraction():

    all_tables = []

    pdf_files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]

    print(f"\n📂 Total PDFs Found: {len(pdf_files)}")

    for pdf in pdf_files:

        pdf_path = os.path.join(PDF_FOLDER, pdf)

        print(f"\n📌 Processing: {pdf}")

        if is_text_pdf(pdf_path):
            print("✅ Text PDF → Camelot Extracting...")
            tables = extract_camelot_tables(pdf_path)

        else:
            tables = extract_ocr_tables(pdf_path)

        all_tables.extend(tables)

    if not all_tables:
        print("\n❌ No data extracted.")
        return

    print("\n📌 Combining All Tables...")

    master_df = pd.concat(all_tables, ignore_index=True)
    master_df.to_csv(MASTER_FILE, index=False)

    print("\n🎉 DONE!")
    print(f"✅ Master Dataset Saved: {MASTER_FILE}")


# ===============================
# RUN SCRIPT
# ===============================

if __name__ == "__main__":

    download_all_pdfs()
    run_full_extraction()
