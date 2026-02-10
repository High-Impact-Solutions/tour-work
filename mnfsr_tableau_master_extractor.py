import os
import requests
import pandas as pd

from bs4 import BeautifulSoup

import camelot
import pdfplumber

from paddleocr import PaddleOCR

# ===============================
# SETTINGS
# ===============================

MNFSR_URL = "https://mnfsr.gov.pk/Publications"

PDF_FOLDER = "MNFSR_PDFs"
OUTPUT_FOLDER = "MNFSR_Clean_Tables"

MASTER_FILE = "MNFSR_TABLEAU_MASTER.csv"

MAX_PAGES = 3   # Keep small first

os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ===============================
# INIT OCR ENGINE
# ===============================

ocr = PaddleOCR(use_angle_cls=True, lang="en")


# ===============================
# STEP 1: DOWNLOAD PDFs
# ===============================

def download_all_pdfs():

    print("\n🔍 Scraping MNFSR Publications Page...")

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
            print(f"⚠ Already Downloaded: {filename}")
            continue

        print(f"⬇ Downloading: {filename}")
        pdf_data = requests.get(url).content

        with open(filepath, "wb") as f:
            f.write(pdf_data)

    print("✅ PDF Download Complete!")


# ===============================
# STEP 2: TEXT PDF CHECK
# ===============================

def is_text_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            return bool(text)
    except:
        return False


# ===============================
# STEP 3: CLEAN TABLE
# ===============================

def clean_table(df):

    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")

    df.columns = [f"Col_{i+1}" for i in range(df.shape[1])]

    return df


# ===============================
# STEP 4A: CAMELot TABLES
# ===============================

def extract_camelot(pdf_path):

    extracted = []

    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=f"1-{MAX_PAGES}",
            flavor="stream"
        )

        for t in tables:
            df = clean_table(t.df)

            if df.empty:
                continue

            df["Source"] = os.path.basename(pdf_path)
            df["Method"] = "Camelot"

            extracted.append(df)

    except:
        pass

    return extracted


# ===============================
# STEP 4B: OCR TABLE EXTRACTION
# ===============================

def extract_ocr(pdf_path):

    extracted = []

    print("🖼 Scanned PDF → PaddleOCR Running...")

    with pdfplumber.open(pdf_path) as pdf:

        for page_no in range(min(MAX_PAGES, len(pdf.pages))):

            page = pdf.pages[page_no]
            image = page.to_image(resolution=250).original

            result = ocr.ocr(image)

            rows = []
            for line in result[0]:
                text = line[1][0]
                rows.append([text])

            if len(rows) < 5:
                continue

            df = pd.DataFrame(rows)
            df = clean_table(df)

            df["Source"] = os.path.basename(pdf_path)
            df["Method"] = "OCR"

            extracted.append(df)

    return extracted


# ===============================
# STEP 5: MASTER PIPELINE
# ===============================

def run_pipeline():

    all_tables = []

    pdf_files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]

    print(f"\n📂 Processing {len(pdf_files)} PDFs...\n")

    for pdf in pdf_files:

        pdf_path = os.path.join(PDF_FOLDER, pdf)

        print(f"📌 {pdf}")

        if is_text_pdf(pdf_path):
            print("   ✅ Text PDF → Camelot")
            tables = extract_camelot(pdf_path)

        else:
            print("   🖼 Scanned PDF → OCR")
            tables = extract_ocr(pdf_path)

        if tables:
            pdf_out = os.path.join(
                OUTPUT_FOLDER,
                pdf.replace(".pdf", "_tables.csv")
            )

            combined = pd.concat(tables, ignore_index=True)
            combined.to_csv(pdf_out, index=False)

            print(f"   ✅ Saved: {pdf_out}")

            all_tables.extend(tables)

        else:
            print("   ⚠ No tables found")

    if not all_tables:
        print("\n❌ No data extracted.")
        return

    print("\n📌 Creating Master Tableau Dataset...")

    master = pd.concat(all_tables, ignore_index=True)
    master.to_csv(MASTER_FILE, index=False)

    print("\n🎉 DONE!")
    print(f"✅ Tableau Master Dataset Saved: {MASTER_FILE}")


# ===============================
# RUN
# ===============================

if __name__ == "__main__":
    download_all_pdfs()
    run_pipeline()
