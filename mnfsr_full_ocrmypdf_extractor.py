import os
import requests
import pandas as pd
import warnings

from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text
import camelot
import ocrmypdf

warnings.filterwarnings("ignore")

# ===============================
# SETTINGS
# ===============================

MNFSR_URL = "https://mnfsr.gov.pk/Publications"

PDF_FOLDER = "MNFSR_PDFs"
OCR_FOLDER = "MNFSR_OCR_PDFs"

MASTER_FILE = "MNFSR_MASTER_DATASET.csv"

MAX_PAGES = 5  # Keep small for testing

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

    print("\n✅ All PDFs Downloaded Successfully!")


# ===============================
# STEP 2: CHECK TEXT PDF
# ===============================

def is_text_pdf(pdf_path):
    try:
        text = extract_text(pdf_path, maxpages=1)
        return bool(text.strip())
    except:
        return False


# ===============================
# STEP 3: OCR SCANNED PDF
# ===============================

def convert_scanned_to_searchable(pdf_path):

    os.makedirs(OCR_FOLDER, exist_ok=True)

    filename = os.path.basename(pdf_path)
    output_pdf = os.path.join(OCR_FOLDER, filename)

    if os.path.exists(output_pdf):
        return output_pdf

    print("🖼 Scanned PDF → Running OCRmyPDF...")

    try:
        ocrmypdf.ocr(
            pdf_path,
            output_pdf,
            deskew=True,
            force_ocr=True
        )

        print("✅ OCR Completed Successfully!")
        return output_pdf

    except Exception as e:
        print("❌ OCR Failed:", e)
        return None


# ===============================
# STEP 4: CLEAN TABLE DATA
# ===============================

def clean_dataframe(df):

    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")

    df.columns = [f"Column_{i+1}" for i in range(df.shape[1])]

    return df


# ===============================
# STEP 5: EXTRACT TABLES USING CAMELOT
# ===============================

def extract_tables(pdf_path):

    tables_list = []

    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=f"1-{MAX_PAGES}",
            flavor="stream"
        )

        print(f"📌 Tables Found: {tables.n}")

        for table in tables:

            df = clean_dataframe(table.df)

            if df.empty:
                continue

            df["Source_PDF"] = os.path.basename(pdf_path)
            df["Extraction_Method"] = "Camelot"

            tables_list.append(df)

    except Exception as e:
        print("❌ Table Extraction Failed:", e)

    return tables_list


# ===============================
# STEP 6: FULL MASTER PIPELINE
# ===============================

def run_full_extraction():

    all_tables = []

    pdf_files = [f for f in os.listdir(PDF_FOLDER) if f.endswith(".pdf")]

    print(f"\n📂 Total PDFs Found: {len(pdf_files)}")

    for pdf in pdf_files:

        pdf_path = os.path.join(PDF_FOLDER, pdf)

        print("\n====================================")
        print(f"📌 Processing: {pdf}")
        print("====================================")

        # Case 1: Text PDF
        if is_text_pdf(pdf_path):
            print("✅ Text PDF → Direct Camelot Extraction")
            tables = extract_tables(pdf_path)

        # Case 2: Scanned PDF
        else:
            searchable_pdf = convert_scanned_to_searchable(pdf_path)

            if searchable_pdf:
                tables = extract_tables(searchable_pdf)
            else:
                tables = []

        all_tables.extend(tables)

    if not all_tables:
        print("\n❌ No tables extracted.")
        return

    print("\n📌 Combining All Tables into Master Dataset...")

    master_df = pd.concat(all_tables, ignore_index=True)
    master_df.to_csv(MASTER_FILE, index=False)

    print("\n🎉 DONE SUCCESSFULLY!")
    print(f"✅ Master Dataset Saved: {MASTER_FILE}")


# ===============================
# RUN SCRIPT
# ===============================

if __name__ == "__main__":

    download_all_pdfs()
    run_full_extraction()
