import os
import camelot
import pytesseract
from pdf2image import convert_from_path
import pandas as pd


PDF_FILE = "report.pdf"
OUTPUT_FOLDER = "Extracted_Data"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# ================================
# 1. Extract Tables (Camelot)
# ================================
def extract_tables():
    print("📌 Extracting tables using Camelot...")

    tables = camelot.read_pdf(PDF_FILE, pages="all")

    print("✅ Tables Found:", tables.n)

    for i, table in enumerate(tables):
        df = table.df

        file_name = f"table_{i+1}.csv"
        df.to_csv(os.path.join(OUTPUT_FOLDER, file_name),
                  index=False)

        print("✅ Saved:", file_name)


# ================================
# 2. OCR Text Extraction
# ================================
def extract_text_ocr():
    print("\n📌 Running OCR on scanned pages...")

    images = convert_from_path(PDF_FILE)

    all_text = ""

    for i, img in enumerate(images):
        print("🔍 OCR Page:", i + 1)

        text = pytesseract.image_to_string(img)

        all_text += f"\n\n--- PAGE {i+1} ---\n{text}"

    with open(os.path.join(OUTPUT_FOLDER, "full_text.txt"),
              "w", encoding="utf-8") as f:
        f.write(all_text)

    print("✅ OCR Text Saved: full_text.txt")


# ================================
# RUN FULL PIPELINE
# ================================
if __name__ == "__main__":
    extract_tables()
    extract_text_ocr()

    print("\n🎉 DONE! Tables + OCR Extracted Successfully")
