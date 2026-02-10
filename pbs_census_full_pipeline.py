import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import tabula


# ================================
# SETTINGS
# ================================
BASE_URL = "https://www.pbs.gov.pk"
PAGE_URL = "https://www.pbs.gov.pk/agriculture-census/"

PDF_FOLDER = "PBS_Census_PDFs"
CSV_FOLDER = "PBS_Census_CSV"
CLEAN_FOLDER = "PBS_Census_CleanCSV"

os.makedirs(PDF_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(CLEAN_FOLDER, exist_ok=True)


# ================================
# STEP 1: SCRAPE PDF LINKS
# ================================
print("\n🔍 Scraping Agriculture Census page...")

response = requests.get(PAGE_URL)
soup = BeautifulSoup(response.text, "html.parser")

pdf_links = []

for a in soup.find_all("a"):
    href = a.get("href")

    if href and ".pdf" in href.lower():

        # Convert relative → absolute URL
        if href.startswith("/"):
            href = BASE_URL + href

        pdf_links.append(href)

pdf_links = list(set(pdf_links))  # remove duplicates

print(f"✅ Found {len(pdf_links)} PDF files.\n")


# ================================
# STEP 2: DOWNLOAD ALL PDFs
# ================================
print("⬇️ Downloading PDFs...\n")

for i, pdf_url in enumerate(pdf_links, start=1):
    try:
        file_name = f"Census_{i}.pdf"
        file_path = os.path.join(PDF_FOLDER, file_name)

        print(f"Downloading ({i}/{len(pdf_links)}): {file_name}")

        pdf_data = requests.get(pdf_url).content

        with open(file_path, "wb") as f:
            f.write(pdf_data)

    except Exception as e:
        print("❌ Failed:", pdf_url, e)

print("\n✅ All PDFs downloaded successfully!")


# ================================
# STEP 3: EXTRACT TABLES USING TABULA
# ================================
print("\n📊 Extracting tables from PDFs...\n")

for pdf_file in os.listdir(PDF_FOLDER):

    if pdf_file.endswith(".pdf"):

        pdf_path = os.path.join(PDF_FOLDER, pdf_file)

        print("📄 Extracting:", pdf_file)

        try:
            # Extract tables from all pages
            tables = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True)

            if not tables:
                print("⚠️ No tables found in:", pdf_file)
                continue

            # Save each table separately
            for t, table in enumerate(tables, start=1):

                raw_csv = os.path.join(
                    CSV_FOLDER,
                    pdf_file.replace(".pdf", f"_Table{t}.csv")
                )

                table.to_csv(raw_csv, index=False)

            print(f"✅ Extracted {len(tables)} tables from {pdf_file}")

        except Exception as e:
            print("❌ Extraction failed:", pdf_file, e)


# ================================
# STEP 4: AUTO CLEAN CSV TABLES
# ================================
print("\n🧹 Cleaning extracted CSV tables...\n")

def clean_dataframe(df):

    # Drop empty rows/cols
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    # Replace line breaks
    df = df.replace(r"\n", " ", regex=True)

    # Remove extra spaces
    df = df.replace(r"\s+", " ", regex=True)

    # Drop duplicate header rows inside table
    df = df.loc[:, ~df.columns.duplicated()]

    return df


for csv_file in os.listdir(CSV_FOLDER):

    if csv_file.endswith(".csv"):

        csv_path = os.path.join(CSV_FOLDER, csv_file)

        try:
            df = pd.read_csv(csv_path)

            df_clean = clean_dataframe(df)

            clean_path = os.path.join(CLEAN_FOLDER, csv_file)

            df_clean.to_csv(clean_path, index=False)

            print("✅ Clean Saved:", csv_file)

        except Exception as e:
            print("❌ Cleaning failed:", csv_file, e)


print("\n🎉 PIPELINE COMPLETE!")
print("📂 PDFs saved in:", PDF_FOLDER)
print("📂 Raw CSV tables saved in:", CSV_FOLDER)
print("📂 Clean structured CSV saved in:", CLEAN_FOLDER)
