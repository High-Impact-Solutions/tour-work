import pdfplumber
import pandas as pd
import os

# Folder where PDFs are saved
pdf_folder = "PBS_PDF_Tables"

# Folder where CSV files will be saved
csv_folder = "PBS_CSV_Output"
os.makedirs(csv_folder, exist_ok=True)

print("📂 Reading PDFs from:", pdf_folder)

# Loop through all PDF files
for file_name in os.listdir(pdf_folder):

    if file_name.endswith(".pdf"):
        pdf_path = os.path.join(pdf_folder, file_name)

        print("\n==============================")
        print("📄 Extracting:", file_name)

        all_tables = []

        with pdfplumber.open(pdf_path) as pdf:

            # Loop through pages
            for page_number, page in enumerate(pdf.pages, start=1):

                tables = page.extract_table()

                if tables:
                    df = pd.DataFrame(tables)

                    all_tables.append(df)

                    print(f"✅ Table found on page {page_number}")

        # Save extracted tables
        if all_tables:
            final_df = pd.concat(all_tables, ignore_index=True)

            csv_name = file_name.replace(".pdf", ".csv")
            csv_path = os.path.join(csv_folder, csv_name)

            final_df.to_csv(csv_path, index=False)

            print("✅ Saved CSV:", csv_path)

        else:
            print("⚠️ No table detected in:", file_name)

print("\n🎉 Extraction Completed!")
print("All CSV files saved in:", csv_folder)
