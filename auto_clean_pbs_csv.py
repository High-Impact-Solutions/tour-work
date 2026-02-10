import pandas as pd
import os


# Folder containing messy extracted CSVs
input_folder = "PBS_CSV_Output"

# Folder for cleaned CSVs
output_folder = "PBS_Clean_CSV"
os.makedirs(output_folder, exist_ok=True)


def clean_table(csv_path):
    """
    Cleans messy PDF-extracted tables like PBS Table_1.csv
    """

    df = pd.read_csv(csv_path)

    # ----------------------------
    # 1. Drop fully empty rows
    # ----------------------------
    df = df.dropna(how="all")

    # ----------------------------
    # 2. Remove messy header rows
    # (Usually first 3–6 rows)
    # ----------------------------
    df = df.iloc[4:].reset_index(drop=True)

    # ----------------------------
    # 3. Drop fully empty columns
    # ----------------------------
    df = df.dropna(axis=1, how="all")

    # ----------------------------
    # 4. Replace broken line breaks
    # ----------------------------
    df = df.replace(r"\n", " ", regex=True)

    # ----------------------------
    # 5. Remove duplicate spaces
    # ----------------------------
    df = df.replace(r"\s+", " ", regex=True)

    # ----------------------------
    # 6. Reset column names
    # ----------------------------
    df.columns = [f"Col_{i}" for i in range(len(df.columns))]

    return df


# ----------------------------
# Process all CSV files
# ----------------------------
print("🔄 Cleaning PBS CSV files...\n")

for file in os.listdir(input_folder):

    if file.endswith(".csv"):
        input_path = os.path.join(input_folder, file)

        print("📄 Cleaning:", file)

        cleaned_df = clean_table(input_path)

        output_path = os.path.join(output_folder, file)
        cleaned_df.to_csv(output_path, index=False)

        print("✅ Saved Clean CSV:", output_path)


print("\n🎉 All files cleaned successfully!")
print("📂 Cleaned tables are inside:", output_folder)
