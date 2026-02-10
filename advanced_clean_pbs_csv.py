import pandas as pd
import os
import re

# Input messy folder
input_folder = "PBS_CSV_Output"

# Output clean folder
output_folder = "PBS_Advanced_Clean"
os.makedirs(output_folder, exist_ok=True)


def find_table_start(df):
    """
    Detect the first real row of data.
    Usually the first row containing numbers is the start.
    """
    for i in range(len(df)):
        row = df.iloc[i].astype(str)

        # Check if row contains any numeric value
        if row.str.contains(r"\d", regex=True).any():
            return i

    return 0


def clean_pbs_table(csv_path):
    """
    Advanced cleaning for PBS extracted tables
    """

    df = pd.read_csv(csv_path)

    # ----------------------------
    # 1. Drop fully empty rows/cols
    # ----------------------------
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    # ----------------------------
    # 2. Replace line breaks inside cells
    # ----------------------------
    df = df.replace(r"\n", " ", regex=True)

    # ----------------------------
    # 3. Remove extra spaces
    # ----------------------------
    df = df.replace(r"\s+", " ", regex=True)

    # ----------------------------
    # 4. Detect table start row automatically
    # ----------------------------
    start_row = find_table_start(df)
    df = df.iloc[start_row:].reset_index(drop=True)

    # ----------------------------
    # 5. Drop rows that are still useless
    # (like "Source: PBS")
    # ----------------------------
    df = df[~df.apply(lambda row: row.astype(str).str.contains("Source", case=False).any(), axis=1)]

    # ----------------------------
    # 6. Reset column names properly
    # ----------------------------
    df.columns = [f"Column_{i+1}" for i in range(len(df.columns))]

    return df


# ----------------------------
# Clean All CSV Files
# ----------------------------
print("\n🚀 Advanced Cleaning Started...\n")

for file in os.listdir(input_folder):

    if file.endswith(".csv"):

        input_path = os.path.join(input_folder, file)
        print("📄 Processing:", file)

        try:
            cleaned_df = clean_pbs_table(input_path)

            output_path = os.path.join(output_folder, file)
            cleaned_df.to_csv(output_path, index=False)

            print("✅ Clean Saved:", output_path)

        except Exception as e:
            print("❌ Failed for:", file)
            print("Error:", e)

print("\n🎉 Advanced Cleaning Completed!")
print("📂 Output Folder:", output_folder)
