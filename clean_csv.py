import pandas as pd
import os

csv_folder = "PBS_CSV_Output"
clean_folder = "PBS_Clean_CSV"

os.makedirs(clean_folder, exist_ok=True)

print("Cleaning CSV files...")

for file in os.listdir(csv_folder):

    if file.endswith(".csv"):

        path = os.path.join(csv_folder, file)

        df = pd.read_csv(path)

        # Remove empty rows
        df = df.dropna(how="all")

        # Reset index
        df = df.reset_index(drop=True)

        clean_path = os.path.join(clean_folder, file)

        df.to_csv(clean_path, index=False)

        print("✅ Cleaned:", file)

print("\n🎉 All Clean CSV saved in:", clean_folder)
