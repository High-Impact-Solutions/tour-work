import os
import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


# ==========================================
# PBS Crop × Year Extractor
# ==========================================

URL = "https://na.data.gov.pk/Crops/Home"

OUTPUT_FOLDER = "PBS_Crop_Data"


def extract_all_crop_year_data():

    print("🚀 Launching Chrome Browser...")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install())
    )

    driver.get(URL)
    time.sleep(5)

    # Create output folder
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # --------------------------------------
    # Load Dropdowns
    # --------------------------------------

    crop_dropdown = Select(driver.find_element(By.ID, "CropId"))
    year_dropdown = Select(driver.find_element(By.ID, "YearId"))

    crops = crop_dropdown.options
    years = year_dropdown.options

    print("🌾 Total Crops Found:", len(crops) - 1)
    print("📅 Total Years Found:", len(years) - 1)

    # --------------------------------------
    # Loop Crops
    # --------------------------------------

    for c in range(1, len(crops)):

        crop_dropdown = Select(driver.find_element(By.ID, "CropId"))
        crop_dropdown.select_by_index(c)

        crop_name = crop_dropdown.options[c].text.strip()
        crop_folder = os.path.join(OUTPUT_FOLDER, crop_name.replace(" ", "_"))

        if not os.path.exists(crop_folder):
            os.makedirs(crop_folder)

        print(f"\n🌾 Crop: {crop_name}")

        time.sleep(2)

        # --------------------------------------
        # Loop Years
        # --------------------------------------

        for y in range(1, len(years)):

            year_dropdown = Select(driver.find_element(By.ID, "YearId"))
            year_dropdown.select_by_index(y)

            year_value = year_dropdown.options[y].text.strip()

            print(f"   📅 Year: {year_value}")

            time.sleep(3)

            # --------------------------------------
            # Extract Table
            # --------------------------------------

            try:
                table = driver.find_element(By.ID, "tblCropData")
                rows = table.find_elements(By.TAG_NAME, "tr")

                extracted_data = []

                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if cols:
                        extracted_data.append(
                            [c.text.strip() for c in cols]
                        )

                if not extracted_data:
                    print("      ⚠ No data found.")
                    continue

                # Convert into DataFrame
                df = pd.DataFrame(extracted_data)

                # Save CSV
                filename = f"{crop_name}_{year_value}.csv"
                filepath = os.path.join(
                    crop_folder,
                    filename.replace(" ", "_")
                )

                df.to_csv(filepath, index=False)

                print("      ✅ Saved:", filepath)

            except Exception as e:
                print("      ❌ Table error:", e)

    driver.quit()
    print("\n🎉 DONE! All Crop-Year CSV files saved successfully.")


# ==========================================
# RUN SCRIPT
# ==========================================

if __name__ == "__main__":
    extract_all_crop_year_data()
