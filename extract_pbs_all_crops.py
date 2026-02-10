import requests
import pandas as pd
import time

# -------------------------------
# STEP 1: Get List of All Crops
# -------------------------------

def get_all_crops():
    url = "https://na.data.gov.pk/Crops/GetCrops"
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to fetch crops list")
        return []

    crops = response.json()
    print(f"Total Crops Found: {len(crops)}")
    return crops


# ----------------------------------------
# STEP 2: Get Yearly Data for One Crop
# ----------------------------------------

def get_crop_yearly_data(crop_id):
    url = "https://na.data.gov.pk/Crops/GetYearly"
    params = {"cropId": crop_id}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        return []

    return response.json()


# ----------------------------------------
# STEP 3: Extract All Crops + Save to CSV
# ----------------------------------------

def extract_all_crops_data():
    crops = get_all_crops()

    all_records = []

    for crop in crops:
        crop_id = crop["cropId"]
        crop_name = crop["cropName"]

        print(f"Extracting: {crop_name}")

        yearly_data = get_crop_yearly_data(crop_id)

        for row in yearly_data:
            all_records.append({
                "Crop": crop_name,
                "FiscalYear": row.get("fiscalyear"),
                "Production": row.get("production"),
                "Area": row.get("area"),
                "Yield": row.get("yield")
            })

        # Small delay to avoid server overload
        time.sleep(0.2)

    # Convert to DataFrame
    df = pd.DataFrame(all_records)

    # Save Clean CSV
    df.to_csv("PBS_All_Crops_Yearly_Data.csv", index=False)

    print("\nExtraction Completed Successfully!")
    print("📁 Saved File: PBS_All_Crops_Yearly_Data.csv")


# -------------------------------
# MAIN PROGRAM
# -------------------------------

if __name__ == "__main__":
    extract_all_crops_data()
