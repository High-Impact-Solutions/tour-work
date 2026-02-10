import requests
import pandas as pd
import os

# ============================================
# OUTPUT SETTINGS
# ============================================

OUTPUT_FOLDER = "PBS_API_Crop_Data"
MASTER_FILE = "PBS_Master_Tableau.csv"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ============================================
# PBS API ENDPOINTS
# ============================================

CROP_LIST_URL = "https://na.data.gov.pk/Crops/GetCropList"
YEARLY_URL = "https://na.data.gov.pk/Crops/GetYearly"

headers = {
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest"
}

# ============================================
# STEP 1: GET ALL CROPS
# ============================================

print("🌾 Downloading Crop List...")

crop_response = requests.get(CROP_LIST_URL, headers=headers)

crop_list = crop_response.json()

print("✅ Crops Found:", len(crop_list))

# ============================================
# STEP 2: LOOP CROPS + EXTRACT YEARLY DATA
# ============================================

master_rows = []

for crop in crop_list:

    crop_id = crop["CropID"]
    crop_name = crop["CropName"]

    print(f"\n🌾 Extracting: {crop_name} (ID={crop_id})")

    payload = {
        "cropId": crop_id
    }

    r = requests.get(YEARLY_URL, params=payload, headers=headers)

    try:
        data = r.json()
    except:
        print("⚠ Invalid JSON response")
        continue

    if not data:
        print("⚠ No yearly data found.")
        continue

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Add Crop Column
    df.insert(0, "Crop", crop_name)

    # Save Crop CSV
    crop_file = os.path.join(
        OUTPUT_FOLDER,
        f"{crop_name.replace(' ', '_')}.csv"
    )

    df.to_csv(crop_file, index=False)

    print("✅ Saved:", crop_file)

    master_rows.append(df)

# ============================================
# STEP 3: SAVE MASTER TABLEAU FILE
# ============================================

if master_rows:
    master_df = pd.concat(master_rows, ignore_index=True)

    master_path = os.path.join(OUTPUT_FOLDER, MASTER_FILE)

    master_df.to_csv(master_path, index=False)

    print("\n🎉 MASTER TABLEAU DATASET CREATED!")
    print("📌 File:", master_path)

else:
    print("\n❌ No crop data extracted at all.")
