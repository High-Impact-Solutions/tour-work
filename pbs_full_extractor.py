import requests
import pandas as pd
import time

# -----------------------------------
# 1. Setup Session + Browser Headers
# -----------------------------------

session = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://na.data.gov.pk/Crops/Home",
    "X-Requested-With": "XMLHttpRequest",
}


# -----------------------------------
# 2. Extract Crop List
# -----------------------------------

def get_crop_list():
    url = "https://na.data.gov.pk/Crops/GetCrops"

    print("🔍 Fetching Crop List...")

    r = session.get(url, headers=HEADERS)

    print("Status:", r.status_code)

    if r.status_code != 200:
        print("❌ Crop list request failed")
        return []

    try:
        data = r.json()
    except:
        print("❌ Response not JSON. Preview:")
        print(r.text[:300])
        return []

    # Sometimes response is wrapped
    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    if not isinstance(data, list):
        print("❌ Unexpected crop format")
        return []

    print(f"✅ Total Crops Found: {len(data)}")
    return data


# -----------------------------------
# 3. Extract Yearly Data for One Crop
# -----------------------------------

def get_yearly_crop_data(crop_id):
    url = "https://na.data.gov.pk/Crops/GetYearly"

    params = {"cropId": crop_id}

    r = session.get(url, headers=HEADERS, params=params)

    if r.status_code != 200:
        return []

    try:
        return r.json()
    except:
        return []


# -----------------------------------
# 4. Extract ALL Crops + Save Clean CSV
# -----------------------------------

def extract_all_crops():
    crops = get_crop_list()

    if not crops:
        print("❌ No crops found. Stopping.")
        return

    all_rows = []

    for crop in crops:
        crop_id = crop.get("cropId")
        crop_name = crop.get("cropName")

        print(f"\n📌 Extracting: {crop_name}")

        yearly_data = get_yearly_crop_data(crop_id)

        if not yearly_data:
            print("⚠ No yearly data found.")
            continue

        for row in yearly_data:
            all_rows.append({
                "Crop": crop_name,
                "FiscalYear": row.get("fiscalyear"),
                "Production": row.get("production"),
                "Area": row.get("area"),
                "Yield": row.get("yield")
            })

        time.sleep(0.5)

    # Save Clean CSV
    df = pd.DataFrame(all_rows)
    df.to_csv("PBS_ALL_CROPS_CLEAN.csv", index=False)

    print("\n🎉 DONE!")
    print("✅ File Saved: PBS_ALL_CROPS_CLEAN.csv")


# -----------------------------------
# RUN PROGRAM
# -----------------------------------

if __name__ == "__main__":
    extract_all_crops()
