import requests
import pandas as pd
import time

session = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://na.data.gov.pk/Crops/Home",
    "X-Requested-With": "XMLHttpRequest",
}


# -----------------------------------
# 1. Extract Crop List
# -----------------------------------
def get_crop_list():
    url = "https://na.data.gov.pk/Crops/GetCrops"

    print("🔍 Fetching Crop List...")

    r = session.get(url, headers=HEADERS)
    print("Status:", r.status_code)

    data = r.json()

    print("\n✅ Sample Crop Record:")
    print(data[0])

    print(f"\n✅ Total Crops Found: {len(data)}")
    return data


# -----------------------------------
# 2. Extract Yearly Data for Crop
# -----------------------------------
def get_yearly_crop_data(crop_id):
    url = "https://na.data.gov.pk/Crops/GetYearly"

    params = {
        "cropId": crop_id,
        "CropID": crop_id,
        "id": crop_id
    }

    r = session.get(url, headers=HEADERS, params=params)

    if r.status_code != 200:
        return []

    try:
        return r.json()
    except:
        return []


# -----------------------------------
# 3. Extract ALL Crops
# -----------------------------------
def extract_all_crops():
    crops = get_crop_list()

    all_rows = []

    for crop in crops:

        # Auto-detect correct keys
        crop_id = crop.get("cropId") or crop.get("CropId") or crop.get("ID") or crop.get("id")
        crop_name = crop.get("cropName") or crop.get("CropName") or crop.get("Name") or crop.get("name")

        if crop_id is None:
            continue

        print(f"\n📌 Extracting Crop: {crop_name} (ID={crop_id})")

        yearly_data = get_yearly_crop_data(crop_id)

        if not yearly_data:
            print("⚠ No yearly data found.")
            continue

        for row in yearly_data:
            all_rows.append({
                "Crop": crop_name,
                "Year": row.get("fiscalyear") or row.get("Year"),
                "Production": row.get("production"),
                "Area": row.get("area"),
                "Yield": row.get("yield")
            })

        time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    df.to_csv("PBS_ALL_CROPS_FINAL.csv", index=False)

    print("\n🎉 DONE!")
    print("✅ Saved: PBS_ALL_CROPS_FINAL.csv")


# -----------------------------------
# RUN
# -----------------------------------
if __name__ == "__main__":
    extract_all_crops()
