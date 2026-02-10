import requests
import pandas as pd

# -----------------------------
# STEP 1: Fetch Crops Properly
# -----------------------------

def get_all_crops():
    url = "https://na.data.gov.pk/Crops/GetCrops"

    response = requests.get(url)

    print("\nStatus Code:", response.status_code)
    print("Response Preview:", response.text[:300])

    # If response is not JSON
    if "application/json" not in response.headers.get("Content-Type", ""):
        print("API did not return JSON!")
        return []

    data = response.json()

    # If response is inside "data"
    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    # Ensure list
    if not isinstance(data, list):
        print("Unexpected crops format:", type(data))
        return []

    print(f"Crops Found: {len(data)}")
    return data


# -----------------------------
# STEP 2: Fetch Yearly Crop Data
# -----------------------------

def get_yearly_data(crop_id):
    url = "https://na.data.gov.pk/Crops/GetYearly"
    params = {"cropId": crop_id}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        return []

    try:
        return response.json()
    except:
        return []


# -----------------------------
# STEP 3: Main Extraction Logic
# -----------------------------

def extract_all():
    crops = get_all_crops()

    if not crops:
        print("No crops extracted. Stopping.")
        return

    all_rows = []

    for crop in crops:

        # Debug crop structure
        print("\nCrop object:", crop)

        crop_id = crop.get("cropId")
        crop_name = crop.get("cropName")

        if crop_id is None:
            continue

        print(f"Extracting yearly data for: {crop_name}")

        yearly = get_yearly_data(crop_id)

        for row in yearly:
            all_rows.append({
                "Crop": crop_name,
                "Year": row.get("fiscalyear"),
                "Production": row.get("production"),
                "Area": row.get("area"),
                "Yield": row.get("yield")
            })

    df = pd.DataFrame(all_rows)

    df.to_csv("PBS_Crops_Clean.csv", index=False)
    print("\nDONE! Saved: PBS_Crops_Clean.csv")


# -----------------------------
# RUN PROGRAM
# -----------------------------

if __name__ == "__main__":
    extract_all()
