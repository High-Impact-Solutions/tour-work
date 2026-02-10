import requests
import pandas as pd

# API URL
url = "https://na.data.gov.pk/Crops/GetYearly"

# Example cropId (Wheat)
params = {
    "cropId": 1
}

print("Fetching data from PBS API...")

# Send request
response = requests.get(url, params=params)

# Convert response JSON to Python list
data = response.json()

# Convert to DataFrame
df = pd.DataFrame(data)

# Add crop name column manually
df["crop"] = "Wheat"

# Save to CSV
df.to_csv("wheat_yearly_data.csv", index=False)

print("Done! Data saved as wheat_yearly_data.csv")
