import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import os

# Base URL of the publications page
BASE_URL = "https://mnfsr.gov.pk"
PUBLICATIONS_URL = f"{BASE_URL}/Publications"

# Output CSV
OUTPUT_CSV = "MNFSR_Publications.csv"

# Folder to save PDF files
PDF_FOLDER = "MNFSR_Publications_PDFs"

# Create folders
os.makedirs(PDF_FOLDER, exist_ok=True)

print("📡 Fetching Publications page...")

resp = requests.get(PUBLICATIONS_URL)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")

# Find the publications table
table = soup.find("table")
if not table:
    print("❌ Could not find publications table.")
    exit()

rows = table.find_all("tr")

publications = []

for tr in rows[1:]:
    cols = tr.find_all("td")
    if not cols:
        continue

    title = cols[1].get_text(strip=True)
    date = cols[2].get_text(strip=True)
    download_link = None

    link_tag = cols[3].find("a")
    if link_tag and link_tag.has_attr("href"):
        download_link = urljoin(BASE_URL, link_tag["href"])

    publications.append({
        "Title": title,
        "Date": date,
        "DownloadLink": download_link
    })

# Save the list as CSV
df = pd.DataFrame(publications)
df.to_csv(OUTPUT_CSV, index=False)

print(f"✅ Saved publication list to {OUTPUT_CSV}")

# Optional: Download PDFs
print("\n📥 Downloading PDFs...")

for pub in publications:
    link = pub["DownloadLink"]
    if not link:
        print(f"⚠ No download link for: {pub['Title']}")
        continue

    filename = os.path.join(PDF_FOLDER, link.split("/")[-1])
    try:
        pdf_resp = requests.get(link)
        pdf_resp.raise_for_status()
        with open(filename, "wb") as f:
            f.write(pdf_resp.content)
        print(f"   ✔ Downloaded: {filename}")
    except Exception as e:
        print(f"   ❌ Failed to download {link}: {e}")

print("\n🎉 Done!")
