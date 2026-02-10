import requests
from bs4 import BeautifulSoup
import os

url = "https://www.pbs.gov.pk/agriculture-sector-of-pakistan-importance-role-key-statistics/"

# Folder to save PDFs
folder = "PBS_PDF_Tables"
os.makedirs(folder, exist_ok=True)

print("Connecting to PBS page...")

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

pdf_count = 0

for link in soup.find_all("a"):
    href = link.get("href")
    title = link.get_text(strip=True)

    if href and href.endswith(".pdf"):

        # Fix relative URLs
        if href.startswith("/"):
            href = "https://www.pbs.gov.pk" + href

        pdf_count += 1
        print(f"Downloading PDF {pdf_count}: {title}")

        pdf_data = requests.get(href)

        file_path = os.path.join(folder, f"Table_{pdf_count}.pdf")

        with open(file_path, "wb") as f:
            f.write(pdf_data.content)

print("\n✅ All PDFs downloaded successfully!")
print("Saved inside folder:", folder)
