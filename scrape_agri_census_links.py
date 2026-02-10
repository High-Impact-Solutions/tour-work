import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.pbs.gov.pk/agriculture-census/"

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

links_data = []

for link in soup.find_all("a"):
    title = link.get_text(strip=True)
    href = link.get("href")

    if title and href:
        if href.startswith("/"):
            href = "https://www.pbs.gov.pk" + href
        links_data.append({"title": title, "url": href})

df = pd.DataFrame(links_data)
df.to_csv("pbs_agri_census_links.csv", index=False)

print("Done — links saved to pbs_agri_census_links.csv")
