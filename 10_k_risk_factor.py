from bs4 import BeautifulSoup

def get_risk_factors(cik: str) -> str:
    #return item 1a risk factors text from most recent 10-k
    cache_path = f"{CACHE_DIR}/{cik}_risk_factors.txt"
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            return f.read()

    # get  filing list
    r = requests.get(
        f"https://data.sec.gov/submissions/CIK{cik}.json",
        headers=HEADERS
    )
    filings = r.json()["filings"]["recent"]

    # find most recent 10-K
    for i, form in enumerate(filings["form"]):
        if form == "10-K":
            accession = filings["accessionNumber"][i].replace("-", "")
            primary_doc = filings["primaryDocument"][i]
            break

    # fetch the filing document
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{primary_doc}"
    r = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(r.text, "lxml")

    # find Item 1a section
    text = soup.get_text(separator=" ")
    start = text.lower().find("item 1a")
    end   = text.lower().find("item 1b", start)
    risk_text = text[start:end] if start != -1 else text[:5000]

    with open(cache_path, "w") as f:
        f.write(risk_text)

    return risk_text