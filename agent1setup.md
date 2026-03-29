# Agent 1 - Setup Guide

## Before you start you need:
- Python 3.8+
- A real email address (SEC needs this)
- Companies House API key, only needed for FTSE 100 companies

---

## 1. Go into the agent 1 folder
```
cd agent1
```

## 2. Set up a virtual environment
```
python -m venv venv
```

Windows:
```
venv\Scripts\activate
```

Mac/Linux:
```
source venv/bin/activate
```

## 3. Install everything
```
pip install -r requirements.txt
```

## 4. Credentials

```
cp .env.example .env
```

Open the .env file and fill these in:
```
SEC_EMAIL=(Your Email)
COMPANIES_HOUSE_KEY=(Your API key)
```

For the Companies House key go to developer.company-information.service.gov.uk, make a free account and grab the key from there.

For SEC you dont need to register, just use a real email or it will block you.

## 5. Run it

US company:
```
python agent1_cli.py --ticker AAPL --(Your Email)
```

UK company:
```
python agent1_cli.py --ticker BP --company "BP PLC" --(Your Email)--companies-house-key (Your API key)
```

With a PDF:
```
python agent1_cli.py --ticker BP --company "BP PLC" --pdf bp_sustainability_2023.pdf --(Your Email)
```

## 6. Output

Results get saved to the /output folder as a JSON file. Depending on the result in the terminal you'll see :
- worked
- partial
- failed

---

## Tests
```
python test_agent1.py
```

---

## Things that might go wrong

**403 error from SEC** - use a real email address, they block fake ones

**Companies House auth failed** - check the API key in your .env has no extra spaces

**PDF extraction failing** - run `pip install PyPDF2`

**pip not recognised** - venv isnt activated, run `venv\Scripts\activate` first

---


