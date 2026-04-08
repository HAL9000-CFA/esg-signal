from .indeed_scraper import fetch_indeed_jobs
from .serpapi_scraper import fetch_google_jobs

def fetch_job_postings(company: str, keywords: list) -> list:
    results = []
    
    # primary: indeed
    try:
        results.extend(fetch_indeed_jobs(company, keywords))
    except Exception as e:
        print(f"[Indeed] Failed: {e}")
    
    # fallback: serp api
    if not results:
        try:
            for kw in keywords[:3]:  # limit - no avoid burning credits
                results.extend(fetch_google_jobs(company, kw))
        except Exception as e:
            print(f"[SerpAPI] Failed: {e}")
    
    return results