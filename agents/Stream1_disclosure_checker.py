"""
Stream 1: Disclosure Quality Checker + Drift Detector
Part of Agent 3

Takes Agent 1 output and Agent 2 material factors, grades each ESG factor
as QUANTIFIED, VAGUE or UNDISCLOSED using Claude, then checks for drift year on year

Run normally:
    python stream1_disclosure_checker.py --ticker BP

Run with cached responses (no API calls):
    python stream1_disclosure_checker.py --ticker BP --use-cached
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

# cache folder - responses get saved here automatically
# judges can run with --use-cached to reproduce results for free (rule 4.4)
CACHE_DIR = Path("cache/stream1")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# audit log - every claude call gets logged here (rule 4.5)
AUDIT_LOG_PATH = Path("logs/audit_log.jsonl")
AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# model config
MODEL_NAME = "claude-opus-4-5"
MODEL_VERSION = "claude-opus-4-5"
TEMPERATURE = 0


def log_api_call(call_type: str, input_tokens: int, output_tokens: int):
    """
    Logs every Claude API call with model, version, tokens and cost estimate
    Required by Rule 4.5
    """
    # claude opus 4.5 pricing: $15 per 1M input, $75 per 1M output
    input_cost = (input_tokens / 1_000_000) * 15
    output_cost = (output_tokens / 1_000_000) * 75
    total_cost = input_cost + output_cost

    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "model": MODEL_NAME,
        "version": MODEL_VERSION,
        "temperature": TEMPERATURE,
        "call_type": call_type,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "estimated_cost_usd": round(total_cost, 6),
    }

    with open(AUDIT_LOG_PATH, "a") as f:
        f.write(json.dumps(log_entry) + "\n")

    print(f"  [audit] tokens: {input_tokens} in / {output_tokens} out | cost: ${total_cost:.4f}")


def grade_single_factor(
    report_text: str,
    factor: str,
    company: str,
    client: anthropic.Anthropic,
    use_cached: bool = False,
) -> dict:
    """
    Grades a single ESG factor as QUANTIFIED, VAGUE or UNDISCLOSED
    Uses Claude API or loads from cache depending on --use-cached flag

    Args:
        report_text: sustainability report text from Agent 1
        factor: ESG factor to check e.g. "greenhouse gas emissions"
        company: company name
        client: anthropic client (None if use_cached)
        use_cached: if True loads from cache instead of calling API
    """

    # one cache file per company + factor
    cache_key = f"{company}_{factor}".lower().replace(" ", "_")
    cache_file = CACHE_DIR / f"{cache_key}.json"

    # load from cache if flag is set
    if use_cached:
        if cache_file.exists():
            with open(cache_file) as f:
                print(f"  [cache] loaded: {factor}")
                return json.load(f)
        else:
            print(f"  [cache] no cache found for {factor} - returning placeholder")
            return {
                "factor": factor,
                "grade": "UNDISCLOSED",
                "evidence": "No cached response available - run without --use-cached first",
            }

    # otherwise make the real claude api call
    prompt = f"""You are analysing a sustainability report for {company}.

Here is the sustainability report text:
{report_text}

ESG factor to check: {factor}

Grade this factor using ONLY these three labels:
- QUANTIFIED: the report mentions this factor AND gives specific numbers or measurable targets
- VAGUE: the report mentions this factor but only in general language with no numbers
- UNDISCLOSED: the report does not mention this factor at all

Return your answer as JSON only, no other text, in this exact format:
{{
  "factor": "{factor}",
  "grade": "QUANTIFIED or VAGUE or UNDISCLOSED",
  "evidence": "the exact quote from the report supporting your grade, or null if undisclosed"
}}"""

    response = client.messages.create(
        model=MODEL_NAME,
        max_tokens=500,
        temperature=TEMPERATURE,
        messages=[{"role": "user", "content": prompt}],
    )

    # log the call
    log_api_call(
        call_type=f"grade_factor:{factor}",
        input_tokens=response.usage.input_tokens,
        output_tokens=response.usage.output_tokens,
    )

    # parse response - strip markdown backticks if claude wraps it
    response_text = response.content[0].text.strip()
    response_text = response_text.replace("```json", "").replace("```", "").strip()
    result = json.loads(response_text)

    # save to cache so judges can use --use-cached
    with open(cache_file, "w") as f:
        json.dump(result, f, indent=2)

    return result


def detect_drift(current_grades: list, previous_grades: list) -> list:
    """
    Compares this years grades to last years grades
    Flags any factor where the grade has changed - no claude needed, pure python

    Args:
        current_grades: list of grade dicts for current year
        previous_grades: list of grade dicts for previous year
    """

    # turn last years grades into a lookup dict
    previous_lookup = {item["factor"]: item["grade"] for item in previous_grades}
    drift_flags = []

    for item in current_grades:
        factor = item["factor"]
        current_grade = item["grade"]
        previous_grade = previous_lookup.get(factor)

        # flag it if the grade changed between years
        if previous_grade and previous_grade != current_grade:
            drift_flags.append(
                {
                    "factor": factor,
                    "previous_grade": previous_grade,
                    "current_grade": current_grade,
                    "drift_detected": True,
                    "note": f"Changed from {previous_grade} to {current_grade} year on year",
                }
            )

    return drift_flags


def extract_report_text_from_agent1(agent1_output: dict) -> str:
    """
    Pulls sustainability report text out of the Agent 1 output dict
    Combines layout parser sections and edgar risk factors into one string

    Args:
        agent1_output: the dict returned by DataGatherer.fetch_all()
    """
    text_parts = []

    # pull from layout parser sections (sustainability report pdf)
    layout_data = agent1_output.get("sources", {}).get("layout_parser", {})
    if layout_data.get("status") == "success":
        sections = layout_data.get("data", {}).get("sections", {})
        for section_name, section_data in sections.items():
            content = section_data.get("content", "")
            if content:
                text_parts.append(f"{section_name}:\n{content}")

    # pull from edgar risk factors
    edgar_data = agent1_output.get("sources", {}).get("edgar", {})
    if edgar_data.get("status") == "success":
        risk_factors = edgar_data.get("data", {}).get("risk_factors", "")
        if risk_factors:
            text_parts.append(f"risk factors:\n{risk_factors}")

    return "\n\n".join(text_parts)


def run_disclosure_checker(
    company: str,
    current_agent1_output: dict,
    previous_agent1_output: dict,
    material_factors: list,
    use_cached: bool = False,
) -> dict:
    """
    Main function - ties everything together
    Call this from the pipeline with real Agent 1 and Agent 2 outputs

    Args:
        company: company name as a string
        current_agent1_output: this years Agent 1 output dict
        previous_agent1_output: last years Agent 1 output dict
        material_factors: list of ESG factors from Agent 2
        use_cached: if True loads cached claude responses instead of calling API

    Returns a dict with grades and drift flags ready for the credibility aggregator
    """

    print(f"\nRunning Stream 1: Disclosure Quality Checker for {company}")
    if use_cached:
        print("Mode: CACHED (no API calls)\n")
    else:
        print("Mode: LIVE (Claude API)\n")

    # set up claude client - only needed if not using cache
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY")) if not use_cached else None

    # pull report text from agent 1 outputs
    current_report_text = extract_report_text_from_agent1(current_agent1_output)
    previous_report_text = extract_report_text_from_agent1(previous_agent1_output)

    print(f"Checking {len(material_factors)} material ESG factors...\n")

    # grade each factor for this years report
    current_grades = []
    for factor in material_factors:
        print(f"  Checking: {factor}")
        grade = grade_single_factor(current_report_text, factor, company, client, use_cached)
        current_grades.append(grade)

    # grade each factor for last years report
    previous_grades = []
    for factor in material_factors:
        grade = grade_single_factor(
            previous_report_text, factor, f"{company}_prev", client, use_cached
        )
        previous_grades.append(grade)

    # compare the two years and flag anything that changed
    drift_flags = detect_drift(current_grades, previous_grades)

    output = {
        "company": company,
        "stream": "disclosure_quality",
        "timestamp": datetime.now().isoformat(),
        "grades": current_grades,
        "drift_flags": drift_flags,
        "summary": {
            "quantified": len([g for g in current_grades if g["grade"] == "QUANTIFIED"]),
            "vague": len([g for g in current_grades if g["grade"] == "VAGUE"]),
            "undisclosed": len([g for g in current_grades if g["grade"] == "UNDISCLOSED"]),
            "drift_detected": len(drift_flags),
        },
    }

    print(f"\nDone. Summary: {output['summary']}")
    return output


# testing - replace these with real agent 1 and agent 2 outputs when pipeline is connected
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Stream 1: Disclosure Quality Checker")
    parser.add_argument("--ticker", default="BP", help="Company ticker")
    parser.add_argument(
        "--use-cached",
        action="store_true",
        help="Load cached Claude responses instead of calling API",
    )
    args = parser.parse_args()

    # placeholder agent 1 output - mimics DataGatherer.fetch_all() structure
    # replace with real agent 1 output when connected
    test_agent1_current = {
        "ticker": args.ticker,
        "company_name": "BP PLC",
        "sources": {
            "edgar": {
                "status": "success",
                "data": {
                    "risk_factors": """
                    BP faces significant climate-related risks. Greenhouse gas emissions
                    were reduced by 12% to 34.5 million tonnes CO2 equivalent in 2023.
                    Capital expenditure on low carbon energy reached $1.2 billion.
                    Employee health and safety remains a core operational priority.
                    """
                },
            },
            "layout_parser": {
                "status": "success",
                "data": {
                    "sections": {
                        "environmental": {
                            "title": "Environmental",
                            "content": "Water management programme continues across all sites. We reduced freshwater consumption by 8%.",
                        },
                        "emissions": {
                            "title": "Emissions",
                            "content": "Scope 1 and 2 emissions reduced to 34.5 million tonnes CO2e, a 12% reduction year on year.",
                        },
                    }
                },
            },
        },
    }

    test_agent1_previous = {
        "ticker": args.ticker,
        "company_name": "BP PLC",
        "sources": {
            "edgar": {
                "status": "success",
                "data": {
                    "risk_factors": """
                    BP is committed to net zero by 2050. Greenhouse gas emissions were
                    39.2 million tonnes CO2 equivalent. Low carbon investment was $800 million.
                    """
                },
            },
            "layout_parser": {
                "status": "success",
                "data": {
                    "sections": {
                        "environmental": {
                            "title": "Environmental",
                            "content": "Water management is an important part of our sustainability strategy.",
                        }
                    }
                },
            },
        },
    }

    # placeholder agent 2 output - replace with real agent 2 output when connected
    test_material_factors = [
        "greenhouse gas emissions",
        "water management",
        "employee health and safety",
        "low carbon investment",
    ]

    result = run_disclosure_checker(
        company="BP",
        current_agent1_output=test_agent1_current,
        previous_agent1_output=test_agent1_previous,
        material_factors=test_material_factors,
        use_cached=args.use_cached,
    )

    print("\nFull output:")
    print(json.dumps(result, indent=2))
