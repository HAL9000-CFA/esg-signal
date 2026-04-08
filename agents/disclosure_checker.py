"""
Stream 1: Disclosure Quality Checker + Drift Detector
Part of Agent 3

Takes Agent 1 output and Agent 2 material factors, grades each ESG factor
as QUANTIFIED, VAGUE or UNDISCLOSED using Claude, then checks for drift year on year

Run normally:
    python stream1_disclosure_checker.py --ticker BP
"""

import argparse
import json
from datetime import datetime
from textwrap import dedent

from pipeline.llm_client import call_claude

# model config
MODEL_NAME = "claude-opus-4-5"
MODEL_VERSION = "claude-opus-4-5"
TEMPERATURE = 0.0

AGENT = "disclosure_quality"
PURPOSE = "Grade ESG factors as QUANTIFIED or VAGUE or UNDISCLOSED, analysing drift over years."

SYSTEM_PROMPT = """
You are an ESG disclosure quality analyst.

Your role is to evaluate whether a sustainability report adequately discloses a specific ESG factor.

You will be given:
- A sustainability report (unstructured text)
- A single ESG factor
- A company name

Your task:
Classify the ESG factor disclosure into EXACTLY one of the following categories:

1. QUANTIFIED
   - The factor is clearly mentioned
   - Includes specific numerical data, metrics, KPIs, or measurable targets
   - Examples: percentages, tonnage, dollar values, time-bound targets

2. VAGUE
   - The factor is mentioned
   - BUT only described qualitatively (no concrete numbers or measurable targets)

3. UNDISCLOSED
   - The factor is not mentioned at all
   - OR only appears in an irrelevant or non-substantive way

Strict rules:
- Use ONLY the provided report text
- Do NOT infer or assume missing information
- Do NOT upgrade vague statements to quantified
- A single number anywhere in a relevant context qualifies as QUANTIFIED
- If unsure between VAGUE and UNDISCLOSED, choose UNDISCLOSED
- Be conservative and precise

Evidence rules:
- If QUANTIFIED or VAGUE include a direct quote (preferred) or close paraphrase
- If UNDISCLOSED evidence must be null

Output requirements:
- Return VALID JSON only
- No markdown, no explanations, no extra text
- Follow the schema EXACTLY

Output format:

{
  "factor": "<factor>",
  "grade": "QUANTIFIED" | "VAGUE" | "UNDISCLOSED",
  "evidence": "<exact quote>" | null
}
"""


def grade_single_factor(
    report_text: str,
    factor: str,
    company: str,
) -> dict:
    """
    Grades a single ESG factor as QUANTIFIED, VAGUE or UNDISCLOSED
    Uses Claude API or loads from cache

    Args:
        report_text: sustainability report text from Agent 1
        factor: ESG factor to check e.g. "greenhouse gas emissions"
        company: company name
    """

    prompt = dedent(
        f"""
        Company: {company}
        ESG factor: {factor}

        Report:
        {report_text}
    """
    )

    response = call_claude(
        agent=AGENT,
        model=MODEL_NAME,
        version=MODEL_VERSION,
        purpose=PURPOSE,
        system=SYSTEM_PROMPT,
        max_tokens=500,
        temperature=TEMPERATURE,
        prompt=prompt,
        # run_id=...
    )

    # parse response - strip markdown backticks if claude wraps it
    response = response.replace("```json", "").replace("```", "").strip()
    result = json.loads(response)

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


def extract_report_text_from_data_gatherer(data_gatherer_output: dict) -> str:
    """
    Pulls sustainability report text out of the Agent 1 data gatherer output dict
    Combines layout parser sections and edgar risk factors into one string

    Args:
        data_gatherer_output: the dict returned by DataGatherer.fetch_all()
    """
    text_parts = []

    # pull from layout parser sections (sustainability report pdf)
    layout_data = data_gatherer_output.get("sources", {}).get("layout_parser", {})

    if layout_data.get("status") == "success":
        sections = layout_data.get("data", {}).get("sections", {})
        for section_name, section_data in sections.items():
            content = section_data.get("content", "")
            if content:
                text_parts.append(f"{section_name}:\n{content}")

    # pull from edgar risk factors
    edgar_data = data_gatherer_output.get("sources", {}).get("edgar", {})
    if edgar_data.get("status") == "success":
        risk_factors = edgar_data.get("data", {}).get("risk_factors", "")
        if risk_factors:
            text_parts.append(f"risk factors:\n{risk_factors}")

    return "\n\n".join(text_parts)


def run_disclosure_checker(
    company: str,
    current_data_gatherer_output: dict,
    previous_data_gatherer_output: dict,
    material_factors: list,
) -> dict:
    """
    Main function - ties everything together
    Call this from the pipeline with real Agent 1 and Agent 2 outputs

    Args:
        company: company name as a string
        current_data_gatherer_output: this years Agent 1 output dict
        previous_data_gatherer_output: last years Agent 1 output dict
        material_factors: list of ESG factors from Agent 2

    Returns a dict with grades and drift flags ready for the credibility aggregator
    """

    print(f"\nRunning Stream 1: Disclosure Quality Checker for {company}")

    # pull report text from agent 1 outputs
    current_report_text = extract_report_text_from_data_gatherer(current_data_gatherer_output)
    previous_report_text = extract_report_text_from_data_gatherer(previous_data_gatherer_output)

    print(f"Checking {len(material_factors)} material ESG factors...\n")

    # grade each factor for this years report
    current_grades = []
    for factor in material_factors:
        print(f"  Checking: {factor}")
        grade = grade_single_factor(current_report_text, factor, company)
        current_grades.append(grade)

    # grade each factor for last years report
    previous_grades = []
    for factor in material_factors:
        grade = grade_single_factor(previous_report_text, factor, f"{company}_prev")
        previous_grades.append(grade)

    # compare the two years and flag anything that changed
    drift_flags = detect_drift(current_grades, previous_grades)

    output = {
        "company": company,
        "stream": AGENT,
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
        current_data_gatherer_output=test_agent1_current,
        previous_data_gatherer_output=test_agent1_previous,
        material_factors=test_material_factors,
    )

    print("\nFull output:")
    print(json.dumps(result, indent=2))
