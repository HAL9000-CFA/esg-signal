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
from pipeline.models import CompanyProfile

# model config
MODEL_NAME = "claude-opus-4-5"
MODEL_VERSION = "claude-opus-4-5"
TEMPERATURE = 0.0

AGENT = "disclosure_quality"
PURPOSE = "Grade ESG factors as QUANTIFIED or VAGUE or UNDISCLOSED, analysing drift over years."

_SYSTEM_PROMPT_SINGLE = """
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

_SYSTEM_PROMPT_BATCH = """
You are an ESG disclosure quality analyst.

You will be given a sustainability report and a list of ESG factors to evaluate.
Grade EVERY factor in the list — do not skip any.

For each factor classify into EXACTLY one of:

1. QUANTIFIED — factor is mentioned with specific numerical data, metrics, KPIs, or measurable targets
2. VAGUE      — factor is mentioned but only qualitatively (no concrete numbers or measurable targets)
3. UNDISCLOSED — factor is not mentioned, or only appears in an irrelevant or non-substantive way

Strict rules:
- Use ONLY the provided report text
- Do NOT infer or assume missing information
- Do NOT upgrade vague statements to quantified
- A single number in a relevant context qualifies as QUANTIFIED
- If unsure between VAGUE and UNDISCLOSED, choose UNDISCLOSED

Evidence rules:
- If QUANTIFIED or VAGUE include a direct quote or close paraphrase
- If UNDISCLOSED set evidence to null

Output requirements:
- Return a JSON ARRAY only — one object per factor, in the same order as the input list
- No markdown, no explanations, no extra text

Output format:
[
  {"factor": "<factor_name>", "grade": "QUANTIFIED"|"VAGUE"|"UNDISCLOSED", "evidence": "<quote>"|null},
  ...
]
"""


def grade_all_factors(
    report_text: str,
    factors: list,
    company: str,
    run_id: str = None,
) -> list:
    """
    Grade all ESG factors in a single Claude call.

    Batching all factors into one request avoids sending the full report text
    once per factor, which saves (N-1) * report_tokens input tokens per run.
    Use this in preference to calling grade_single_factor() in a loop.

    Args:
        report_text: annual report text from CompanyProfile
        factors: list of factor name strings or MaterialFactor objects
                 (name attribute is used if objects are passed)
        company: company name
        run_id: Airflow run ID for audit log grouping (optional)

    Returns:
        List of dicts: [{"factor": str, "grade": str, "evidence": str|None}, ...]
        Length and order match the input factors list.
    """
    factor_names = [f.name if hasattr(f, "name") else str(f) for f in factors]
    factors_list = "\n".join(f"- {name}" for name in factor_names)

    prompt = dedent(
        f"""
        Company: {company}

        ESG factors to grade:
        {factors_list}

        Report:
        {report_text}
        """
    )

    response = call_claude(
        agent=AGENT,
        model=MODEL_NAME,
        version=MODEL_VERSION,
        purpose=f"Batch grade {len(factor_names)} ESG factors",
        system=_SYSTEM_PROMPT_BATCH,
        max_tokens=150 * len(factor_names),  # ~150 tokens per factor
        temperature=TEMPERATURE,
        prompt=prompt,
        run_id=run_id,
    )

    response = response.replace("```json", "").replace("```", "").strip()
    results = json.loads(response)

    # Guarantee one result per input factor, in order, even if Claude drops one
    result_map = {r["factor"]: r for r in results}
    return [
        result_map.get(name, {"factor": name, "grade": "UNDISCLOSED", "evidence": None})
        for name in factor_names
    ]


_CHUNK_SIZE = 20_000   # characters per chunk
_CHUNK_OVERLAP = 2_000  # overlap between consecutive chunks

# Grade priority: QUANTIFIED > VAGUE > UNDISCLOSED
_GRADE_PRIORITY = {"QUANTIFIED": 2, "VAGUE": 1, "UNDISCLOSED": 0}


def _chunk_text(text: str) -> list[str]:
    """
    Splits text into overlapping chunks of _CHUNK_SIZE chars with _CHUNK_OVERLAP
    chars of overlap between consecutive chunks.  Returns a list with at least
    one element (the full text when it is already within the size limit).
    """
    if len(text) <= _CHUNK_SIZE:
        return [text]
    step = _CHUNK_SIZE - _CHUNK_OVERLAP
    return [text[i : i + _CHUNK_SIZE] for i in range(0, len(text), step) if text[i : i + _CHUNK_SIZE]]


def _merge_grades(grades: list[dict]) -> dict:
    """
    Merges per-chunk grade dicts using QUANTIFIED > VAGUE > UNDISCLOSED priority.
    The evidence from the highest-priority result is kept.
    """
    best = max(grades, key=lambda g: _GRADE_PRIORITY.get(g.get("grade", "UNDISCLOSED"), 0))
    return best


def grade_single_factor(
    report_text: str,
    factor: str,
    company: str,
    run_id: str = None,
) -> dict:
    """
    Grades a single ESG factor as QUANTIFIED, VAGUE or UNDISCLOSED.

    When report_text exceeds 20,000 characters it is split into overlapping
    chunks of 20,000 chars (2,000 char overlap) and each chunk is graded
    independently.  The highest-priority grade across all chunks is returned:
    QUANTIFIED > VAGUE > UNDISCLOSED.

    Prefer grade_all_factors() when evaluating multiple factors — it sends the
    report text only once, saving (N-1) * report_tokens input tokens per run.

    Args:
        report_text: sustainability report text from Agent 1
        factor: ESG factor to check e.g. "greenhouse gas emissions"
        company: company name
        run_id: Airflow run ID for audit log grouping (optional)
    """
    chunks = _chunk_text(report_text)
    chunk_results = []
    for idx, chunk in enumerate(chunks):
        prompt = dedent(
            f"""
            Company: {company}
            ESG factor: {factor}

            Report:
            {chunk}
        """
        )

        response = call_claude(
            agent=AGENT,
            model=MODEL_NAME,
            version=MODEL_VERSION,
            purpose=PURPOSE,
            system=_SYSTEM_PROMPT_SINGLE,
            max_tokens=500,
            temperature=TEMPERATURE,
            prompt=prompt,
            run_id=run_id,
        )

        response = response.replace("```json", "").replace("```", "").strip()
        result = json.loads(response)
        chunk_results.append(result)

        # Short-circuit: no need to check remaining chunks once QUANTIFIED is found
        if result.get("grade") == "QUANTIFIED":
            break

    return _merge_grades(chunk_results)


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


def extract_report_text(profile: CompanyProfile) -> str:
    """
    Returns the annual report text from a CompanyProfile for ESG factor grading.
    Layout parser sections (PDF) will be appended here once issue #8 is complete.

    Args:
        profile: CompanyProfile produced by DataGatherer.fetch_company_profile()
    """
    return profile.annual_report_text or ""


def run_disclosure_checker(
    company: str,
    current_profile: CompanyProfile,
    material_factors: list,
    previous_profile: CompanyProfile = None,
    run_id: str = None,
) -> dict:
    """
    Main entry point for Stream 1: Disclosure Quality Checker.

    Grades all material ESG factors in a single Claude call (batched) and
    optionally detects year-on-year drift if a previous_profile is provided.

    Args:
        company: company name as a string
        current_profile: CompanyProfile for the current year from DataGatherer
        material_factors: List[MaterialFactor] from RelevanceFilter, or List[str]
        previous_profile: CompanyProfile for the prior year (optional — drift
                          detection is skipped when not provided)
        run_id: Airflow run ID for audit log grouping (optional)

    Returns a dict with grades and drift flags ready for the credibility scorer.
    """
    current_report_text = extract_report_text(current_profile)

    # Single Claude call grades all factors at once
    current_grades = grade_all_factors(current_report_text, material_factors, company, run_id)

    drift_flags = []
    if previous_profile is not None:
        previous_report_text = extract_report_text(previous_profile)
        previous_grades = grade_all_factors(
            previous_report_text, material_factors, f"{company}_prev", run_id
        )
        drift_flags = detect_drift(current_grades, previous_grades)

    return {
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


# testing — replace with real DataGatherer.fetch_company_profile() calls when pipeline is wired
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Stream 1: Disclosure Quality Checker")
    parser.add_argument("--ticker", default="BP", help="Company ticker")

    args = parser.parse_args()

    # placeholder profiles — replace with DataGatherer.fetch_company_profile() calls
    test_profile_current = CompanyProfile(
        ticker=args.ticker,
        name="BP PLC",
        index="FTSE100",
        sic_code="11100",
        sic_description=None,
        country="GB",
        latest_annual_filing=None,
        annual_report_text=(
            "BP faces significant climate-related risks. Greenhouse gas emissions "
            "were reduced by 12% to 34.5 million tonnes CO2 equivalent in 2023. "
            "Capital expenditure on low carbon energy reached $1.2 billion. "
            "Employee health and safety remains a core operational priority. "
            "Water management programme continues across all sites. "
            "We reduced freshwater consumption by 8%. "
            "Scope 1 and 2 emissions reduced to 34.5 million tonnes CO2e, a 12% reduction year on year."
        ),
        raw_financials={},
        source_urls=[],
        errors=[],
    )

    test_profile_previous = CompanyProfile(
        ticker=args.ticker,
        name="BP PLC",
        index="FTSE100",
        sic_code="11100",
        sic_description=None,
        country="GB",
        latest_annual_filing=None,
        annual_report_text=(
            "BP is committed to net zero by 2050. Greenhouse gas emissions were "
            "39.2 million tonnes CO2 equivalent. Low carbon investment was $800 million. "
            "Water management is an important part of our sustainability strategy."
        ),
        raw_financials={},
        source_urls=[],
        errors=[],
    )

    # placeholder agent 2 output — replace with real relevance_filter output when wired
    test_material_factors = [
        "greenhouse gas emissions",
        "water management",
        "employee health and safety",
        "low carbon investment",
    ]

    result = run_disclosure_checker(
        company="BP",
        current_profile=test_profile_current,
        material_factors=test_material_factors,
        previous_profile=test_profile_previous,
    )

    print("\nFull output:")
    print(json.dumps(result, indent=2))
