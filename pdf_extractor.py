import os

def extract_pdf(pdf_path: str) -> dict:
    # extract sustainability report PDF into sections.
    # returns dict of {section_name: text_content}.
    if not os.path.exists(pdf_path):
        print(f"[PDF] File not found: {pdf_path}")
        return {}

    # try layoutParser first - ref layoutparser  -- add into esg file
    try:
        return _extract_layoutparser(pdf_path)
    except Exception as e:
        print(f"[PDF] LayoutParser failed ({e}), falling back to pdfplumber")
        return _extract_pdfplumber(pdf_path)

def _extract_layoutparser(pdf_path: str) -> dict:
    import layoutparser as lp
    import pdf2image

    images = pdf2image.convert_from_path(pdf_path, dpi=150)
    model  = lp.PaddleDetectionLayoutModel(
        "lp://PaddleDetection/PaddleFPN_r50_FPN_3x_coco",
        threshold=0.5,
        label_map={0: "Text", 1: "Title", 2: "List", 3: "Table", 4: "Figure"}
    )

    full_text = []
    for image in images:
        layout = model.detect(image)
        for block in layout:
            if block.type in ("Text", "List"):
                full_text.append(block.text or "")

    return _split_into_sections("\n".join(full_text))

def _extract_pdfplumber(pdf_path: str) -> dict:
    import pdfplumber

    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)

    return _split_into_sections("\n".join(pages))

# section header keywords to detect
SECTION_KEYWORDS = [
    "climate", "emissions", "carbon", "greenhouse",
    "water", "biodiversity", "waste",
    "health and safety", "employees", "diversity",
    "governance", "ethics", "supply chain",
    "targets", "commitments", "performance",
] # add more

def _split_into_sections(text: str) -> dict:
    
    # heuristically split text into sections by keyword headings.
    # returns {section_name: text_block}.
    
    lines  = text.split("\n")
    sections = {"full_text": text}
    current_section = "preamble"
    current_lines   = []

    for line in lines:
        line_lower = line.lower().strip()
        matched = next(
            (kw for kw in SECTION_KEYWORDS if kw in line_lower and len(line.strip()) < 80),
            None
        )
        if matched:
            if current_lines:
                sections[current_section] = "\n".join(current_lines)
            current_section = matched
            current_lines   = []
        else:
            current_lines.append(line)

    if current_lines:
        sections[current_section] = "\n".join(current_lines)

    return sections