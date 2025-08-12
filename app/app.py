from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple
import re
import json

app = FastAPI(
    title="ABAP SELECT* Remediator for SAP Note 2768887 (Clean Output, No remediated_code)"
)

# Regex to match SELECT * FROM ...
SELECT_STAR_RE = re.compile(
    r"""(?P<full>SELECT\s+(?:SINGLE\s+)?\*\s+FROM\s+(?P<table>\w+)
        (?P<middle>.*?)
        (?:(?:INTO\s+TABLE\s+(?P<into_tab>\w+))|(?:INTO\s+(?P<into_wa>\w+)))
        (?P<tail>.*?))\.""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

# Request/Response model
class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""


# ---------------- Core Functions ----------------

def ensure_draft_filter(sel_stmt: str, table: str) -> str:
    """Ensure SELECT for VBRK/VBRP includes DRAFT = SPACE filter."""
    table_up = table.upper()
    if table_up not in {"VBRK", "VBRP"}:
        return sel_stmt
    # Check if filter already present
    if re.search(rf"{table_up}-DRAFT\s*=\s*['\"]? ?['\"]?", sel_stmt, re.IGNORECASE):
        return sel_stmt
    # Add filter
    where_match = re.search(r"\bWHERE\b", sel_stmt, re.IGNORECASE)
    if where_match:
        start = where_match.end()
        return sel_stmt[:start] + f" {table_up}-DRAFT = SPACE AND" + sel_stmt[start:]
    else:
        m = re.search(r"\bINTO\b", sel_stmt, re.IGNORECASE)
        if m:
            return sel_stmt[:m.start()] + f" WHERE {table_up}-DRAFT = SPACE " + sel_stmt[m.start():]
        else:
            return sel_stmt.rstrip(".") + f" WHERE {table_up}-DRAFT = SPACE."


def build_replacement_stmt(sel_text: str, table: str, target_type: str, target_name: str) -> str:
    """Build the updated SELECT with DRAFT filter — ensure single line and spaces only."""
    stmt = ensure_draft_filter(sel_text, table)
    # Remove excessive whitespace/newlines, keep single spaces
    stmt = re.sub(r"\s+", " ", stmt).strip()
    return stmt


def find_selects(txt: str):
    """Find all SELECT * statements in code."""
    out = []
    for m in SELECT_STAR_RE.finditer(txt):
        out.append({
            "text": m.group("full"),
            "table": m.group("table"),
            "target_type": "itab" if m.group("into_tab") else "wa",
            "target_name": (m.group("into_tab") or m.group("into_wa")),
            "span": m.span(0),
        })
    return out


def apply_span_replacements(source: str, repls: List[Tuple[Tuple[int, int], str]]) -> str:
    """Replace code segments based on character spans."""
    out = source
    for (s, e), r in sorted(repls, key=lambda x: x[0][0], reverse=True):
        out = out[:s] + r + out[e:]
    return out


# ---------------- API Endpoint ----------------

@app.post("/remediate-array")
def remediate_array(units: List[Unit]):
    """Find & remediate SELECT * from VBRK/VBRP — return clean suggested statements without remediated_code."""
    results = []
    for u in units:
        src = u.code or ""
        selects = find_selects(src)
        replacements = []
        selects_metadata = []

        for sel in selects:
            sel_info = {
                "table": sel["table"],
                "target_type": sel["target_type"],
                "target_name": sel["target_name"],
                "start_char_in_unit": sel["span"][0],
                "end_char_in_unit": sel["span"][1],
                "used_fields": [],           # Not scanning fields in this simple example
                "ambiguous": False,          # Always false here, no complex detection
                "suggested_fields": None,
                "suggested_statement": None
            }

            if sel["table"].upper() in ("VBRK", "VBRP"):
                new_stmt = build_replacement_stmt(
                    sel["text"], sel["table"], sel["target_type"], sel["target_name"]
                )
                if new_stmt != sel["text"]:
                    replacements.append((sel["span"], new_stmt))
                    sel_info["suggested_statement"] = new_stmt

            selects_metadata.append(sel_info)

        # Apply replacement internally (not returned)
        _ = apply_span_replacements(src, replacements)

        obj = json.loads(u.model_dump_json())
        obj["selects"] = selects_metadata
        results.append(obj)

    return results