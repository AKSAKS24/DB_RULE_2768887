from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Optional, Tuple, Set
import re
import json

app = FastAPI(title="ABAP SELECT* Remediator for SAP Note 2768887")

# ABAP SELECT * Regex, as in original
SELECT_STAR_RE = re.compile(
    r"""(?P<full>SELECT\s+(?:SINGLE\s+)?\*\s+FROM\s+(?P<table>\w+)
        (?P<middle>.*?)
        (?:(?:INTO\s+TABLE\s+(?P<into_tab>\w+))|(?:INTO\s+(?P<into_wa>\w+)))
        (?P<tail>.*?))\.""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

# For code input/output structure
class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

def ensure_draft_filter(sel_stmt: str, table: str) -> str:
    """Ensure that the SELECT statement for VBRK/VBRP includes a DRAFT = SPACE filter."""
    table_up = table.upper()
    if table_up not in {"VBRK", "VBRP"}:
        return sel_stmt
    # Already filtered?
    if re.search(rf"{table_up}-DRAFT\s*=\s*['\"]? ?['\"]?", sel_stmt, re.IGNORECASE):
        return sel_stmt
    # If WHERE exists, add AND
    where_match = re.search(r"\bWHERE\b", sel_stmt, re.IGNORECASE)
    if where_match:
        # Insert after WHERE (and possible opening parens, and spaces)
        start = where_match.end()
        new_stmt = sel_stmt[:start] + f" {table_up}-DRAFT = SPACE AND" + sel_stmt[start:]
        return new_stmt
    else:
        # Insert WHERE clause before INTO
        m = re.search(r"\bINTO\b", sel_stmt, re.IGNORECASE)
        if m:
            return sel_stmt[:m.start()] + f" WHERE {table_up}-DRAFT = SPACE " + sel_stmt[m.start():]
        else:
            return sel_stmt.rstrip(".") + f" WHERE {table_up}-DRAFT = SPACE."
        
def build_replacement_stmt(sel_text: str, table: str, target_type: str, target_name: str) -> str:
    """If remediating VBRK/VBRP SELECT, inject DRAFT filter."""
    # Use original fields logic (keep "*") for simplicity/compatibility
    stmt = sel_text
    stmt = ensure_draft_filter(stmt, table)
    return stmt

def find_selects(txt: str):
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

def apply_span_replacements(source: str, repls: List[Tuple[Tuple[int,int], str]]) -> str:
    out = source
    for (s,e), r in sorted(repls, key=lambda x: x[0][0], reverse=True):
        out = out[:s] + r + out[e:]
    return out

def concat_units(units: List[Unit]) -> str:
    return "".join((u.code or "") + "\n" for u in units)

# --------- API ENDPOINT --------
@app.post("/remediate-array")
def remediate_array(units: List[Unit]):
    """Finds and remediates SELECT * from VBRK/VBRP in the provided code units."""
    results = []
    for u in units:
        src = u.code or ""
        selects = find_selects(src)
        replacements = []
        for sel in selects:
            # Only remediate SELECTs for VBRK or VBRP, as per SAP note
            if sel["table"].upper() in ("VBRK", "VBRP"):
                new_stmt = build_replacement_stmt(sel["text"], sel["table"], sel["target_type"], sel["target_name"])
                if new_stmt != sel["text"]:
                    replacements.append((sel["span"], new_stmt))
        remediated = apply_span_replacements(src, replacements)
        obj = json.loads(u.model_dump_json())
        obj["remediated_code"] = remediated
        results.append(obj)
    return results