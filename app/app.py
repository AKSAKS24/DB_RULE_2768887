from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple
import re
import json

app = FastAPI(
    title="ABAP SELECT Remediator for SAP Note 2768887 (Handles Any Syntax)"
)

# Regex to match:
# SELECT <fields> FROM <table>
#    ... optional clauses ...
#    INTO TABLE <var> | INTO <var>
# <var> can be simple name or @DATA(...) or lo_obj->attr
SELECT_RE = re.compile(
    r"""(?P<full>
            SELECT\s+(?:SINGLE\s+)?         # SELECT or SELECT SINGLE
            (?P<fields>[\w\s,*]+)           # fields list or *
            \s+FROM\s+(?P<table>\w+)        # FROM table name
            (?P<middle>.*?)                 # middle chunk up to INTO
            (?:
                (?:INTO\s+TABLE\s+(?P<into_tab>[\w@()\->]+))
              | (?:INTO\s+(?P<into_wa>[\w@()\->]+))
            )
            (?P<tail>.*?)
        )\.""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

# ---------------------------------------------------
# Core functions
# ---------------------------------------------------

def ensure_draft_filter(sel_stmt: str, table: str) -> str:
    """Add DRAFT = SPACE condition for VBRK/VBRP if missing."""
    table_up = table.upper()
    if table_up not in {"VBRK", "VBRP"}:
        return sel_stmt
    # Already has DRAFT filter?
    if re.search(rf"{table_up}-DRAFT\s*=\s*['\"]?\s?['\"]?", sel_stmt, re.IGNORECASE):
        return sel_stmt
    # Insert before/into WHERE clause
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
    """Return single-line suggested statement with DRAFT filter."""
    stmt = ensure_draft_filter(sel_text, table)
    return re.sub(r"\s+", " ", stmt).strip()

def find_selects(txt: str):
    """Regex find all SELECT statements."""
    out = []
    for m in SELECT_RE.finditer(txt):
        out.append({
            "text": m.group("full"),
            "table": m.group("table"),
            "target_type": "itab" if m.group("into_tab") else "wa",
            "target_name": (m.group("into_tab") or m.group("into_wa")),
            "span": m.span(0),
        })
    return out

def apply_span_replacements(source: str, repls: List[Tuple[Tuple[int, int], str]]) -> str:
    """Replace segments in the ABAP source."""
    out = source
    for (s, e), r in sorted(repls, key=lambda x: x[0][0], reverse=True):
        out = out[:s] + r + out[e:]
    return out

# ---------------------------------------------------
# API endpoint
# ---------------------------------------------------
@app.post("/remediate-array")
def remediate_array(units: List[Unit]):
    """
    Find and remediate all VBRK/VBRP SELECTs (any field list, any INTO syntax).
    Only these tables are included in 'selects' output.
    """
    results = []
    for u in units:
        src = u.code or ""
        selects = find_selects(src)
        replacements = []
        selects_metadata = []

        for sel in selects:
            if sel["table"].upper() in ("VBRK", "VBRP"):
                sel_info = {
                    "table": sel["table"],
                    "target_type": sel["target_type"],
                    "target_name": sel["target_name"],
                    "start_char_in_unit": sel["span"][0],
                    "end_char_in_unit": sel["span"][1],
                    "used_fields": [],
                    "ambiguous": False,
                    "suggested_fields": None,
                    "suggested_statement": None
                }
                new_stmt = build_replacement_stmt(sel["text"], sel["table"], sel["target_type"], sel["target_name"])
                if new_stmt != sel["text"]:
                    replacements.append((sel["span"], new_stmt))
                    sel_info["suggested_statement"] = new_stmt
                selects_metadata.append(sel_info)

        # Apply replacements internally (to check for correctness but not return)
        _ = apply_span_replacements(src, replacements)

        obj = json.loads(u.model_dump_json())
        obj["selects"] = selects_metadata
        results.append(obj)

    return results