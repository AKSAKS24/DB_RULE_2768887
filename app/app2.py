from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple
import re
import json

app = FastAPI(
    title="ABAP SELECT Remediator for SAP Note 2768887 (Handles Any Syntax, including JOINS)"
)

# Improved regex: matches SELECT ... FROM ... [JOIN ...] ... INTO ...
SELECT_RE = re.compile(
    r"""(?P<full>
        SELECT\s+(?:SINGLE\s+)?                # SELECT or SELECT SINGLE
        (?P<fields>.+?)                        # everything up to FROM (fields)
        \s+FROM\s+(?P<from_clause>.*?)         # everything from FROM ... (stops at WHERE/INTO/ORDER/GROUP/HAVING/etc)
        (?=
            \s+(WHERE|INTO|ORDER|GROUP|HAVING|FOR\s+ALL\s+ENTRIES|$)
        )
        (?P<middle>.*?)
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

def extract_tables(from_clause: str) -> list:
    """
    Extract all tables (and aliases if present) used in FROM and all JOINs.
    Returns list of dicts with {table: 'VBRK', alias: 'A'}.
    """
    tables = []
    # Split main FROM and all JOINs
    join_parts = re.split(r'\bJOIN\b', from_clause, flags=re.IGNORECASE)
    tbl_alias_re = re.compile(r'(\w+)(?:\s+(?:AS\s+)?(\w+))?', re.IGNORECASE)
    for join_part in join_parts:
        join_part = re.split(r'\bON\b', join_part, flags=re.IGNORECASE)[0]
        candidates = join_part.split(',')
        for candidate in candidates:
            m = tbl_alias_re.match(candidate.strip())
            if m:
                tables.append({
                    "table": m.group(1).upper(),
                    "alias": (m.group(2) or m.group(1)).upper()
                })
    return tables

def find_selects(txt: str):
    """
    Yields any select statement involving VBRK/VBRP, including given aliases and join forms.
    """
    out = []
    for m in SELECT_RE.finditer(txt):
        from_clause = m.group("from_clause")
        tables = extract_tables(from_clause)
        is_vbrk_vbrp = [t for t in tables if t["table"] in ("VBRK", "VBRP")]
        if not is_vbrk_vbrp:
            continue

        out.append({
            "text": m.group("full"),
            "tables": tables,  # list of {table, alias}
            "from_clause": from_clause,
            "target_type": "itab" if m.group("into_tab") else "wa",
            "target_name": (m.group("into_tab") or m.group("into_wa")),
            "span": m.span(0),
        })
    return out

def ensure_draft_filter(sel_stmt: str, tables: list) -> str:
    """
    Adds the DRAFT=SPACE condition to all VBRK/VBRP tables in sel_stmt (using aliases if present).
    Will not duplicate if already present.
    """
    v_tables = [t for t in tables if t["table"] in ("VBRK", "VBRP")]
    if not v_tables:
        return sel_stmt
    # If any DRAFT already present for any relevant alias, skip
    already_filtered = False
    for t in v_tables:
        pat = rf"{t['alias']}-DRAFT\s*=\s*['\"]?\s?['\"]?"
        if re.search(pat, sel_stmt, re.IGNORECASE):
            already_filtered = True
            break
    if already_filtered:
        return sel_stmt

    # Prepare draft check segment with correct alias per table:
    draft_checks = " AND ".join([f"{t['alias']}-DRAFT = SPACE" for t in v_tables])

    # Insert into WHERE-clause, or create WHERE if missing (before INTO):
    m_where = re.search(r"\bWHERE\b", sel_stmt, re.IGNORECASE)
    if m_where:
        # After WHERE, before first condition (or as first AND):
        insert_pos = m_where.end()
        stmt = sel_stmt[:insert_pos] + f" {draft_checks} AND" + sel_stmt[insert_pos:]
    else:
        # Place before INTO
        m = re.search(r"\bINTO\b", sel_stmt, re.IGNORECASE)
        if m:
            stmt = sel_stmt[:m.start()] + f" WHERE {draft_checks} " + sel_stmt[m.start():]
        else:
            # Append at end as new WHERE segment
            stmt = sel_stmt.rstrip(".") + f" WHERE {draft_checks}."
    return stmt

def build_replacement_stmt(sel_text: str, tables: list, target_type: str, target_name: str) -> str:
    stmt = ensure_draft_filter(sel_text, tables)
    return re.sub(r"\s+", " ", stmt).strip()

def apply_span_replacements(source: str, repls: List[Tuple[Tuple[int, int], str]]) -> str:
    out = source
    for (s, e), r in sorted(repls, key=lambda x: x[0][0], reverse=True):
        out = out[:s] + r + out[e:]
    return out

@app.post("/remediate-array")
async def remediate_array(units: List[Unit]):
    """
    Find and remediate all VBRK/VBRP SELECTs (any field list, any JOIN/INTO syntax, any alias, etc).
    Returns English suggestion, code snippets and the fully remediated code.
    Only SELECTs needing a remediation or suggestion are included in the selects array.
    """
    results = []
    for u in units:
        src = u.code or ""
        selects = find_selects(src)
        replacements = []
        selects_metadata = []
        for sel in selects:
            new_stmt = build_replacement_stmt(
                sel["text"],
                sel["tables"],
                sel["target_type"],
                sel["target_name"]
            )
            # Only include if remediation is needed
            if new_stmt != sel["text"]:
                table_list = ", ".join([f"{t['table']} ({t['alias']})" for t in sel["tables"] if t["table"] in ("VBRK", "VBRP")])
                eng_suggestion = (
                    f"For this SELECT with join on {table_list}, add condition(s) '[alias]-DRAFT = SPACE' for each relevant table."
                )
                sel_info = {
                    "tables": sel["tables"],
                    "target_type": sel["target_type"],
                    "target_name": sel["target_name"],
                    "start_char_in_unit": sel["span"][0],
                    "end_char_in_unit": sel["span"][1],
                    "original_snippet": sel["text"],
                    "remediated_snippet": new_stmt,
                    "used_fields": [],
                    "ambiguous": False,
                    "suggested_fields": None,
                    "suggested_statement": eng_suggestion,
                }
                replacements.append((sel["span"], new_stmt))
                selects_metadata.append(sel_info)
        remediated_code = apply_span_replacements(src, replacements)
        result = json.loads(u.model_dump_json())
        result["original_code"] = src
        result["remediated_code"] = remediated_code
        result["selects"] = selects_metadata
        results.append(result)

    # ------- KEY: Return "system style" ------
    if len(results) == 1:
        return results[0]
    return results

@app.get("/health")
def health():
    return {"ok": True}