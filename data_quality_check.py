"""
╔══════════════════════════════════════════════════════════╗
║         AUTOMATED DATA QUALITY CHECKER                  ║
║         Microsoft SQL Server Edition                    ║
║         Bronze Layer Validation Gate                    ║
╚══════════════════════════════════════════════════════════╝
"""

import pyodbc
import json
import re
from datetime import datetime
from pathlib import Path

from config import SQL_SERVER_CONFIG, TABLES

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
REPORT_DIR        = Path("dq_reports")
OUTLIER_THRESHOLD = 1.5

FORMAT_RULES = {
    "email":   r"^[\w\.-]+@[\w\.-]+\.\w{2,}$",
    "phone":   r"^\+?[\d\s\-\(\)]{7,15}$",
    "date":    r"^\d{4}-\d{2}-\d{2}$",
    "zipcode": r"^\d{5}(-\d{4})?$",
}


# ─────────────────────────────────────────────
# CONNECT TO SQL SERVER
# ─────────────────────────────────────────────
def get_connection():
    cfg = SQL_SERVER_CONFIG

    if cfg.get("trusted_connection"):
        conn_str = (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"UID={cfg['username']};"
            f"PWD={cfg['password']};"
        )

    return pyodbc.connect(conn_str)


# ─────────────────────────────────────────────
# DATA QUALITY CHECKER CLASS
# ─────────────────────────────────────────────
class DataQualityChecker:

    def __init__(self, conn, table):
        self.conn    = conn
        self.table   = table
        self.cur     = conn.cursor()
        self.results = {
            "meta":       {},
            "nulls":      {},
            "duplicates": {},
            "formats":    {},
            "outliers":   {},
            "trim":       {},
            "summary":    {},
        }

    # ── CHECK 1: TABLE INFO ──────────────────
    def scan_metadata(self):
        self.cur.execute(f"SELECT COUNT(*) FROM {self.table}")
        total_rows = self.cur.fetchone()[0]

        schema, tbl = self._split_table()
        self.cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema, tbl)
        cols_info = self.cur.fetchall()

        self.results["meta"] = {
            "table":      self.table,
            "total_rows": total_rows,
            "columns":    [r[0] for r in cols_info],
            "col_types":  {r[0]: r[1] for r in cols_info},
            "scanned_at": datetime.now().isoformat(timespec="seconds"),
        }
        return self

    # ── CHECK 2: NULL COUNTS ─────────────────
    def check_nulls(self):
        cols  = self.results["meta"]["columns"]
        total = self.results["meta"]["total_rows"]

        for col in cols:
            self.cur.execute(
                f"SELECT COUNT(*) FROM {self.table} WHERE [{col}] IS NULL"
            )
            null_count = self.cur.fetchone()[0]
            self.results["nulls"][col] = {
                "null_count": null_count,
                "null_pct":   round(null_count / total * 100, 1) if total else 0,
                "status":     "FAIL" if null_count > 0 else "PASS",
            }
        return self

    # ── CHECK 3: DUPLICATE ROWS ──────────────
    def check_duplicates(self):
        cols     = self.results["meta"]["columns"]
        col_list = ", ".join(f"[{c}]" for c in cols)

        self.cur.execute(f"""
            SELECT {col_list}, COUNT(*) AS cnt
            FROM {self.table}
            GROUP BY {col_list}
            HAVING COUNT(*) > 1
        """)
        dup_groups = self.cur.fetchall()

        self.results["duplicates"] = {
            "duplicate_groups": len(dup_groups),
            "affected_rows":    len(dup_groups),
            "status":           "FAIL" if dup_groups else "PASS",
            "sample_duplicates":[list(r) for r in dup_groups[:3]],
        }
        return self

    # ── CHECK 4: FORMAT VALIDATION ───────────
    def check_formats(self):
        cols = self.results["meta"]["columns"]

        for col in cols:
            fmt_type = None
            for fmt_name in FORMAT_RULES:
                if fmt_name in col.lower():
                    fmt_type = fmt_name
                    break

            if not fmt_type:
                continue

            self.cur.execute(
                f"SELECT [{col}] FROM {self.table} WHERE [{col}] IS NOT NULL"
            )
            values  = [r[0] for r in self.cur.fetchall()]
            pattern = FORMAT_RULES[fmt_type]
            invalid = [v for v in values if not re.match(pattern, str(v))]

            self.results["formats"][col] = {
                "format_type":    fmt_type,
                "total_non_null": len(values),
                "invalid_count":  len(invalid),
                "invalid_pct":    round(len(invalid) / len(values) * 100, 1) if values else 0,
                "invalid_samples":invalid[:5],
                "status":         "FAIL" if invalid else "PASS",
            }
        return self

    # ── CHECK 5: OUTLIER DETECTION ───────────
    def check_outliers(self):
        cols      = self.results["meta"]["columns"]
        col_types = self.results["meta"]["col_types"]

        numeric_types = {
            "int", "bigint", "smallint", "tinyint",
            "decimal", "numeric", "float", "real", "money"
        }

        for col in cols:
            if col_types.get(col, "").lower() not in numeric_types:
                continue

            try:
                self.cur.execute(f"""
                    SELECT
                        PERCENTILE_CONT(0.25) WITHIN GROUP
                            (ORDER BY [{col}]) OVER () AS q1,
                        PERCENTILE_CONT(0.75) WITHIN GROUP
                            (ORDER BY [{col}]) OVER () AS q3,
                        AVG(CAST([{col}] AS FLOAT))    AS mean_val,
                        STDEV([{col}])                 AS stdev_val,
                        COUNT([{col}])                 AS cnt
                    FROM {self.table}
                    WHERE [{col}] IS NOT NULL
                """)
                row = self.cur.fetchone()

                if not row or row[4] < 4:
                    continue

                q1, q3, mean_val, stdev_val, cnt = row
                iqr  = q3 - q1
                low  = q1 - OUTLIER_THRESHOLD * iqr
                high = q3 + OUTLIER_THRESHOLD * iqr

                self.cur.execute(f"""
                    SELECT [{col}]
                    FROM {self.table}
                    WHERE [{col}] IS NOT NULL
                      AND (
                          CAST([{col}] AS FLOAT) < ?
                          OR
                          CAST([{col}] AS FLOAT) > ?
                      )
                    ORDER BY [{col}]
                """, low, high)

                outlier_vals = [r[0] for r in self.cur.fetchall()]

                self.results["outliers"][col] = {
                    "q1":            round(float(q1), 2),
                    "q3":            round(float(q3), 2),
                    "iqr":           round(float(iqr), 2),
                    "lower_fence":   round(float(low), 2),
                    "upper_fence":   round(float(high), 2),
                    "mean":          round(float(mean_val), 2),
                    "stdev":         round(float(stdev_val or 0), 2),
                    "outlier_count": len(outlier_vals),
                    "outlier_values":outlier_vals[:10],
                    "status":        "FAIL" if outlier_vals else "PASS",
                }

            except pyodbc.Error as e:
                self.results["outliers"][col] = {
                    "error":  str(e),
                    "status": "ERROR"
                }

        return self

    # ── CHECK 6: TRIM CHECK ──────────────────
    def check_trim(self):
        """
        Finds values that have extra spaces before or after the text.

        Examples of trim violations:
            "  Alice"      <- leading space
            "Bob  "        <- trailing space
            "  Carol  "    <- both sides

        SQL used:
            WHERE column != LTRIM(RTRIM(column))
            LTRIM removes leading spaces
            RTRIM removes trailing spaces
        """
        cols      = self.results["meta"]["columns"]
        col_types = self.results["meta"]["col_types"]

        # Only check text columns
        text_types = {
            "char", "nchar", "varchar", "nvarchar",
            "text", "ntext"
        }

        for col in cols:
            if col_types.get(col, "").lower() not in text_types:
                continue

            # Count rows where value != trimmed value
            self.cur.execute(f"""
                SELECT COUNT(*)
                FROM {self.table}
                WHERE [{col}] IS NOT NULL
                  AND [{col}] != LTRIM(RTRIM([{col}]))
            """)
            trim_count = self.cur.fetchone()[0]

            # Get sample values that have spaces
            self.cur.execute(f"""
                SELECT TOP 5 [{col}]
                FROM {self.table}
                WHERE [{col}] IS NOT NULL
                  AND [{col}] != LTRIM(RTRIM([{col}]))
            """)
            sample_vals = [r[0] for r in self.cur.fetchall()]

            self.results["trim"][col] = {
                "trim_count":   trim_count,
                "sample_values":sample_vals,
                "status":       "FAIL" if trim_count > 0 else "PASS",
            }

        return self

    # ── QUALITY SCORE ────────────────────────
    def build_summary(self):
        r = self.results

        null_cols  = sum(1 for v in r["nulls"].values()    if v["status"] == "FAIL")
        fmt_cols   = sum(1 for v in r["formats"].values()  if v["status"] == "FAIL")
        out_cols   = sum(1 for v in r["outliers"].values() if v.get("status") == "FAIL")
        trim_cols  = sum(1 for v in r["trim"].values()     if v["status"] == "FAIL")
        has_dups   = r["duplicates"]["status"] == "FAIL"

        total_issues = null_cols + fmt_cols + out_cols + trim_cols + (1 if has_dups else 0)
        total_cols   = max(1, len(r["meta"]["columns"]))
        score        = max(0, round(100 - (total_issues / total_cols) * 25))

        if   score >= 90: grade = "EXCELLENT"
        elif score >= 75: grade = "GOOD"
        elif score >= 50: grade = "NEEDS WORK"
        else:             grade = "CRITICAL"

        r["summary"] = {
            "total_rows":            r["meta"]["total_rows"],
            "columns_with_nulls":    null_cols,
            "duplicate_groups":      r["duplicates"]["duplicate_groups"],
            "columns_bad_format":    fmt_cols,
            "columns_with_outliers": out_cols,
            "columns_with_trim":     trim_cols,
            "total_issues":          total_issues,
            "quality_score":         score,
            "grade":                 grade,
            "silver_layer_ready":    grade in ("EXCELLENT", "GOOD"),
        }
        return self

    # ── RUN ALL CHECKS ───────────────────────
    def run_all(self):
        return (
            self.scan_metadata()
                .check_nulls()
                .check_duplicates()
                .check_formats()
                .check_outliers()
                .check_trim()
                .build_summary()
                .results
        )

    # ── HELPER ───────────────────────────────
    def _split_table(self):
        parts = self.table.replace("[","").replace("]","").split(".")
        if len(parts) == 2:
            return parts[0], parts[1]
        return "dbo", parts[0]


# ─────────────────────────────────────────────
# PRINT REPORT IN TERMINAL
# ─────────────────────────────────────────────
def print_console(results):
    s = results["summary"]
    m = results["meta"]

    print("\n" + "=" * 60)
    print(f"  DQ REPORT  |  {m['table'].upper()}")
    print("=" * 60)
    print(f"  Rows scanned  : {m['total_rows']:,}")
    print(f"  Quality Score : {s['quality_score']}/100  [{s['grade']}]")
    print(f"  Silver Ready  : {'YES' if s['silver_layer_ready'] else 'NO - fix issues first'}")
    print("-" * 60)

    print("\n  NULLS")
    found = False
    for col, v in results["nulls"].items():
        if v["null_count"] > 0:
            print(f"    {col:<30} {v['null_count']} nulls ({v['null_pct']}%)")
            found = True
    if not found:
        print("    No nulls found")

    print("\n  DUPLICATES")
    d = results["duplicates"]
    if d["duplicate_groups"] > 0:
        print(f"    {d['duplicate_groups']} duplicate groups found")
    else:
        print("    No duplicates found")

    print("\n  FORMAT VIOLATIONS")
    found = False
    for col, v in results["formats"].items():
        if v["invalid_count"] > 0:
            print(f"    {col:<30} {v['invalid_count']} invalid [{v['format_type']}]")
            found = True
    if not found:
        print("    No format violations found")

    print("\n  OUTLIERS")
    found = False
    for col, v in results["outliers"].items():
        if v.get("outlier_count", 0) > 0:
            print(f"    {col:<30} {v['outlier_count']} outlier(s) -> {v['outlier_values'][:3]}")
            found = True
    if not found:
        print("    No outliers found")

    print("\n  TRIM VIOLATIONS (extra spaces)")
    found = False
    for col, v in results["trim"].items():
        if v["trim_count"] > 0:
            print(f"    {col:<30} {v['trim_count']} values have extra spaces")
            found = True
    if not found:
        print("    No trim violations found")

    print("\n" + "=" * 60)


# ─────────────────────────────────────────────
# SAVE JSON REPORT
# ─────────────────────────────────────────────
def save_json_report(results, path):
    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  JSON saved -> {path}")


# ─────────────────────────────────────────────
# SAVE HTML REPORT
# ─────────────────────────────────────────────
def save_html_report(results, path):
    meta     = results["meta"]
    summ     = results["summary"]
    nulls    = results["nulls"]
    dups     = results["duplicates"]
    formats  = results["formats"]
    outliers = results["outliers"]

    grade_color = {
        "EXCELLENT": "#22c55e",
        "GOOD":      "#84cc16",
        "NEEDS WORK":"#f59e0b",
        "CRITICAL":  "#ef4444",
    }.get(summ["grade"], "#6b7280")

    silver_color = "#22c55e" if summ["silver_layer_ready"] else "#ef4444"
    silver_label = "SILVER READY" if summ["silver_layer_ready"] else "NOT READY"

    def badge(status):
        color = "#22c55e" if status == "PASS" else "#ef4444"
        return f'<span style="background:{color};color:#fff;padding:2px 10px;border-radius:4px;font-size:11px;font-weight:700">{status}</span>'

    null_rows = ""
    for col, v in nulls.items():
        bar = min(v["null_pct"], 100)
        null_rows += f"""<tr><td>{col}</td><td>{v['null_count']:,}</td>
          <td>{v['null_pct']}%
            <div style="display:inline-block;width:80px;background:#2d3748;border-radius:3px;height:6px;vertical-align:middle;margin-left:8px">
              <div style="background:#ef4444;width:{bar}%;height:6px;border-radius:3px"></div></div>
          </td><td>{badge(v['status'])}</td></tr>"""

    fmt_rows = ""
    for col, v in formats.items():
        samples = ", ".join(str(s) for s in v["invalid_samples"]) or "none"
        fmt_rows += f"""<tr><td>{col}</td><td>{v['format_type']}</td>
          <td>{v['invalid_count']} / {v['total_non_null']}</td>
          <td style="color:#94a3b8;font-size:12px">{samples}</td>
          <td>{badge(v['status'])}</td></tr>"""

    out_rows = ""
    for col, v in outliers.items():
        if "error" in v:
            continue
        vals = ", ".join(str(x) for x in v.get("outlier_values", [])[:3]) or "none"
        out_rows += f"""<tr><td>{col}</td><td>{v.get('mean','')}</td>
          <td>{v.get('stdev','')}</td>
          <td>[{v.get('lower_fence','')} - {v.get('upper_fence','')}]</td>
          <td>{v.get('outlier_count',0)}</td>
          <td style="color:#f87171;font-size:12px">{vals}</td>
          <td>{badge(v['status'])}</td></tr>"""

    # Trim rows
    trim_rows = ""
    for col, v in results["trim"].items():
        samples = " | ".join(
            f'"{s}"' for s in v["sample_values"]
        ) or "none"
        trim_rows += f"""<tr><td>{col}</td>
          <td>{v['trim_count']}</td>
          <td style="color:#f87171;font-size:12px;font-family:monospace">{samples}</td>
          <td>{badge(v['status'])}</td></tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>DQ Report - {meta['table']}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;700&display=swap');
    * {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{ font-family:'Inter',sans-serif; background:#0f1117; color:#e2e8f0; padding:40px 24px; }}
    .wrap {{ max-width:1100px; margin:0 auto; }}
    .header {{ display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #1e2535; padding-bottom:24px; margin-bottom:32px; }}
    .header h1 {{ font-family:'JetBrains Mono',monospace; font-size:20px; color:#7dd3fc; }}
    .header .sub {{ color:#64748b; font-size:12px; margin-top:6px; font-family:'JetBrains Mono',monospace; }}
    .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:16px; margin-bottom:40px; }}
    .card {{ background:#151c2c; border:1px solid #1e2d45; border-radius:12px; padding:20px; text-align:center; }}
    .card .val {{ font-size:30px; font-weight:700; font-family:'JetBrains Mono',monospace; }}
    .card .lbl {{ font-size:11px; color:#64748b; text-transform:uppercase; letter-spacing:1px; margin-top:6px; }}
    .section {{ margin-bottom:36px; }}
    .section-title {{ font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:1.5px; color:#7dd3fc; border-left:3px solid #7dd3fc; padding-left:10px; margin-bottom:14px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th {{ background:#1a2235; color:#94a3b8; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:.8px; padding:10px 14px; text-align:left; }}
    td {{ padding:10px 14px; border-bottom:1px solid #1e2535; vertical-align:middle; }}
    tr:hover td {{ background:#151c2c; }}
    .grade {{ display:inline-block; background:{grade_color}22; color:{grade_color}; border:1px solid {grade_color}; font-family:'JetBrains Mono',monospace; font-size:14px; font-weight:700; padding:6px 18px; border-radius:999px; }}
    .silver {{ display:inline-block; background:{silver_color}22; color:{silver_color}; border:1px solid {silver_color}; font-size:13px; font-weight:700; padding:5px 14px; border-radius:999px; margin-top:8px; }}
    .empty {{ color:#475569; font-size:13px; padding:12px 0; }}
  </style>
</head>
<body><div class="wrap">

  <div class="header">
    <div>
      <h1>DATA QUALITY REPORT</h1>
      <div class="sub">table: {meta['table']} &nbsp;|&nbsp; rows: {meta['total_rows']:,} &nbsp;|&nbsp; scanned: {meta['scanned_at']}</div>
    </div>
    <div style="text-align:right">
      <div class="grade">{summ['grade']}</div>
      <div><span class="silver">{silver_label}</span></div>
    </div>
  </div>

  <div class="cards">
    <div class="card"><div class="val" style="color:#7dd3fc">{summ['quality_score']}</div><div class="lbl">Quality Score</div></div>
    <div class="card"><div class="val">{meta['total_rows']:,}</div><div class="lbl">Total Rows</div></div>
    <div class="card"><div class="val" style="color:{'#ef4444' if summ['columns_with_nulls'] else '#22c55e'}">{summ['columns_with_nulls']}</div><div class="lbl">Null Columns</div></div>
    <div class="card"><div class="val" style="color:{'#ef4444' if summ['duplicate_groups'] else '#22c55e'}">{summ['duplicate_groups']}</div><div class="lbl">Duplicates</div></div>
    <div class="card"><div class="val" style="color:{'#ef4444' if summ['columns_bad_format'] else '#22c55e'}">{summ['columns_bad_format']}</div><div class="lbl">Bad Formats</div></div>
    <div class="card"><div class="val" style="color:{'#ef4444' if summ['columns_with_outliers'] else '#22c55e'}">{summ['columns_with_outliers']}</div><div class="lbl">Outliers</div></div>
    <div class="card"><div class="val" style="color:{'#ef4444' if summ['columns_with_trim'] else '#22c55e'}">{summ['columns_with_trim']}</div><div class="lbl">Trim Issues</div></div>
  </div>

  <div class="section">
    <div class="section-title">Null / Missing Value Analysis</div>
    <table><tr><th>Column</th><th>Null Count</th><th>Null %</th><th>Status</th></tr>
    {null_rows if null_rows else '<tr><td colspan="4" class="empty">No null values found</td></tr>'}
    </table>
  </div>

  <div class="section">
    <div class="section-title">Duplicate Row Detection</div>
    <table><tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Duplicate Groups Found</td><td style="font-family:monospace">{dups['duplicate_groups']}</td></tr>
    <tr><td>Status</td><td>{badge(dups['status'])}</td></tr>
    </table>
  </div>

  <div class="section">
    <div class="section-title">Format Validation</div>
    {"<table><tr><th>Column</th><th>Format</th><th>Invalid Count</th><th>Sample Values</th><th>Status</th></tr>" + fmt_rows + "</table>"
      if fmt_rows else '<div class="empty">No format-checkable columns found</div>'}
  </div>

  <div class="section">
    <div class="section-title">Outlier Detection (IQR x {OUTLIER_THRESHOLD})</div>
    {"<table><tr><th>Column</th><th>Mean</th><th>StdDev</th><th>Valid Range</th><th>Outliers</th><th>Values</th><th>Status</th></tr>" + out_rows + "</table>"
      if out_rows else '<div class="empty">No numeric columns or no outliers detected</div>'}
  </div>

  <div class="section">
    <div class="section-title">Trim Check (Extra Spaces)</div>
    {"<table><tr><th>Column</th><th>Trim Violations</th><th>Sample Values</th><th>Status</th></tr>" + trim_rows + "</table>"
      if trim_rows else '<div class="empty">No trim violations found</div>'}
  </div>

</div></body></html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  HTML saved -> {path}")


# ─────────────────────────────────────────────
# MAIN — RUNS EVERYTHING
# ─────────────────────────────────────────────
def main():
    REPORT_DIR.mkdir(exist_ok=True)

    print("Connecting to SQL Server...")
    conn = get_connection()
    print("Connected successfully!\n")

    for table in TABLES:
        print(f"Checking {table}...")

        checker = DataQualityChecker(conn, table)
        results = checker.run_all()

        print_console(results)

        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        tbl_safe = table.replace(".", "_")
        save_json_report(results, REPORT_DIR / f"dq_{tbl_safe}_{ts}.json")
        save_html_report(results, REPORT_DIR / f"dq_{tbl_safe}_{ts}.html")

    conn.close()
    print("\nAll tables checked!")
    print(f"Reports saved to: {REPORT_DIR.resolve()}")


if __name__ == "__main__":
    main()