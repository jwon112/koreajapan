"""
Build local (project-owned) mapping tables for Japan procedure codes.

Tool-agnostic: any external workflow that produces a CSV of
  source_code → target_concept_id (+ optional score)
can be imported; Usagi is one possible generator of that CSV.

Modes:
  - rules_only: deterministic rules from JP_PROCEDURE_MASTER + source_to_concept_map
  - rules_plus_external: rules + import external CSV, auto-accept rows with score >= threshold

Intermediate tables live in CDM for audit/reproducibility.
"""

from __future__ import annotations

import csv
import os
import time
import urllib

from sqlalchemy import create_engine, text


server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_raw = "japan_cohort_raw"
database_cdm = "japan_cohort_cdm"
use_windows_auth = True


def _engine_cdm():
    if use_windows_auth:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_cdm};"
            f"Trusted_Connection=Yes;Encrypt=no;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_cdm};"
            f"UID=sa;PWD=KoreaJapan44@;Encrypt=no;"
        )
    params = urllib.parse.quote_plus(conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", isolation_level="AUTOCOMMIT")


def _engine_raw():
    if use_windows_auth:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_raw};"
            f"Trusted_Connection=Yes;Encrypt=no;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_raw};"
            f"UID=sa;PWD=KoreaJapan44@;Encrypt=no;"
        )
    params = urllib.parse.quote_plus(conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", isolation_level="AUTOCOMMIT")


DDL = """
IF OBJECT_ID(N'dbo.jp_procedure_map_rules', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.jp_procedure_map_rules (
    source_code              VARCHAR(50)   NOT NULL,
    target_concept_id        INT           NOT NULL,
    rule_name                VARCHAR(50)   NOT NULL,
    created_at               DATETIME2(0)  NOT NULL CONSTRAINT DF_jp_pmr_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_jp_procedure_map_rules PRIMARY KEY (source_code, rule_name, target_concept_id)
  );
END;

IF OBJECT_ID(N'dbo.jp_procedure_map_external', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.jp_procedure_map_external (
    source_code              VARCHAR(50)   NOT NULL,
    target_concept_id        INT           NOT NULL,
    score                    FLOAT         NULL,
    auto_accepted            BIT           NOT NULL CONSTRAINT DF_jp_pme_auto DEFAULT 0,
    created_at               DATETIME2(0)  NOT NULL CONSTRAINT DF_jp_pme_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_jp_procedure_map_external PRIMARY KEY (source_code, target_concept_id)
  );
END;

-- Legacy table name (pre tool-agnostic rename): copy once if new table is empty
IF OBJECT_ID(N'dbo.jp_procedure_map_usagi', N'U') IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dbo.jp_procedure_map_external)
BEGIN
  INSERT INTO dbo.jp_procedure_map_external (source_code, target_concept_id, score, auto_accepted, created_at)
  SELECT source_code, target_concept_id, score, auto_accepted, created_at
  FROM dbo.jp_procedure_map_usagi;
END;

IF OBJECT_ID(N'dbo.jp_procedure_map_final', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.jp_procedure_map_final (
    source_code              VARCHAR(50)   NOT NULL PRIMARY KEY,
    target_concept_id        INT           NOT NULL,
    method                   VARCHAR(30)   NOT NULL,  -- rules / external_auto
    score                    FLOAT         NULL,
    updated_at               DATETIME2(0)  NOT NULL CONSTRAINT DF_jp_pmf_updated_at DEFAULT SYSUTCDATETIME()
  );
END;
"""


EXPORT_SQL = """
SELECT
  pm.procedure_code AS sourceCode,
  pm.standardized_procedure_name AS sourceName,
  pm.standardized_procedure_code AS standardizedCode,
  pm.standardized_procedure_version AS standardizedVersion,
  pm.procedure_category_medium_class AS cat_medium,
  pm.procedure_category_small_classi AS cat_small,
  pm.procedure_category_subclassific AS cat_sub,
  pm.procedure_category_sub_and_deta AS cat_detail,
  pm.icd9cm_level1 AS icd9_1,
  pm.icd9cm_level2 AS icd9_2,
  pm.icd9cm_level3 AS icd9_3
FROM japan_cohort_raw.dbo.JP_PROCEDURE_MASTER pm
WHERE pm.procedure_code IS NOT NULL AND LTRIM(RTRIM(pm.procedure_code)) <> ''
"""


RULES_REFRESH_SQL = """
TRUNCATE TABLE dbo.jp_procedure_map_rules;

INSERT INTO dbo.jp_procedure_map_rules (source_code, target_concept_id, rule_name)
SELECT DISTINCT
  UPPER(LTRIM(RTRIM(pm.procedure_code))) AS source_code,
  stm.target_concept_id,
  'pm_standardized_code' AS rule_name
FROM japan_cohort_raw.dbo.JP_PROCEDURE_MASTER pm
JOIN dbo.source_to_concept_map stm
  ON stm.source_code = UPPER(LTRIM(RTRIM(pm.standardized_procedure_code)))
 AND stm.invalid_reason IS NULL
 AND LOWER(stm.domain_id) = 'procedure'
WHERE pm.standardized_procedure_code IS NOT NULL AND LTRIM(RTRIM(pm.standardized_procedure_code)) <> '';

INSERT INTO dbo.jp_procedure_map_rules (source_code, target_concept_id, rule_name)
SELECT DISTINCT
  UPPER(LTRIM(RTRIM(pm.procedure_code))) AS source_code,
  stm.target_concept_id,
  'pm_icd9cm_level3' AS rule_name
FROM japan_cohort_raw.dbo.JP_PROCEDURE_MASTER pm
JOIN dbo.source_to_concept_map stm
  ON stm.source_code = UPPER(LTRIM(RTRIM(pm.icd9cm_level3)))
 AND stm.invalid_reason IS NULL
 AND LOWER(stm.domain_id) = 'procedure'
WHERE pm.icd9cm_level3 IS NOT NULL AND LTRIM(RTRIM(pm.icd9cm_level3)) <> '';

INSERT INTO dbo.jp_procedure_map_rules (source_code, target_concept_id, rule_name)
SELECT DISTINCT
  UPPER(LTRIM(RTRIM(pm.procedure_code))) AS source_code,
  stm.target_concept_id,
  'pm_icd9cm_level2' AS rule_name
FROM japan_cohort_raw.dbo.JP_PROCEDURE_MASTER pm
JOIN dbo.source_to_concept_map stm
  ON stm.source_code = UPPER(LTRIM(RTRIM(pm.icd9cm_level2)))
 AND stm.invalid_reason IS NULL
 AND LOWER(stm.domain_id) = 'procedure'
WHERE pm.icd9cm_level2 IS NOT NULL AND LTRIM(RTRIM(pm.icd9cm_level2)) <> '';

INSERT INTO dbo.jp_procedure_map_rules (source_code, target_concept_id, rule_name)
SELECT DISTINCT
  UPPER(LTRIM(RTRIM(pm.procedure_code))) AS source_code,
  stm.target_concept_id,
  'pm_icd9cm_level1' AS rule_name
FROM japan_cohort_raw.dbo.JP_PROCEDURE_MASTER pm
JOIN dbo.source_to_concept_map stm
  ON stm.source_code = UPPER(LTRIM(RTRIM(pm.icd9cm_level1)))
 AND stm.invalid_reason IS NULL
 AND LOWER(stm.domain_id) = 'procedure'
WHERE pm.icd9cm_level1 IS NOT NULL AND LTRIM(RTRIM(pm.icd9cm_level1)) <> '';
"""


FINAL_REFRESH_SQL = """
TRUNCATE TABLE dbo.jp_procedure_map_final;

WITH ranked AS (
  SELECT
    r.source_code,
    r.target_concept_id,
    r.rule_name,
    ROW_NUMBER() OVER (
      PARTITION BY r.source_code
      ORDER BY
        CASE r.rule_name
          WHEN 'pm_standardized_code' THEN 1
          WHEN 'pm_icd9cm_level3' THEN 2
          WHEN 'pm_icd9cm_level2' THEN 3
          WHEN 'pm_icd9cm_level1' THEN 4
          ELSE 9
        END,
        r.target_concept_id
    ) AS rn
  FROM dbo.jp_procedure_map_rules r
)
INSERT INTO dbo.jp_procedure_map_final (source_code, target_concept_id, method, score)
SELECT source_code, target_concept_id, 'rules' AS method, NULL AS score
FROM ranked
WHERE rn = 1;

;WITH u AS (
  SELECT
    u.source_code,
    u.target_concept_id,
    u.score,
    ROW_NUMBER() OVER (
      PARTITION BY u.source_code
      ORDER BY ISNULL(u.score, 0) DESC, u.target_concept_id
    ) AS rn
  FROM dbo.jp_procedure_map_external u
  WHERE u.auto_accepted = 1
)
MERGE dbo.jp_procedure_map_final AS dst
USING (SELECT source_code, target_concept_id, score FROM u WHERE rn = 1) AS src
  ON dst.source_code = src.source_code
WHEN MATCHED THEN
  UPDATE SET target_concept_id = src.target_concept_id, method = 'external_auto', score = src.score, updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT (source_code, target_concept_id, method, score) VALUES (src.source_code, src.target_concept_id, 'external_auto', src.score);
"""


def export_mapping_reference_csv(out_path: str) -> int:
    """JP_PROCEDURE_MASTER → CSV for any external mapping tool (column names are hints, not tool-specific)."""
    parent = os.path.dirname(os.path.abspath(out_path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    eng_raw = _engine_raw()
    with eng_raw.connect() as conn:
        rows = conn.execute(text(EXPORT_SQL)).mappings().all()
    if not rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["sourceCode", "sourceName"])
            w.writeheader()
        return 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))
    return len(rows)


def import_external_mapping_csv(path: str, auto_threshold: float) -> int:
    """
    Flexible CSV columns:
      source_code | sourceCode
      target_concept_id | conceptId | targetConceptId
      score | similarity | similarity_score | mappingScore (optional)
    """
    eng = _engine_cdm()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return 0

    def _get(d, *keys):
        for k in keys:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return None

    payload = []
    for r in rows:
        sc = _get(r, "source_code", "sourceCode", "sourceCodeId", "source concept code")
        tc = _get(r, "target_concept_id", "targetConceptId", "conceptId", "target concept id")
        score = _get(r, "score", "similarity", "similarity_score", "mappingScore")
        if sc is None or tc is None:
            continue
        sc = str(sc).strip().upper()
        try:
            tc_i = int(float(tc))
        except Exception:
            continue
        try:
            score_f = float(score) if score is not None else None
        except Exception:
            score_f = None
        auto = 1 if (score_f is not None and score_f >= auto_threshold) else 0
        payload.append((sc, tc_i, score_f, auto))

    if not payload:
        return 0

    with eng.connect() as conn:
        for sc, tc_i, score_f, auto in payload:
            conn.execute(
                text(
                    """
MERGE dbo.jp_procedure_map_external AS dst
USING (SELECT :source_code AS source_code, :target_concept_id AS target_concept_id) AS src
  ON dst.source_code = src.source_code AND dst.target_concept_id = src.target_concept_id
WHEN MATCHED THEN
  UPDATE SET score = :score, auto_accepted = :auto, created_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
  INSERT (source_code, target_concept_id, score, auto_accepted) VALUES (:source_code, :target_concept_id, :score, :auto);
"""
                ),
                {
                    "source_code": sc,
                    "target_concept_id": tc_i,
                    "score": score_f,
                    "auto": auto,
                },
            )
    return len(payload)


def _normalize_mode(mode: str) -> str:
    m = (mode or "rules_only").lower().strip()
    if m == "rules_plus_usagi":
        print(
            "[procedure_mapping] NOTE: procedure_mapping_mode 'rules_plus_usagi' is deprecated; "
            "use 'rules_plus_external'.",
            flush=True,
        )
        return "rules_plus_external"
    return m


def build(
    mode: str,
    external_mapping_csv: str | None,
    auto_score_threshold: float,
    mapping_export_csv: str | None,
):
    mode = _normalize_mode(mode)
    if mode not in ("rules_only", "rules_plus_external"):
        raise ValueError(f"Unknown procedure_mapping_mode: {mode}")

    eng = _engine_cdm()
    t0 = time.perf_counter()
    with eng.connect() as conn:
        conn.execute(text(DDL))

    if mapping_export_csv:
        n = export_mapping_reference_csv(mapping_export_csv)
        print(
            f"[procedure_mapping] Exported reference CSV rows: {n} → {mapping_export_csv}",
            flush=True,
        )

    with eng.connect() as conn:
        conn.execute(text(RULES_REFRESH_SQL))
    print(f"[procedure_mapping] Rules refreshed ({time.perf_counter() - t0:.1f}s)", flush=True)

    if mode == "rules_plus_external" and external_mapping_csv:
        n = import_external_mapping_csv(external_mapping_csv, auto_threshold=auto_score_threshold)
        print(
            f"[procedure_mapping] Imported external mapping rows: {n}  (auto>= {auto_score_threshold})",
            flush=True,
        )

    with eng.connect() as conn:
        conn.execute(text(FINAL_REFRESH_SQL))
    print(f"[procedure_mapping] Final mapping refreshed ({time.perf_counter() - t0:.1f}s)", flush=True)


def main():
    mode = os.environ.get("PROCEDURE_MAPPING_MODE", "rules_only")
    ext_csv = os.environ.get("PROCEDURE_EXTERNAL_MAPPING_CSV") or os.environ.get(
        "PROCEDURE_USAGI_RESULTS_CSV"
    ) or None
    export_csv = os.environ.get("PROCEDURE_MAPPING_EXPORT_CSV") or os.environ.get(
        "PROCEDURE_USAGI_EXPORT_CSV"
    ) or None
    thr = float(
        os.environ.get(
            "PROCEDURE_MAPPING_AUTO_SCORE_THRESHOLD",
            os.environ.get("PROCEDURE_USAGI_AUTO_THRESHOLD", "0.90"),
        )
    )
    build(mode, ext_csv, thr, export_csv)


if __name__ == "__main__":
    main()
