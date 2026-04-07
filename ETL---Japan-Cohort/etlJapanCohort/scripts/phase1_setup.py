"""
Phase 1: japan_cohort_raw → japan_cohort_cdm ETL (도메인별 SQL)
- run_japan_etl(flags) 에서 넘긴 dict 로 실행할 스크립트 선택
- execute_japan_etl.py 에서 호출
"""

from sqlalchemy import create_engine, text
import os
import re
import time
import urllib
import subprocess
import sys

server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_raw = "japan_cohort_raw"
database_cdm = "japan_cohort_cdm"
use_windows_auth = True

_script_dir = os.path.dirname(os.path.abspath(__file__))
sql_dir = os.path.normpath(os.path.join(_script_dir, "..", "inst", "sql", "sql_server"))

# 플래그 이름 → SQL 파일 (실행 순서 고정)
ETL_STEP_SQL = [
    ("person", "040.Person_japan.sql"),
    ("death", "050.Death_japan.sql"),
    ("observation_period", "060.Observation_period_japan.sql"),
    ("visit_occurrence", "070.Visit_occurrence_japan.sql"),
    ("condition_occurrence", "080.Condition_occurrence_japan.sql"),
    ("drug_exposure", "100.Drug_exposure_japan.sql"),
    ("procedure_occurrence", "110.Procedure_occurrence_japan.sql"),
]

MASTER_SQL = "010.Master_table_japan.sql"


def _format_elapsed(seconds):
    if seconds >= 3600:
        return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m {seconds % 60:.1f}s"
    if seconds >= 60:
        return f"{int(seconds // 60)}m {seconds % 60:.1f}s"
    return f"{seconds:.1f}s"


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
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        isolation_level="AUTOCOMMIT"
    )


def load_sql_batches(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    content = content.replace("@raw_database", f"{database_raw}.dbo")
    content = content.replace("@cdm_database", f"{database_cdm}.dbo")
    # GO 배치 분리 (SQL Server)
    batches = [b.strip() for b in re.split(r"^\s*GO\s*$", content, flags=re.MULTILINE) if b.strip()]
    return batches


def run_sql_file(engine, sql_path, label):
    if not os.path.exists(sql_path):
        print(f"  [SKIP] Not found: {label}")
        return
    batches = load_sql_batches(sql_path)
    to_run = [b for b in batches if b and not b.strip().startswith("--")]
    n = len(to_run)
    print(f"  Running {label}  ({n} SQL batch{'es' if n != 1 else ''})...", flush=True)
    file_t0 = time.perf_counter()
    with engine.connect() as conn:
        for i, batch in enumerate(to_run, start=1):
            t0 = time.perf_counter()
            preview = batch.strip().split("\n", 1)[0].strip()
            if len(preview) > 72:
                preview = preview[:69] + "..."
            print(f"    [{i}/{n}] {preview}", flush=True)
            conn.execute(text(batch))
            print(f"    [{i}/{n}] ok  ({_format_elapsed(time.perf_counter() - t0)})", flush=True)
    print(
        f"  Done: {label}  (total {_format_elapsed(time.perf_counter() - file_t0)})",
        flush=True,
    )


def run_master_table():
    path = os.path.join(sql_dir, MASTER_SQL)
    run_sql_file(_engine_cdm(), path, MASTER_SQL)


def run_etl_steps(flags):
    """
    flags: dict (또는 속성으로 True/False 인 객체)
    """
    enabled = []
    for key, sql_name in ETL_STEP_SQL:
        val = flags.get(key) if isinstance(flags, dict) else getattr(flags, key, False)
        if val:
            enabled.append((key, sql_name))
    if not enabled:
        return
    total = len(enabled)
    print(f"  Domain steps enabled: {total}  →  " + ", ".join(k for k, _ in enabled), flush=True)
    engine = _engine_cdm()
    phase_t0 = time.perf_counter()
    for step_i, (key, sql_name) in enumerate(enabled, start=1):
        print(f"\n  --- Step {step_i}/{total}: {key} ---", flush=True)
        if key == "procedure_occurrence":
            # Optional: build local mapping tables before procedure ETL
            mode = None
            if isinstance(flags, dict):
                mode = flags.get("procedure_mapping_mode")
            else:
                mode = getattr(flags, "procedure_mapping_mode", None)
            if mode:
                mode = str(mode).strip().lower()
                if mode == "rules_plus_usagi":
                    mode = "rules_plus_external"
                if mode not in ("rules_only", "rules_plus_external"):
                    raise ValueError(
                        "procedure_mapping_mode must be 'rules_only' or 'rules_plus_external' "
                        "(legacy 'rules_plus_usagi' is normalized to rules_plus_external). "
                        f"Got {mode!r}."
                    )

                def _pmap_get(key_new, key_old, default=None):
                    if isinstance(flags, dict):
                        v = flags.get(key_new)
                        if v is not None:
                            return v
                        return flags.get(key_old, default)
                    v = getattr(flags, key_new, None)
                    if v is not None:
                        return v
                    return getattr(flags, key_old, default)

                ext_csv = _pmap_get(
                    "procedure_external_mapping_csv",
                    "procedure_usagi_results_csv",
                )
                thr = _pmap_get(
                    "procedure_mapping_auto_score_threshold",
                    "procedure_usagi_auto_threshold",
                    0.90,
                )
                export_csv = _pmap_get(
                    "procedure_mapping_export_csv",
                    "procedure_usagi_export_csv",
                )

                env = os.environ.copy()
                env["PROCEDURE_MAPPING_MODE"] = mode
                if ext_csv:
                    env["PROCEDURE_EXTERNAL_MAPPING_CSV"] = str(ext_csv)
                if export_csv:
                    env["PROCEDURE_MAPPING_EXPORT_CSV"] = str(export_csv)
                env["PROCEDURE_MAPPING_AUTO_SCORE_THRESHOLD"] = str(thr if thr is not None else 0.90)

                script = os.path.join(_script_dir, "procedure_mapping.py")
                print(
                    f"  [procedure_mapping] mode={mode}  auto_score_threshold={env['PROCEDURE_MAPPING_AUTO_SCORE_THRESHOLD']}",
                    flush=True,
                )
                if export_csv:
                    print(f"  [procedure_mapping] mapping_export_csv={export_csv}", flush=True)
                if ext_csv:
                    print(f"  [procedure_mapping] external_mapping_csv={ext_csv}", flush=True)

                rc = subprocess.call([sys.executable, script], env=env, cwd=_script_dir)
                if rc != 0:
                    raise RuntimeError(f"procedure_mapping.py failed (exit={rc})")
        path = os.path.join(sql_dir, sql_name)
        run_sql_file(engine, path, sql_name)
    print(
        f"\n  All domain steps finished  (total {_format_elapsed(time.perf_counter() - phase_t0)})",
        flush=True,
    )


if __name__ == "__main__":
    # 하위 호환: 기본은 전부 실행
    import types
    flags = {k: True for k, _ in ETL_STEP_SQL}
    print("\n--- Phase 1: all ETL steps (legacy __main__) ---\n")
    run_etl_steps(flags)
    print("\n[Phase 1 Complete]")
