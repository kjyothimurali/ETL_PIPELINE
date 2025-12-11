# run_pipeline.py
"""
Run the full ETL pipeline end-to-end:

1. extract.py          -> fetch_all_cities(...) or fallback to `python extract.py`
2. transform.py        -> transform_files(json_paths) or fallback to `python transform.py`
3. load.py             -> load_csv_to_supabase(...) or fallback to `python load.py`
4. etl_analysis.py     -> main() or fallback to `python etl_analysis.py`

Behavior:
- Attempts to import and call the functions directly (preferred).
- If import fails (or an imported function is missing), falls back to running the script
  as a subprocess using the current Python interpreter.
- Performs basic checks between stages (e.g., raw files existed, transformed CSV exists).
- Prints simple start/finish logs and error details.
- Safe to run on Windows / Linux (uses `sys.executable`).
"""
from __future__ import annotations

import sys
import subprocess
import traceback
from pathlib import Path
from datetime import datetime
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[0]
RAW_DIR = ROOT / "data" / "raw"
STAGED_DIR = ROOT / "data" / "staged"
TRANSFORMED_CSV = STAGED_DIR / "air_quality_transformed.csv"

PY = sys.executable  # python interpreter to run subprocesses


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _run_subprocess(script: Path, args: Optional[List[str]] = None) -> int:
    cmd = [PY, str(script)]
    if args:
        cmd += args
    print(f"[{_now()}] > Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    print(proc.stdout or "", end="")
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    return proc.returncode


def run_extract_via_import() -> bool:
    """
    Try to import extract.fetch_all_cities and run it.
    Returns True on success (saved files present), False otherwise.
    """
    try:
        import importlib

        mod = importlib.import_module("extract")
        if hasattr(mod, "fetch_all_cities"):
            print(f"[{_now()}] Running extract.fetch_all_cities()")
            res = mod.fetch_all_cities()  # expect list of dicts
            # simple success heuristic: at least one dict with success true and raw_path exists
            any_ok = False
            for r in res or []:
                if r.get("success") in ("true", True) and r.get("raw_path"):
                    p = Path(r["raw_path"])
                    if p.exists():
                        any_ok = True
            return any_ok
        else:
            print("extract.fetch_all_cities not found; falling back to subprocess.")
            return False
    except Exception:
        print("Import-run of extract failed; falling back to subprocess.")
        traceback.print_exc()
        return False


def run_transform_via_import(raw_files: Optional[List[Path]] = None) -> bool:
    """
    Try to import transform.transform_files and call it with available raw files.
    Returns True if transformed CSV exists after running.
    """
    try:
        import importlib

        mod = importlib.import_module("transform")
        if hasattr(mod, "transform_files"):
            # prepare list of raw files
            if raw_files is None:
                raw_files = sorted([p for p in RAW_DIR.glob("*_raw_*") if p.suffix in (".json", ".txt")])
            raw_paths = [str(p) for p in raw_files]
            print(f"[{_now()}] Running transform.transform_files(...) on {len(raw_paths)} files")
            mod.transform_files(raw_paths)
            return TRANSFORMED_CSV.exists()
        else:
            print("transform.transform_files not found; falling back to subprocess.")
            return False
    except Exception:
        print("Import-run of transform failed; falling back to subprocess.")
        traceback.print_exc()
        return False


def run_load_via_import() -> bool:
    """
    Try to import load.load_csv_to_supabase and call it.
    Returns True if the load step was attempted (we can't always detect DB success here).
    """
    try:
        import importlib

        mod = importlib.import_module("load")
        if hasattr(mod, "load_csv_to_supabase"):
            print(f"[{_now()}] Running load.load_csv_to_supabase(...)")
            # call with the path constant expected by that module or pass TRANSFORMED_CSV
            try:
                mod.load_csv_to_supabase(str(TRANSFORMED_CSV))
            except TypeError:
                # maybe function expects no args
                mod.load_csv_to_supabase()
            return True
        else:
            print("load.load_csv_to_supabase not found; falling back to subprocess.")
            return False
    except Exception:
        print("Import-run of load failed; falling back to subprocess.")
        traceback.print_exc()
        return False


def run_analysis_via_import() -> bool:
    """
    Try to import etl_analysis.main() and call it.
    Returns True if main() executed without raising.
    """
    try:
        import importlib

        mod = importlib.import_module("etl_analysis")
        if hasattr(mod, "main"):
            print(f"[{_now()}] Running etl_analysis.main()")
            mod.main()
            return True
        else:
            print("etl_analysis.main not found; falling back to subprocess.")
            return False
    except Exception:
        print("Import-run of etl_analysis failed; falling back to subprocess.")
        traceback.print_exc()
        return False


def run_stage_with_fallback(script_name: str, import_runner, script_path: Path) -> bool:
    """
    Generic: try import_runner(); if it returns True -> success.
    Otherwise run script_path as subprocess and return whether script returned 0.
    """
    ok = import_runner()
    if ok:
        print(f"[{_now()}] {script_name} completed via import method.")
        return True

    print(f"[{_now()}] Falling back to running {script_path} as a subprocess.")
    rc = _run_subprocess(script_path)
    if rc == 0:
        print(f"[{_now()}] {script_name} completed (subprocess return code 0).")
        return True
    else:
        print(f"[{_now()}] {script_name} failed (subprocess return code {rc}).")
        return False


def main():
    print(f"[{_now()}] Pipeline start")

    # 1) Extract
    extract_script = ROOT / "extract.py"
    ok_extract = run_stage_with_fallback(
        "Extract",
        run_extract_via_import,
        extract_script,
    )

    # Determine raw files for transform step
    raw_files = sorted([p for p in RAW_DIR.glob("*_raw_*") if p.suffix in (".json", ".txt")])

    # 2) Transform (only if raw files exist)
    transform_script = ROOT / "transform.py"
    if not raw_files:
        print(f"[{_now()}] No raw files found in {RAW_DIR}; skipping Transform stage.")
        ok_transform = False
    else:
        ok_transform = run_stage_with_fallback(
            "Transform",
            lambda: run_transform_via_import(raw_files),
            transform_script,
        )

    # 3) Load (only if transformed CSV exists)
    load_script = ROOT / "load.py"
    if TRANSFORMED_CSV.exists():
        ok_load = run_stage_with_fallback("Load", run_load_via_import, load_script)
    else:
        print(f"[{_now()}] Transformed CSV not found at {TRANSFORMED_CSV}; skipping Load stage.")
        ok_load = False

    # 4) Analysis (try regardless; analysis may read Supabase even if previous stage failed)
    analysis_script = ROOT / "etl_analysis.py"
    ok_analysis = run_stage_with_fallback("Analysis", run_analysis_via_import, analysis_script)

    print(f"[{_now()}] Pipeline finished. Summary:")
    print(f"  Extract:   {'OK' if ok_extract else 'FAILED / SKIPPED'}")
    print(f"  Transform: {'OK' if ok_transform else 'FAILED / SKIPPED'}")
    print(f"  Load:      {'OK' if ok_load else 'FAILED / SKIPPED'}")
    print(f"  Analysis:  {'OK' if ok_analysis else 'FAILED / SKIPPED'}")


if __name__ == "__main__":
    main()
