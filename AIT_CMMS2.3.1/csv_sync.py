"""
CSV Synchronisation Module for AIT CMMS
========================================
Keeps the three priority CSV files (PM_LIST_A220_1.csv, PM_LIST_A220_2.csv,
PM_LIST_A220_3.csv) in sync with the SQLite database.

Rules
-----
* When an asset is added / updated with a priority level (1, 2 or 3) the
  corresponding CSV file is written with the asset's data.
* When a priority changes the asset is removed from the old CSV and added
  to the new one.
* When an asset has no priority (None / 0) it is removed from all three
  CSV files if it was previously listed.
* The CSV files are always kept sorted by BFM number for readability.

CSV columns  (same header format as the originals):
    SAP, BFM, DESCRIPTION, TOOL ID, LOCATION, PRIORITY, PM QTY,
    REVIEWED, Time of Completion
"""

import os
import csv
import threading
from typing import Optional, List, Dict

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

CSV_FILES = {
    1: os.path.join(_SCRIPT_DIR, "PM_LIST_A220_1.csv"),
    2: os.path.join(_SCRIPT_DIR, "PM_LIST_A220_2.csv"),
    3: os.path.join(_SCRIPT_DIR, "PM_LIST_A220_3.csv"),
}

CSV_COLUMNS = [
    "SAP",
    "BFM",
    "DESCRIPTION",
    "TOOL ID",
    "LOCATION",
    "PRIORITY",
    "PM QTY",
    "REVIEWED",
    "Time of Completion",
]

_csv_lock = threading.Lock()  # Protect concurrent CSV writes


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_csv(filepath: str) -> List[Dict]:
    """Read a priority CSV file and return a list of row dicts."""
    if not os.path.exists(filepath):
        return []

    rows = []
    try:
        with open(filepath, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(dict(row))
    except Exception as e:
        print(f"Warning: could not read {filepath}: {e}")

    return rows


def _write_csv(filepath: str, rows: List[Dict]) -> None:
    """Write rows to a priority CSV file (creates the file if missing)."""
    try:
        with open(filepath, "w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(f"Warning: could not write {filepath}: {e}")


def _bfm_key(row: Dict) -> str:
    """Return a normalised BFM string for comparison."""
    bfm = row.get("BFM", "")
    try:
        return str(int(float(str(bfm).strip())))
    except (ValueError, TypeError):
        return str(bfm).strip()


def _normalise_bfm(bfm) -> str:
    """Normalise a BFM value to a canonical string."""
    try:
        return str(int(float(str(bfm).strip())))
    except (ValueError, TypeError):
        return str(bfm).strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sync_asset_to_csv(
    bfm_no,
    description: str = "",
    location: str = "",
    sap_no: str = "",
    tool_id: str = "",
    priority: Optional[int] = None,
    pm_qty: str = "",
    reviewed: str = "NO",
    time_of_completion: str = "",
) -> None:
    """
    Add or update an asset in the appropriate priority CSV file.

    Parameters
    ----------
    bfm_no       : BFM equipment number (str or int)
    description  : Equipment description
    location     : Physical location
    sap_no       : SAP material number
    tool_id      : Tool ID / drawing number
    priority     : 1, 2 or 3  (None means "remove from all CSVs")
    pm_qty       : PM quantity
    reviewed     : "YES" / "NO"
    time_of_completion : Last completion date string
    """
    bfm_str = _normalise_bfm(bfm_no)
    if not bfm_str:
        return

    with _csv_lock:
        # Remove this BFM from every CSV first (handles priority changes cleanly)
        for p_level, filepath in CSV_FILES.items():
            rows = _read_csv(filepath)
            new_rows = [r for r in rows if _bfm_key(r) != bfm_str]
            if len(new_rows) != len(rows):  # Something was removed
                _write_csv(filepath, new_rows)

        # If a valid priority was given, add to the correct CSV
        if priority in (1, 2, 3):
            filepath = CSV_FILES[priority]
            rows = _read_csv(filepath)

            new_row = {
                "SAP": sap_no,
                "BFM": bfm_str,
                "DESCRIPTION": description,
                "TOOL ID": tool_id,
                "LOCATION": location,
                "PRIORITY": priority,
                "PM QTY": pm_qty,
                "REVIEWED": reviewed,
                "Time of Completion": time_of_completion,
            }

            # Append and sort by BFM
            rows.append(new_row)
            rows.sort(key=_bfm_key)
            _write_csv(filepath, rows)

            print(
                f"CSV sync: BFM {bfm_str} written to PM_LIST_A220_{priority}.csv"
            )
        else:
            print(f"CSV sync: BFM {bfm_str} removed from all priority CSVs")


def remove_asset_from_csv(bfm_no) -> None:
    """
    Remove an asset from all priority CSV files.

    Parameters
    ----------
    bfm_no : BFM equipment number
    """
    sync_asset_to_csv(bfm_no, priority=None)


def get_priority_from_csv(bfm_no) -> Optional[int]:
    """
    Look up the priority of an asset by scanning all three CSV files.

    Returns 1, 2, 3 or None if the asset is not listed in any CSV.
    """
    bfm_str = _normalise_bfm(bfm_no)
    for p_level, filepath in CSV_FILES.items():
        rows = _read_csv(filepath)
        for row in rows:
            if _bfm_key(row) == bfm_str:
                return p_level
    return None


def load_priority_map() -> Dict[str, int]:
    """
    Build and return a complete {bfm_str: priority} mapping from all CSV files.
    Useful for bulk priority lookups (e.g. PM scheduling).
    """
    priority_map: Dict[str, int] = {}
    for p_level, filepath in CSV_FILES.items():
        rows = _read_csv(filepath)
        for row in rows:
            bfm = _bfm_key(row)
            if bfm:
                priority_map[bfm] = p_level
    return priority_map


def rebuild_csvs_from_db(conn) -> None:
    """
    Rebuild all three CSV files from the equipment table in the SQLite database.

    The equipment table must have a 'priority' column (1, 2 or 3).
    Assets without a recognised priority are skipped.

    Parameters
    ----------
    conn : SQLite connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT bfm_equipment_no, description, location, sap_no, tool_id,
                   priority, pm_qty, last_monthly_pm
            FROM equipment
            WHERE priority IN (1, 2, 3)
            ORDER BY priority, bfm_equipment_no
            """
        )
        rows = cursor.fetchall()
    except Exception as e:
        print(f"rebuild_csvs_from_db: query failed: {e}")
        return
    finally:
        cursor.close()

    # Bucket rows by priority
    buckets: Dict[int, List[Dict]] = {1: [], 2: [], 3: []}
    for row in rows:
        if isinstance(row, dict):
            bfm = _normalise_bfm(row.get("bfm_equipment_no", ""))
            priority = row.get("priority")
            if priority in (1, 2, 3) and bfm:
                buckets[priority].append(
                    {
                        "SAP": row.get("sap_no", ""),
                        "BFM": bfm,
                        "DESCRIPTION": row.get("description", ""),
                        "TOOL ID": row.get("tool_id", ""),
                        "LOCATION": row.get("location", ""),
                        "PRIORITY": priority,
                        "PM QTY": row.get("pm_qty", ""),
                        "REVIEWED": "YES",
                        "Time of Completion": row.get("last_monthly_pm", ""),
                    }
                )

    with _csv_lock:
        for p_level, csv_rows in buckets.items():
            _write_csv(CSV_FILES[p_level], csv_rows)
            print(
                f"CSV rebuild: {len(csv_rows)} assets written to "
                f"PM_LIST_A220_{p_level}.csv"
            )
