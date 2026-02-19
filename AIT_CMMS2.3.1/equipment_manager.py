"""
Equipment Manager Module – SQLite edition
Handles equipment-related operations including:
- Equipment CRUD operations
- Equipment status management
- Equipment search and filtering
- Equipment validation
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, date


class EquipmentManager:
    """Manages equipment operations in the CMMS system"""

    def __init__(self, conn):
        """
        Initialise equipment manager.

        Args:
            conn: SQLite database connection
        """
        self.conn = conn

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_equipment_by_bfm(self, bfm_no: str) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT bfm_equipment_no, description, location, monthly_pm, annual_pm,
                   last_monthly_pm, last_annual_pm, next_annual_pm, status, priority
            FROM equipment
            WHERE bfm_equipment_no = ?
            """,
            (bfm_no,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        if isinstance(row, dict):
            return {
                "bfm_no":          row["bfm_equipment_no"],
                "description":     row["description"],
                "location":        row["location"],
                "has_monthly":     bool(row["monthly_pm"]),
                "has_annual":      bool(row["annual_pm"]),
                "last_monthly_pm": row["last_monthly_pm"],
                "last_annual_pm":  row["last_annual_pm"],
                "next_annual_pm":  row["next_annual_pm"],
                "status":          row["status"],
                "priority":        row.get("priority", 0),
            }
        # Tuple fallback
        return {
            "bfm_no":          row[0],
            "description":     row[1],
            "location":        row[2],
            "has_monthly":     bool(row[3]) if row[3] is not None else False,
            "has_annual":      bool(row[4]) if row[4] is not None else False,
            "last_monthly_pm": row[5],
            "last_annual_pm":  row[6],
            "next_annual_pm":  row[7],
            "status":          row[8],
            "priority":        row[9] if len(row) > 9 else 0,
        }

    def search_equipment(
        self, search_term: str, status_filter: Optional[str] = None
    ) -> List[Dict]:
        """Search equipment by BFM number or description (case-insensitive)."""
        cursor = self.conn.cursor()
        like_term = f"%{search_term}%"

        # SQLite LIKE is case-insensitive for ASCII by default
        query = """
            SELECT bfm_equipment_no, description, location, status
            FROM equipment
            WHERE (bfm_equipment_no LIKE ? OR description LIKE ?)
        """
        params: list = [like_term, like_term]

        if status_filter:
            query += " AND status = ?"
            params.append(status_filter)

        query += " ORDER BY bfm_equipment_no LIMIT 100"
        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            if isinstance(row, dict):
                results.append(row)
            else:
                results.append(
                    {
                        "bfm_no":      row[0],
                        "description": row[1],
                        "location":    row[2],
                        "status":      row[3],
                    }
                )
        return results

    def get_all_equipment(self, status_filter: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor()
        query = """
            SELECT bfm_equipment_no, description, location, monthly_pm, annual_pm,
                   last_monthly_pm, last_annual_pm, status
            FROM equipment
        """
        params: list = []
        if status_filter:
            query += " WHERE status = ?"
            params.append(status_filter)
        query += " ORDER BY bfm_equipment_no"
        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            if isinstance(row, dict):
                results.append(
                    {
                        "bfm_no":          row["bfm_equipment_no"],
                        "description":     row["description"],
                        "location":        row["location"],
                        "has_monthly":     bool(row["monthly_pm"]),
                        "has_annual":      bool(row["annual_pm"]),
                        "last_monthly_pm": row["last_monthly_pm"],
                        "last_annual_pm":  row["last_annual_pm"],
                        "status":          row["status"],
                    }
                )
            else:
                results.append(
                    {
                        "bfm_no":          row[0],
                        "description":     row[1],
                        "location":        row[2],
                        "has_monthly":     bool(row[3]) if row[3] is not None else False,
                        "has_annual":      bool(row[4]) if row[4] is not None else False,
                        "last_monthly_pm": row[5],
                        "last_annual_pm":  row[6],
                        "status":          row[7],
                    }
                )
        return results

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_bfm_number(self, bfm_no: str) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM equipment WHERE bfm_equipment_no = ?", (bfm_no,)
            )
            row = cursor.fetchone()
            count = row["COUNT(*)"] if isinstance(row, dict) else row[0]
            self.conn.commit()
            return count > 0
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise e

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def update_equipment_status(
        self, bfm_no: str, new_status: str, user_id: str
    ) -> bool:
        try:
            cursor = self.conn.cursor()

            cursor.execute(
                "SELECT status FROM equipment WHERE bfm_equipment_no = ?", (bfm_no,)
            )
            old_row = cursor.fetchone()
            old_status = (
                old_row["status"] if isinstance(old_row, dict) else old_row[0]
            ) if old_row else None

            cursor.execute(
                "UPDATE equipment SET status = ? WHERE bfm_equipment_no = ?",
                (new_status, bfm_no),
            )

            cursor.execute(
                """
                INSERT INTO audit_log
                (table_name, record_id, action, user_id, old_values, new_values, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "equipment",
                    bfm_no,
                    "update",
                    user_id,
                    f'{{"status": "{old_status}"}}',
                    f'{{"status": "{new_status}"}}',
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )

            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating equipment status: {e}")
            self.conn.rollback()
            return False

    def update_equipment_pm_dates(
        self, bfm_no: str, pm_type: str, completion_date: str, user_id: str
    ) -> bool:
        try:
            cursor = self.conn.cursor()
            if pm_type == "Monthly":
                cursor.execute(
                    "UPDATE equipment SET last_monthly_pm = ? WHERE bfm_equipment_no = ?",
                    (completion_date, bfm_no),
                )
            elif pm_type == "Annual":
                cursor.execute(
                    "UPDATE equipment SET last_annual_pm = ? WHERE bfm_equipment_no = ?",
                    (completion_date, bfm_no),
                )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating PM dates: {e}")
            self.conn.rollback()
            return False

    def add_equipment(
        self, equipment_data: Dict, user_id: str
    ) -> Tuple[bool, str]:
        """
        Add new equipment to the database.

        equipment_data keys:
            bfm_no, description, location, has_monthly, has_annual,
            status, priority (optional, 0/1/2/3)
        """
        try:
            cursor = self.conn.cursor()

            if self.validate_bfm_number(equipment_data["bfm_no"]):
                return (
                    False,
                    f"Equipment {equipment_data['bfm_no']} already exists",
                )

            cursor.execute(
                """
                INSERT INTO equipment
                    (bfm_equipment_no, description, location,
                     monthly_pm, annual_pm, status, priority)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    equipment_data["bfm_no"],
                    equipment_data.get("description", ""),
                    equipment_data.get("location", ""),
                    1 if equipment_data.get("has_monthly", False) else 0,
                    1 if equipment_data.get("has_annual", False) else 0,
                    equipment_data.get("status", "Active"),
                    equipment_data.get("priority", 0),
                ),
            )

            cursor.execute(
                """
                INSERT INTO audit_log
                (table_name, record_id, action, user_id, new_values, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "equipment",
                    equipment_data["bfm_no"],
                    "insert",
                    user_id,
                    str(equipment_data),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )

            self.conn.commit()
            return True, f"Equipment {equipment_data['bfm_no']} added successfully"
        except Exception as e:
            print(f"Error adding equipment: {e}")
            self.conn.rollback()
            return False, f"Error adding equipment: {str(e)}"

    def delete_equipment(
        self, bfm_no: str, user_id: str
    ) -> Tuple[bool, str]:
        try:
            cursor = self.conn.cursor()
            equipment = self.get_equipment_by_bfm(bfm_no)
            if not equipment:
                return False, f"Equipment {bfm_no} not found"

            cursor.execute(
                "DELETE FROM equipment WHERE bfm_equipment_no = ?", (bfm_no,)
            )

            cursor.execute(
                """
                INSERT INTO audit_log
                (table_name, record_id, action, user_id, old_values, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "equipment",
                    bfm_no,
                    "delete",
                    user_id,
                    str(equipment),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )

            self.conn.commit()
            return True, f"Equipment {bfm_no} deleted successfully"
        except Exception as e:
            print(f"Error deleting equipment: {e}")
            self.conn.rollback()
            return False, f"Error deleting equipment: {str(e)}"

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_equipment_statistics(self) -> Dict:
        cursor = self.conn.cursor()
        stats: Dict = {}

        def _count(sql, *params):
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return (row["COUNT(*)"] if isinstance(row, dict) else row[0]) or 0

        stats["total"]             = _count("SELECT COUNT(*) FROM equipment")
        stats["active"]            = _count("SELECT COUNT(*) FROM equipment WHERE status = ?", "Active")
        stats["run_to_failure"]    = _count("SELECT COUNT(*) FROM equipment WHERE status = ?", "Run to Failure")
        stats["missing"]           = _count("SELECT COUNT(*) FROM equipment WHERE status = ?", "Missing")

        monthly_eq = _count(
            "SELECT COUNT(*) FROM equipment WHERE monthly_pm = 1 AND status = ?", "Active"
        )
        annual_eq = _count(
            "SELECT COUNT(*) FROM equipment WHERE annual_pm = 1 AND status = ?", "Active"
        )

        stats["monthly_pm"]                 = monthly_eq
        stats["monthly_pm_annual_workload"]  = monthly_eq * 12
        stats["annual_pm"]                  = annual_eq
        stats["annual_pm_annual_workload"]   = annual_eq * 1

        return stats

    def get_equipment_requiring_attention(self) -> Dict[str, List[Dict]]:
        cursor = self.conn.cursor()
        today = date.today().isoformat()

        results: Dict[str, List[Dict]] = {
            "overdue_monthly": [],
            "overdue_annual":  [],
            "missing":         [],
            "no_pm_history":   [],
        }

        # Overdue monthly (>35 days) – SQLite date arithmetic
        cursor.execute(
            """
            SELECT bfm_equipment_no, description, last_monthly_pm,
                   CAST(julianday(?) - julianday(last_monthly_pm) AS INTEGER) AS days_overdue
            FROM equipment
            WHERE monthly_pm = 1
              AND status = 'Active'
              AND last_monthly_pm IS NOT NULL
              AND last_monthly_pm != ''
              AND julianday(?) - julianday(last_monthly_pm) > 35
            ORDER BY days_overdue DESC
            LIMIT 50
            """,
            (today, today),
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                results["overdue_monthly"].append(
                    {
                        "bfm_no":      row["bfm_equipment_no"],
                        "description": row["description"],
                        "last_pm":     row["last_monthly_pm"],
                        "days_overdue":row["days_overdue"],
                    }
                )
            else:
                results["overdue_monthly"].append(
                    {
                        "bfm_no":      row[0],
                        "description": row[1],
                        "last_pm":     row[2],
                        "days_overdue":row[3],
                    }
                )

        # Overdue annual (>370 days)
        cursor.execute(
            """
            SELECT bfm_equipment_no, description, last_annual_pm,
                   CAST(julianday(?) - julianday(last_annual_pm) AS INTEGER) AS days_overdue
            FROM equipment
            WHERE annual_pm = 1
              AND status = 'Active'
              AND last_annual_pm IS NOT NULL
              AND last_annual_pm != ''
              AND julianday(?) - julianday(last_annual_pm) > 370
            ORDER BY days_overdue DESC
            LIMIT 50
            """,
            (today, today),
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                results["overdue_annual"].append(
                    {
                        "bfm_no":      row["bfm_equipment_no"],
                        "description": row["description"],
                        "last_pm":     row["last_annual_pm"],
                        "days_overdue":row["days_overdue"],
                    }
                )
            else:
                results["overdue_annual"].append(
                    {
                        "bfm_no":      row[0],
                        "description": row[1],
                        "last_pm":     row[2],
                        "days_overdue":row[3],
                    }
                )

        # Missing
        cursor.execute(
            """
            SELECT bfm_equipment_no, description, location
            FROM equipment
            WHERE status = 'Missing'
            ORDER BY bfm_equipment_no
            """
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                results["missing"].append(row)
            else:
                results["missing"].append(
                    {"bfm_no": row[0], "description": row[1], "location": row[2]}
                )

        # No PM history
        cursor.execute(
            """
            SELECT bfm_equipment_no, description, monthly_pm, annual_pm
            FROM equipment
            WHERE status = 'Active'
              AND (monthly_pm = 1 OR annual_pm = 1)
              AND (
                  (monthly_pm = 1 AND (last_monthly_pm IS NULL OR last_monthly_pm = ''))
                  OR
                  (annual_pm  = 1 AND (last_annual_pm  IS NULL OR last_annual_pm  = ''))
              )
            ORDER BY bfm_equipment_no
            LIMIT 50
            """
        )
        for row in cursor.fetchall():
            if isinstance(row, dict):
                results["no_pm_history"].append(
                    {
                        "bfm_no":      row["bfm_equipment_no"],
                        "description": row["description"],
                        "has_monthly": bool(row["monthly_pm"]),
                        "has_annual":  bool(row["annual_pm"]),
                    }
                )
            else:
                results["no_pm_history"].append(
                    {
                        "bfm_no":      row[0],
                        "description": row[1],
                        "has_monthly": bool(row[2]) if row[2] is not None else False,
                        "has_annual":  bool(row[3]) if row[3] is not None else False,
                    }
                )

        return results
