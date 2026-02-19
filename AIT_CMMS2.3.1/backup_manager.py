"""
Backup Manager Module – SQLite edition
Handles automated database backups for the SQLite CMMS database.
- JSON+gzip backup format (platform-independent)
- Backup rotation and retention
- Restore capabilities
"""

import os
import gzip
import json
import sqlite3
import shutil
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import hashlib

BACKUP_FILE_EXTENSION = ".cmmsbackup"
_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cmms_data.db")


def _serialize_value(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, bytes):
        return {'_t': 'bytes', 'v': v.hex()}
    return str(v)


def _deserialize_value(v):
    if isinstance(v, dict) and '_t' in v:
        t, val = v['_t'], v['v']
        if t == 'bytes':
            return bytes.fromhex(val)
    return v


class BackupManager:
    """Manages SQLite database backups"""

    def __init__(self, pool=None, backup_dir: Optional[str] = None):
        self.pool = pool
        self.db_path = _DB_FILE
        if backup_dir:
            self.backup_dir = Path(backup_dir)
        else:
            self.backup_dir = Path(os.path.dirname(os.path.abspath(__file__))) / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _get_connection(self):
        """Get a direct SQLite connection"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_all_tables(self, conn) -> List[str]:
        """Get list of all user tables in the database"""
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        return [row[0] for row in cur.fetchall()]

    def create_backup(self, backup_name: Optional[str] = None, notes: str = "") -> Tuple[bool, str]:
        """
        Create a database backup as a gzip-compressed JSON file.

        Returns (success, message_or_filepath)
        """
        with self._lock:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                if not backup_name:
                    backup_name = f"cmms_backup_{timestamp}"

                backup_path = self.backup_dir / f"{backup_name}{BACKUP_FILE_EXTENSION}"

                conn = self._get_connection()
                try:
                    tables = self._get_all_tables(conn)
                    backup_data = {
                        "version": "2.3.1",
                        "backup_date": datetime.now().isoformat(),
                        "notes": notes,
                        "tables": {}
                    }

                    for table in tables:
                        cur = conn.cursor()
                        cur.execute(f"SELECT * FROM {table}")
                        columns = [desc[0] for desc in cur.description]
                        rows = []
                        for row in cur.fetchall():
                            row_dict = {}
                            for i, col in enumerate(columns):
                                row_dict[col] = _serialize_value(row[i])
                            rows.append(row_dict)
                        backup_data["tables"][table] = {
                            "columns": columns,
                            "rows": rows,
                            "count": len(rows)
                        }

                    # Also create a raw SQLite file copy
                    raw_backup_path = str(backup_path) + ".db"
                    backup_conn = sqlite3.connect(raw_backup_path)
                    conn.backup(backup_conn)
                    backup_conn.close()

                finally:
                    conn.close()

                # Write compressed JSON backup
                json_data = json.dumps(backup_data, ensure_ascii=False, default=str)
                with gzip.open(str(backup_path), 'wt', encoding='utf-8') as f:
                    f.write(json_data)

                size = backup_path.stat().st_size
                total_rows = sum(t["count"] for t in backup_data["tables"].values())
                print(f"Backup created: {backup_path.name} ({size:,} bytes, {total_rows:,} rows)")
                return True, str(backup_path)

            except Exception as e:
                print(f"Backup failed: {e}")
                return False, str(e)

    def restore_backup(self, backup_path: str) -> Tuple[bool, str]:
        """
        Restore the database from a backup file.

        Returns (success, message)
        """
        with self._lock:
            try:
                path = Path(backup_path)
                if not path.exists():
                    return False, f"Backup file not found: {backup_path}"

                # Try raw .db backup first
                raw_path = str(path) + ".db"
                if os.path.exists(raw_path):
                    shutil.copy2(raw_path, self.db_path)
                    print(f"Database restored from: {raw_path}")
                    return True, "Database restored successfully from SQLite backup."

                # Otherwise restore from JSON backup
                with gzip.open(str(path), 'rt', encoding='utf-8') as f:
                    backup_data = json.load(f)

                conn = self._get_connection()
                try:
                    cur = conn.cursor()
                    cur.execute("PRAGMA foreign_keys=OFF")

                    for table_name, table_data in backup_data["tables"].items():
                        cur.execute(f"DELETE FROM {table_name}")

                        columns = table_data["columns"]
                        placeholders = ",".join(["?" for _ in columns])
                        col_names = ",".join(columns)

                        for row_dict in table_data["rows"]:
                            values = [_deserialize_value(row_dict.get(col)) for col in columns]
                            cur.execute(
                                f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})",
                                values
                            )

                    cur.execute("PRAGMA foreign_keys=ON")
                    conn.commit()
                finally:
                    conn.close()

                print(f"Database restored from JSON backup: {path.name}")
                return True, "Database restored successfully."

            except Exception as e:
                print(f"Restore failed: {e}")
                return False, str(e)

    def list_backups(self) -> List[Dict]:
        """List all available backups"""
        backups = []
        for f in self.backup_dir.glob(f"*{BACKUP_FILE_EXTENSION}"):
            try:
                stat = f.stat()
                backups.append({
                    "name": f.stem,
                    "path": str(f),
                    "size": stat.st_size,
                    "created": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "size_mb": round(stat.st_size / 1024 / 1024, 2)
                })
            except Exception:
                pass
        backups.sort(key=lambda x: x["created"], reverse=True)
        return backups

    def delete_backup(self, backup_path: str) -> Tuple[bool, str]:
        """Delete a backup file"""
        try:
            path = Path(backup_path)
            if path.exists():
                path.unlink()
            raw_path = Path(str(path) + ".db")
            if raw_path.exists():
                raw_path.unlink()
            return True, f"Backup deleted: {path.name}"
        except Exception as e:
            return False, str(e)

    def cleanup_old_backups(self, keep_count: int = 10) -> int:
        """Keep only the most recent N backups, delete the rest."""
        backups = self.list_backups()
        deleted = 0
        for backup in backups[keep_count:]:
            success, _ = self.delete_backup(backup["path"])
            if success:
                deleted += 1
        return deleted

    def get_backup_info(self, backup_path: str) -> Optional[Dict]:
        """Get metadata from a backup file without restoring it."""
        try:
            with gzip.open(backup_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
            info = {
                "version": data.get("version"),
                "backup_date": data.get("backup_date"),
                "notes": data.get("notes"),
                "tables": {}
            }
            for tname, tdata in data.get("tables", {}).items():
                info["tables"][tname] = tdata.get("count", 0)
            return info
        except Exception as e:
            print(f"Could not read backup info: {e}")
            return None
