"""
Database Utilities for AIT CMMS - SQLite Backend
Provides thread-safe SQLite connection management, user authentication,
audit logging, and transaction management.

Replaces the previous NEON/PostgreSQL implementation with a fully offline
SQLite solution. The external API is kept identical so all existing modules
continue to work without changes to their import statements.
"""

import sqlite3
import os
import threading
import hashlib
import time
from contextlib import contextmanager
from datetime import datetime


# ---------------------------------------------------------------------------
# Path to the SQLite database file
# ---------------------------------------------------------------------------
_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cmms_data.db")


# ---------------------------------------------------------------------------
# Row factory: returns rows that support BOTH integer-index AND string-key
# access, matching the behaviour of psycopg2's RealDictCursor while also
# being backward-compatible with tuple-style row[0] access used throughout
# the existing codebase.
# ---------------------------------------------------------------------------

class _Row(dict):
    """
    dict subclass returned by every SQLite query.

    Supports:
      row['column_name']   – named access (dict style)
      row[0]               – positional access (tuple style)
      len(row)             – number of columns
      isinstance(row, dict) → True
    """

    __slots__ = ("_keys",)

    def __init__(self, keys, values):
        super().__init__(zip(keys, values))
        object.__setattr__(self, "_keys", keys)

    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(object.__getattribute__(self, "_keys")[key])
        return super().__getitem__(key)

    def __len__(self):
        return len(object.__getattribute__(self, "_keys"))

    def get(self, key, default=None):
        if isinstance(key, int):
            keys = object.__getattribute__(self, "_keys")
            if 0 <= key < len(keys):
                return super().get(keys[key], default)
            return default
        return super().get(key, default)


def _dict_factory(cursor, row):
    """Return a _Row so callers can use row[0] or row['name'] interchangeably."""
    keys = [col[0] for col in cursor.description]
    return _Row(keys, row)


# ---------------------------------------------------------------------------
# DatabaseConnectionPool  (SQLite re-implementation, same public API)
# ---------------------------------------------------------------------------
class DatabaseConnectionPool:
    """
    Thread-safe SQLite connection manager.

    Keeps one connection per thread (thread-local storage).
    WAL journal mode is enabled for better concurrent read performance.
    The public API matches the original PostgreSQL pool so that all
    existing callers work without modification.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._initialized = False
            self._db_path = _DB_FILE
            self._local = threading.local()   # thread-local connections
            self._write_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public initialisation (called from AITCMMSSystem.__init__)
    # ------------------------------------------------------------------
    def initialize(self, db_config=None, min_conn=1, max_conn=10):
        """
        Initialise the SQLite backend.

        db_config is accepted but ignored (kept for API compatibility).
        The database file path is resolved automatically next to this script.
        """
        if not self._initialized:
            self._initialized = True
            # Touch the database to create it and enable WAL mode
            conn = self._make_connection()
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.close()
            print(f"SQLite database initialised: {self._db_path}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _make_connection(self):
        """Create a new SQLite connection with standard settings."""
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        conn.row_factory = _dict_factory
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _get_thread_connection(self):
        """Return (or create) the connection for the current thread."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._make_connection()
            self._local.conn = conn
        else:
            # Verify the connection is still alive
            try:
                conn.execute("SELECT 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self._make_connection()
                self._local.conn = conn
        return conn

    # ------------------------------------------------------------------
    # Public connection API (matches original pool API)
    # ------------------------------------------------------------------
    def get_connection(self, max_retries=3):
        """Return a live SQLite connection for the current thread."""
        for attempt in range(max_retries):
            try:
                return self._get_thread_connection()
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (2 ** attempt))
                else:
                    raise Exception(
                        f"Failed to get SQLite connection after {max_retries} attempts: {e}"
                    )

    def return_connection(self, conn):
        """
        'Return' a connection to the pool.
        For SQLite thread-local connections we simply leave it open.
        """
        # Nothing to do – thread-local connection stays alive for reuse.
        pass

    def close_all(self):
        """Close the connection on the current thread."""
        conn = getattr(self._local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    # ------------------------------------------------------------------
    # Context-manager cursor (matches original get_cursor() API)
    # ------------------------------------------------------------------
    @contextmanager
    def get_cursor(self, commit=True):
        """
        Context manager that yields a dict-cursor and optionally commits.

        Usage:
            with db_pool.get_cursor() as cursor:
                cursor.execute("SELECT * FROM equipment")
                data = cursor.fetchall()
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# OptimisticConcurrencyControl  (adapted for SQLite)
# ---------------------------------------------------------------------------
class OptimisticConcurrencyControl:
    """Version-based optimistic locking (SQLite edition)."""

    @staticmethod
    def check_version(cursor, table, record_id, expected_version, id_column="id"):
        cursor.execute(
            f"SELECT version FROM {table} WHERE {id_column} = ?",
            (record_id,),
        )
        result = cursor.fetchone()

        if not result:
            return False, None, f"Record not found in {table}"

        current_version = result["version"] if isinstance(result, dict) else result[0]

        if current_version != expected_version:
            return False, current_version, (
                f"Conflict detected: Record was modified by another user. "
                f"Expected version {expected_version}, found {current_version}."
            )

        return True, current_version, "Version check passed"

    @staticmethod
    def increment_version(cursor, table, record_id, id_column="id"):
        cursor.execute(
            f"""
            UPDATE {table}
            SET version = version + 1,
                updated_date = CURRENT_TIMESTAMP
            WHERE {id_column} = ?
            """,
            (record_id,),
        )


# ---------------------------------------------------------------------------
# AuditLogger  (adapted for SQLite – uses ? placeholders)
# ---------------------------------------------------------------------------
class AuditLogger:
    """Logs all database changes for audit trail."""

    @staticmethod
    def log(
        cursor,
        user_name,
        action,
        table_name,
        record_id,
        old_values=None,
        new_values=None,
        notes=None,
    ):
        cursor.execute(
            """
            INSERT INTO audit_log
            (user_name, action, table_name, record_id, old_values, new_values, notes, action_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                user_name,
                action,
                table_name,
                record_id,
                str(old_values),
                str(new_values),
                notes,
            ),
        )


# ---------------------------------------------------------------------------
# UserManager  (adapted for SQLite – uses ? placeholders)
# ---------------------------------------------------------------------------
class UserManager:
    """Manages user authentication and sessions."""

    @staticmethod
    def hash_password(password):
        return hashlib.sha256(password.encode()).hexdigest()

    @staticmethod
    def verify_password(password, hashed_password):
        return UserManager.hash_password(password) == hashed_password

    @staticmethod
    def authenticate(cursor, username, password):
        cursor.execute(
            """
            SELECT id, username, full_name, role, password_hash, is_active
            FROM users
            WHERE username = ?
            """,
            (username,),
        )
        user = cursor.fetchone()

        if not user:
            return None

        if isinstance(user, (tuple, list)):
            user = {
                "id": user[0],
                "username": user[1],
                "full_name": user[2],
                "role": user[3],
                "password_hash": user[4],
                "is_active": user[5],
            }
        elif not isinstance(user, dict):
            user = dict(user)

        if not user["is_active"]:
            return None

        if not UserManager.verify_password(password, user["password_hash"]):
            return None

        del user["password_hash"]
        return user

    @staticmethod
    def change_password(cursor, username, current_password, new_password):
        cursor.execute(
            """
            SELECT id, password_hash, is_active
            FROM users
            WHERE username = ?
            """,
            (username,),
        )
        user = cursor.fetchone()

        if not user:
            return False, "User not found"

        if isinstance(user, (tuple, list)):
            user = {"id": user[0], "password_hash": user[1], "is_active": user[2]}
        elif not isinstance(user, dict):
            user = dict(user)

        if not user["is_active"]:
            return False, "Account is not active"

        if not UserManager.verify_password(current_password, user["password_hash"]):
            return False, "Current password is incorrect"

        new_hash = UserManager.hash_password(new_password)
        cursor.execute(
            """
            UPDATE users
            SET password_hash = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (new_hash, user["id"]),
        )
        return True, "Password changed successfully"

    @staticmethod
    def create_session(cursor, user_id, username):
        cursor.execute(
            """
            INSERT INTO user_sessions
            (user_id, username, login_time, last_activity, is_active)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
            """,
            (user_id, username),
        )
        # SQLite uses lastrowid instead of RETURNING
        return cursor.lastrowid

    @staticmethod
    def update_session_activity(cursor, session_id):
        cursor.execute(
            """
            UPDATE user_sessions
            SET last_activity = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (session_id,),
        )

    @staticmethod
    def end_session(cursor, session_id):
        cursor.execute(
            """
            UPDATE user_sessions
            SET logout_time = CURRENT_TIMESTAMP, is_active = 0
            WHERE id = ?
            """,
            (session_id,),
        )

    @staticmethod
    def get_active_sessions(cursor):
        cursor.execute(
            """
            SELECT s.id, s.user_id, s.username, u.full_name, u.role,
                   s.login_time, s.last_activity
            FROM user_sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.is_active = 1
            ORDER BY s.login_time DESC
            """
        )
        return cursor.fetchall()


# ---------------------------------------------------------------------------
# TransactionManager  (adapted for SQLite)
# ---------------------------------------------------------------------------
class TransactionManager:
    """Manages database transactions with retry logic."""

    @staticmethod
    @contextmanager
    def transaction(pool, max_retries=3):
        conn = pool.get_connection()
        cursor = conn.cursor()
        retries = 0

        while retries < max_retries:
            try:
                yield cursor
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                # SQLite equivalent of deadlock / locked database
                conn.rollback()
                retries += 1
                if retries >= max_retries:
                    raise Exception(
                        f"Transaction failed after {max_retries} retries: {e}"
                    )
                print(f"Database locked, retrying... (attempt {retries}/{max_retries})")
                time.sleep(0.1 * retries)
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise e
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Global pool instance (same name as before)
# ---------------------------------------------------------------------------
db_pool = DatabaseConnectionPool()
