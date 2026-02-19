"""
SQLite Schema Initialisation for AIT CMMS
==========================================
Creates all database tables, indexes, and seeds initial data
(default users, KPI definitions) when the application starts
for the first time on a machine.

Run automatically from the main application; can also be executed
directly as a standalone script for fresh installs:

    python sqlite_schema_init.py
"""

import sqlite3
import hashlib
import os

_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cmms_data.db")


def get_connection(db_path=None):
    path = db_path or _DB_FILE
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ---------------------------------------------------------------------------
# DDL – core tables
# ---------------------------------------------------------------------------

def create_core_tables(conn):
    cur = conn.cursor()

    # ------------------------------------------------------------------
    # users
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT    NOT NULL UNIQUE,
            password_hash TEXT    NOT NULL,
            full_name     TEXT,
            role          TEXT    DEFAULT 'Technician',
            is_active     INTEGER DEFAULT 1,
            created_date  TEXT    DEFAULT CURRENT_TIMESTAMP,
            updated_date  TEXT
        )
    """)

    # ------------------------------------------------------------------
    # user_sessions
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            username      TEXT,
            login_time    TEXT    DEFAULT CURRENT_TIMESTAMP,
            last_activity TEXT    DEFAULT CURRENT_TIMESTAMP,
            logout_time   TEXT,
            is_active     INTEGER DEFAULT 1
        )
    """)

    # ------------------------------------------------------------------
    # audit_log
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name        TEXT,
            action           TEXT,
            table_name       TEXT,
            record_id        TEXT,
            old_values       TEXT,
            new_values       TEXT,
            notes            TEXT,
            action_timestamp TEXT    DEFAULT CURRENT_TIMESTAMP,
            -- legacy column names used by equipment_manager.py
            user_id          TEXT,
            timestamp        TEXT    DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # equipment
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS equipment (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no  TEXT    NOT NULL UNIQUE,
            sap_no            TEXT,
            description       TEXT,
            tool_id           TEXT,
            location          TEXT,
            master_lin        TEXT,
            weekly_pm         INTEGER DEFAULT 0,
            monthly_pm        INTEGER DEFAULT 0,
            six_month_pm      INTEGER DEFAULT 0,
            annual_pm         INTEGER DEFAULT 0,
            last_weekly_pm    TEXT,
            last_monthly_pm   TEXT,
            last_six_month_pm TEXT,
            last_annual_pm    TEXT,
            next_annual_pm    TEXT,
            status            TEXT    DEFAULT 'Active',
            priority          INTEGER DEFAULT 0,
            pm_qty            TEXT,
            picture_1         BLOB,
            picture_2         BLOB,
            picture_1_path    TEXT,
            picture_2_path    TEXT,
            custom_pm_start_date TEXT,
            notes             TEXT,
            version           INTEGER DEFAULT 1,
            created_date      TEXT    DEFAULT CURRENT_TIMESTAMP,
            updated_date      TEXT
        )
    """)

    # ------------------------------------------------------------------
    # pm_completions
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pm_completions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no TEXT    NOT NULL REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            pm_type          TEXT    NOT NULL,
            technician_name  TEXT,
            completion_date  TEXT    NOT NULL,
            labor_hours      REAL    DEFAULT 0,
            notes            TEXT,
            special_equipment TEXT,
            created_date     TEXT    DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # weekly_pm_schedules
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_pm_schedules (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no  TEXT    NOT NULL REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            pm_type           TEXT    NOT NULL,
            assigned_technician TEXT,
            week_start_date   TEXT    NOT NULL,
            scheduled_date    TEXT,
            status            TEXT    DEFAULT 'Scheduled',
            completed_date    TEXT,
            notes             TEXT,
            created_date      TEXT    DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # corrective_maintenance
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS corrective_maintenance (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            cm_number           TEXT    UNIQUE,
            bfm_equipment_no    TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE SET NULL,
            description         TEXT,
            priority            TEXT,
            status              TEXT    DEFAULT 'Open',
            assigned_technician TEXT,
            reported_date       TEXT    DEFAULT CURRENT_TIMESTAMP,
            created_date        TEXT    DEFAULT CURRENT_TIMESTAMP,
            closed_date         TEXT,
            labor_hours         REAL    DEFAULT 0,
            notes               TEXT,
            root_cause          TEXT,
            corrective_action   TEXT,
            version             INTEGER DEFAULT 1
        )
    """)

    # ------------------------------------------------------------------
    # cm_parts_requests
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cm_parts_requests (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            cm_number      TEXT    REFERENCES corrective_maintenance(cm_number) ON DELETE CASCADE,
            part_number    TEXT,
            model_number   TEXT,
            requested_by   TEXT,
            requested_date TEXT    DEFAULT CURRENT_TIMESTAMP,
            email_sent     INTEGER DEFAULT 0,
            notes          TEXT
        )
    """)

    # ------------------------------------------------------------------
    # equipment_missing_parts  (EMP)
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS equipment_missing_parts (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_number               TEXT    UNIQUE,
            bfm_equipment_no         TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            status                   TEXT    DEFAULT 'Open',
            missing_parts_description TEXT,
            created_date             TEXT    DEFAULT CURRENT_TIMESTAMP,
            resolved_date            TEXT
        )
    """)

    # ------------------------------------------------------------------
    # cannot_find_assets
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cannot_find_assets (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            status           TEXT    DEFAULT 'Missing',
            search_status    TEXT,
            found_date       TEXT,
            notes            TEXT,
            created_date     TEXT    DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # deactivated_assets
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deactivated_assets (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            reason           TEXT,
            deactivated_by   TEXT,
            deactivated_date TEXT    DEFAULT CURRENT_TIMESTAMP,
            notes            TEXT
        )
    """)

    # ------------------------------------------------------------------
    # run_to_failure_assets
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS run_to_failure_assets (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            justification    TEXT,
            approved_by      TEXT,
            start_date       TEXT    DEFAULT CURRENT_TIMESTAMP,
            labor_hours      REAL    DEFAULT 0,
            notes            TEXT
        )
    """)

    # ------------------------------------------------------------------
    # mro_inventory
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mro_inventory (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            name              TEXT    NOT NULL,
            part_number       TEXT    UNIQUE NOT NULL,
            model_number      TEXT,
            equipment         TEXT,
            engineering_system TEXT,
            unit_of_measure   TEXT,
            quantity_in_stock REAL    DEFAULT 0,
            unit_price        REAL    DEFAULT 0,
            minimum_stock     REAL    DEFAULT 0,
            supplier          TEXT,
            location          TEXT,
            rack              TEXT,
            row               TEXT,
            bin               TEXT,
            picture_1_path    TEXT,
            picture_2_path    TEXT,
            picture_1_data    BLOB,
            picture_2_data    BLOB,
            notes             TEXT,
            last_updated      TEXT    DEFAULT CURRENT_TIMESTAMP,
            created_date      TEXT    DEFAULT CURRENT_TIMESTAMP,
            status            TEXT    DEFAULT 'Active'
        )
    """)

    # ------------------------------------------------------------------
    # mro_stock_transactions
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mro_stock_transactions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            part_number      TEXT    REFERENCES mro_inventory(part_number) ON DELETE CASCADE,
            transaction_type TEXT,
            quantity_changed REAL,
            quantity_after   REAL,
            reason           TEXT,
            performed_by     TEXT,
            transaction_date TEXT    DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # cm_parts_used
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cm_parts_used (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cm_number    TEXT    REFERENCES corrective_maintenance(cm_number) ON DELETE CASCADE,
            part_number  TEXT    REFERENCES mro_inventory(part_number) ON DELETE SET NULL,
            quantity_used REAL   DEFAULT 1,
            used_date    TEXT    DEFAULT CURRENT_TIMESTAMP,
            notes        TEXT
        )
    """)

    # ------------------------------------------------------------------
    # equipment_manuals
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS equipment_manuals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            manual_path      TEXT,
            document_name    TEXT,
            document_revision TEXT,
            uploaded_by      TEXT,
            upload_date      TEXT    DEFAULT CURRENT_TIMESTAMP,
            notes            TEXT
        )
    """)

    # ------------------------------------------------------------------
    # pm_templates
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pm_templates (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            bfm_equipment_no TEXT    REFERENCES equipment(bfm_equipment_no) ON DELETE CASCADE,
            pm_type          TEXT    NOT NULL,
            template_content TEXT,
            created_by       TEXT,
            created_date     TEXT    DEFAULT CURRENT_TIMESTAMP,
            updated_date     TEXT
        )
    """)

    conn.commit()
    print("Core tables created.")


# ---------------------------------------------------------------------------
# DDL – KPI tables
# ---------------------------------------------------------------------------

def create_kpi_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_definitions (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            function_code     TEXT    NOT NULL,
            kpi_name          TEXT    NOT NULL UNIQUE,
            description       TEXT,
            formula           TEXT,
            acceptance_criteria TEXT,
            frequency         TEXT,
            data_source       TEXT,
            is_active         INTEGER DEFAULT 1,
            created_date      TEXT    DEFAULT CURRENT_TIMESTAMP,
            updated_date      TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_manual_data (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            kpi_name           TEXT    NOT NULL REFERENCES kpi_definitions(kpi_name) ON DELETE CASCADE,
            measurement_period TEXT    NOT NULL,
            data_field         TEXT    NOT NULL,
            data_value         REAL,
            data_text          TEXT,
            notes              TEXT,
            entered_by         TEXT,
            entered_date       TEXT    DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(kpi_name, measurement_period, data_field)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_results (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            kpi_name           TEXT    NOT NULL REFERENCES kpi_definitions(kpi_name) ON DELETE CASCADE,
            measurement_period TEXT    NOT NULL,
            calculated_value   REAL,
            calculated_text    TEXT,
            target_value       REAL,
            meets_criteria     INTEGER,
            calculation_date   TEXT    DEFAULT CURRENT_TIMESTAMP,
            calculated_by      TEXT,
            notes              TEXT,
            UNIQUE(kpi_name, measurement_period)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS kpi_exports (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            export_date   TEXT    DEFAULT CURRENT_TIMESTAMP,
            export_period TEXT,
            export_type   TEXT,
            exported_by   TEXT,
            file_name     TEXT,
            kpi_count     INTEGER,
            notes         TEXT
        )
    """)

    conn.commit()
    print("KPI tables created.")


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

def create_indexes(conn):
    cur = conn.cursor()
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_equipment_bfm      ON equipment(bfm_equipment_no)",
        "CREATE INDEX IF NOT EXISTS idx_equipment_status   ON equipment(status)",
        "CREATE INDEX IF NOT EXISTS idx_equipment_priority ON equipment(priority)",
        "CREATE INDEX IF NOT EXISTS idx_pm_completions_bfm ON pm_completions(bfm_equipment_no)",
        "CREATE INDEX IF NOT EXISTS idx_pm_completions_date ON pm_completions(completion_date)",
        "CREATE INDEX IF NOT EXISTS idx_weekly_sched_bfm   ON weekly_pm_schedules(bfm_equipment_no)",
        "CREATE INDEX IF NOT EXISTS idx_weekly_sched_week  ON weekly_pm_schedules(week_start_date)",
        "CREATE INDEX IF NOT EXISTS idx_weekly_sched_status ON weekly_pm_schedules(status)",
        "CREATE INDEX IF NOT EXISTS idx_cm_bfm             ON corrective_maintenance(bfm_equipment_no)",
        "CREATE INDEX IF NOT EXISTS idx_cm_status          ON corrective_maintenance(status)",
        "CREATE INDEX IF NOT EXISTS idx_mro_part           ON mro_inventory(part_number)",
        "CREATE INDEX IF NOT EXISTS idx_mro_status         ON mro_inventory(status)",
        "CREATE INDEX IF NOT EXISTS idx_audit_table        ON audit_log(table_name)",
    ]
    for sql in indexes:
        cur.execute(sql)
    conn.commit()
    print("Indexes created.")


# ---------------------------------------------------------------------------
# Seed data – default users and KPI definitions
# ---------------------------------------------------------------------------

def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def seed_default_users(conn):
    cur = conn.cursor()
    defaults = [
        ("admin",     _hash("admin123"),     "Administrator",         "Manager"),
        ("manager",   _hash("manager123"),   "Maintenance Manager",   "Manager"),
        ("tech1",     _hash("tech1"),         "Technician 1",          "Technician"),
        ("tech2",     _hash("tech2"),         "Technician 2",          "Technician"),
        ("parts",     _hash("parts123"),      "Parts Coordinator",     "Parts Coordinator"),
    ]
    for username, pw_hash, full_name, role in defaults:
        cur.execute(
            """
            INSERT OR IGNORE INTO users (username, password_hash, full_name, role)
            VALUES (?, ?, ?, ?)
            """,
            (username, pw_hash, full_name, role),
        )
    conn.commit()
    print("Default users seeded.")


def seed_kpi_definitions(conn):
    cur = conn.cursor()
    kpi_data = [
        ("F1",             "FR1",
         "Injury frequency rate",
         "number Accident (with sick leave > 24h) / nb hours worked x 1,000,000",
         "0", "Monthly", "Supplier Own Record"),

        ("F1",             "Near Miss",
         "Near miss reports",
         "N/A", "Raise when near miss identified", "Monthly", "Supplier own record"),

        ("F2.1",           "TTR (Time to Repair) Adherence",
         "Time to Repair Adherence",
         "(number of maintenance Andons within time / number of maintenance received) x 100%",
         "P1 asset <2hours P2 asset <4hours", "Monthly", "Maintenance provider"),

        ("F2.1",           "MTBF Mean Time Between Failure",
         "Average time between asset breakdown",
         "MTBF= Total operating time / Number of WO with disruption",
         "P1 assets >80hours P2 assets >40hours", "Monthly", "Maintenance provider"),

        ("F2.1",           "Technical Availability Adherence",
         "Technical availability percentage",
         "(nb assets with TA reached / nb assets) x 100%",
         "P1 Critical assets >95%", "Monthly", "Maintenance provider"),

        ("F2.1",           "MRT (Mean Response Time)",
         "Mean time from request to response",
         "Sum(response time) / number of WO with disruption",
         "P1 <15 min P2 <1 hr P3 <3 hr", "Monthly", "Maintenance provider"),

        ("F2.1",           "WO opened vs WO closed",
         "WOs opened vs closed in a month",
         "number of WO open vs WO closed",
         "No >40 open WO", "Monthly", "Maintenance provider"),

        ("F2.1",           "WO Backlog",
         "Number of WO open at a point in time",
         "Total of WO open",
         "<10% of the WO raised in a month", "Monthly", "Maintenance provider"),

        ("F2.1",           "WO age profile",
         "Age of open WO",
         "Age of work order",
         "Nb of WO to exceed 60 days", "Monthly", "Maintenance provider"),

        ("F2.2",           "Preventive Maintenance Adherence",
         "Adherence to scheduled PM WOs",
         "(number of WO completed / number of WO scheduled) x 100%",
         ">95%", "Monthly", "Maintenance provider"),

        ("F4.2",           "Top Breakdown",
         "Top Break Down Analysis",
         "NA",
         "Pareto of failure on critical assets", "Monthly", "Maintenance provider"),

        ("F4.3",           "Purchaser Monthly process Confirmation",
         "Monthly go look see routine result",
         "NA",
         "Score of >90%", "Monthly", "Maintenance provider"),

        ("F4 and all",     "Purchaser satisfaction",
         "Customer Satisfaction Survey",
         "Yearly satisfaction survey",
         "1/year", "Quarterly", "local"),

        ("All Functions",  "Non Conformances raised",
         "Number of Non Conformances raised",
         "NC Count", "0", "Monthly", "Local"),

        ("All functions",  "Non Conformances closed",
         "Number of Non Conformances closed within timeframe",
         "NC closed before due date count",
         "100% closed in contractual timeframe", "Monthly", "Contractual"),

        ("F7.1",           "Mean Time to Deliver a Quote",
         "Mean time to deliver a quote",
         "Average lead-time for each request",
         "<48 hours", "Monthly", "Maintenance provider"),

        ("F7.1",           "Purchaser Satisfaction Survey",
         "Annual satisfaction survey score",
         "Survey score",
         ">=90%", "Yearly", "local"),
    ]
    for kpi in kpi_data:
        cur.execute(
            """
            INSERT OR IGNORE INTO kpi_definitions
            (function_code, kpi_name, description, formula,
             acceptance_criteria, frequency, data_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            kpi,
        )
    conn.commit()
    print(f"KPI definitions seeded ({len(kpi_data)} entries).")


# ---------------------------------------------------------------------------
# Master initialisation entry point
# ---------------------------------------------------------------------------

def initialise_database(db_path=None):
    """
    Create all tables and seed data if they do not already exist.
    Safe to call every time the application starts.
    """
    conn = get_connection(db_path)
    try:
        create_core_tables(conn)
        create_kpi_tables(conn)
        create_indexes(conn)
        seed_default_users(conn)
        seed_kpi_definitions(conn)
        print("SQLite database initialisation complete.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    initialise_database()
    print(f"\nDatabase file: {_DB_FILE}")
