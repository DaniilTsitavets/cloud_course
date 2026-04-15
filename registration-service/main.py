import pyodbc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

DB_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:cloud2026.database.windows.net,1433;"
    "DATABASE=pr2;"
    "UID=pasinozavr;"
    "PWD=61YcGTqd;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)


def get_conn():
    return pyodbc.connect(DB_CONN_STR)


def init_db():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Daniil_Tsitavets_registration')
                EXEC('CREATE SCHEMA Daniil_Tsitavets_registration')
        """)

        # Drop old tables if they have the wrong schema (int PKs instead of UUIDs)
        cur.execute("""
            IF EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'Daniil_Tsitavets_registration'
                  AND TABLE_NAME   = 'registrations'
                  AND COLUMN_NAME  = 'registerID'
            ) DROP TABLE Daniil_Tsitavets_registration.registrations
        """)
        cur.execute("""
            IF EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'Daniil_Tsitavets_registration'
                  AND TABLE_NAME   = 'users'
                  AND COLUMN_NAME  = 'UserID'
            ) DROP TABLE Daniil_Tsitavets_registration.users
        """)
        conn.commit()

        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('Daniil_Tsitavets_registration.users'))
            CREATE TABLE Daniil_Tsitavets_registration.users (
                user_id    UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                name       NVARCHAR(100) NOT NULL,
                email      NVARCHAR(100) NOT NULL UNIQUE,
                created_at DATETIME2 DEFAULT GETDATE()
            )
        """)

        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('Daniil_Tsitavets_registration.registrations'))
            CREATE TABLE Daniil_Tsitavets_registration.registrations (
                registration_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                user_id         UNIQUEIDENTIFIER NOT NULL REFERENCES Daniil_Tsitavets_registration.users(user_id),
                class_id        UNIQUEIDENTIFIER NOT NULL,
                status          NVARCHAR(20) NOT NULL DEFAULT 'PENDING',
                registered_at   DATETIME2 DEFAULT GETDATE(),
                cancelled_at    DATETIME2 NULL
            )
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM Daniil_Tsitavets_registration.users")
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO Daniil_Tsitavets_registration.users (name, email) VALUES
                (N'Alice Johnson',  N'alice@example.com'),
                (N'Bob Smith',      N'bob@example.com'),
                (N'Carol White',    N'carol@example.com'),
                (N'David Brown',    N'david@example.com'),
                (N'Eva Martinez',   N'eva@example.com')
            """)
            conn.commit()

            # Grab some user_ids for stub registrations
            cur.execute("SELECT TOP 3 user_id FROM Daniil_Tsitavets_registration.users")
            users = [r[0] for r in cur.fetchall()]

            # Use a fixed dummy class_id (will link to real ones via ChefService in later tasks)
            dummy_class = "00000000-0000-0000-0000-000000000001"
            for uid in users:
                cur.execute("""
                    INSERT INTO Daniil_Tsitavets_registration.registrations (user_id, class_id, status)
                    VALUES (?, ?, N'CONFIRMED')
                """, uid, dummy_class)
            conn.commit()
            print("Stub data inserted.")

    print("RegistrationService DB initialized.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="RegistrationService", lifespan=lifespan)


def row_to_dict(cursor, row):
    return {col[0]: val for col, val in zip(cursor.description, row)}


class RegistrationIn(BaseModel):
    user_id: str
    class_id: str


@app.get("/users")
def list_users():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM Daniil_Tsitavets_registration.users ORDER BY created_at DESC")
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]


@app.get("/registrations")
def list_registrations():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.*, u.name AS user_name, u.email AS user_email
            FROM Daniil_Tsitavets_registration.registrations r
            JOIN Daniil_Tsitavets_registration.users u ON u.user_id = r.user_id
            ORDER BY r.registered_at DESC
        """)
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]


@app.post("/registrations", status_code=201)
def create_registration(body: RegistrationIn):
    with get_conn() as conn:
        cur = conn.cursor()

        # Check user exists
        cur.execute("SELECT 1 FROM Daniil_Tsitavets_registration.users WHERE user_id = ?", body.user_id)
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="User not found")

        cur.execute("""
            INSERT INTO Daniil_Tsitavets_registration.registrations (user_id, class_id, status)
            OUTPUT INSERTED.registration_id
            VALUES (?, ?, N'PENDING')
        """, body.user_id, body.class_id)
        new_id = cur.fetchone()[0]
        conn.commit()
        return {"registration_id": str(new_id), "status": "PENDING"}
