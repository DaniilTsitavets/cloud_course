import json
import logging
import os
import pyodbc
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.servicebus import ServiceBusClient, ServiceBusMessage

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

SB_SEND_CONN_STR = os.environ["SB_SEND_CONN_STR"]
SB_QUEUE_NAME = os.environ["SB_QUEUE_NAME"]


def publish_registration_completed(registration_id: str, user_id: str, class_id: str):
    payload = json.dumps({
        "event": "RegistrationCompleted",
        "registration_id": registration_id,
        "user_id": user_id,
        "class_id": class_id,
    })
    with ServiceBusClient.from_connection_string(SB_SEND_CONN_STR) as client:
        with client.get_queue_sender(queue_name=SB_QUEUE_NAME) as sender:
            sender.send_messages(ServiceBusMessage(payload))
    logger.info("[ServiceBus] Published RegistrationCompleted: registration_id=%s", registration_id)

DB_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={os.environ['DB_SERVER']};"
    f"DATABASE={os.environ['DB_NAME']};"
    f"UID={os.environ['DB_USER']};"
    f"PWD={os.environ['DB_PASSWORD']};"
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
            logger.info("Stub data inserted.")

    logger.info("RegistrationService DB initialized.")


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

    publish_registration_completed(str(new_id), body.user_id, body.class_id)
    return {"registration_id": str(new_id), "status": "PENDING"}
