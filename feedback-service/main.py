import asyncio
import json
import os
import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional
from azure.servicebus.aio import ServiceBusClient as AsyncServiceBusClient

load_dotenv()

SB_LISTEN_CONN_STR = os.environ["SB_LISTEN_CONN_STR"]
SB_QUEUE_NAME = os.environ["SB_QUEUE_NAME"]
POLL_INTERVAL_SECONDS = 10


async def poll_service_bus():
    print(f"[ServiceBus] Poller started — checking every {POLL_INTERVAL_SECONDS}s")
    while True:
        try:
            async with AsyncServiceBusClient.from_connection_string(SB_LISTEN_CONN_STR) as client:
                async with client.get_queue_receiver(
                    queue_name=SB_QUEUE_NAME, max_wait_time=5
                ) as receiver:
                    async for msg in receiver:
                        try:
                            data = json.loads(str(msg))
                            print(f"[ServiceBus] Received event: {data}")
                        except json.JSONDecodeError:
                            print(f"[ServiceBus] Non-JSON message: {msg}")
                        await receiver.complete_message(msg)
        except Exception as exc:
            print(f"[ServiceBus] Poller error: {exc}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

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
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Daniil_Tsitavets_feedback')
                EXEC('CREATE SCHEMA Daniil_Tsitavets_feedback')
        """)

        conn.commit()

        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('Daniil_Tsitavets_feedback.feedbacks'))
            CREATE TABLE Daniil_Tsitavets_feedback.feedbacks (
                feedback_id     UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                registration_id UNIQUEIDENTIFIER NOT NULL,
                user_id         UNIQUEIDENTIFIER NOT NULL,
                class_id        UNIQUEIDENTIFIER NOT NULL,
                rating          INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
                comment         NVARCHAR(1000),
                created_at      DATETIME2 DEFAULT GETDATE(),
                CONSTRAINT uq_user_class UNIQUE (user_id, class_id)
            )
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM Daniil_Tsitavets_feedback.feedbacks")
        if cur.fetchone()[0] == 0:
            # Stub UUIDs (would normally come from real registrations/users/classes)
            stubs = [
                ("A1000000-0000-0000-0000-000000000001",
                 "B1000000-0000-0000-0000-000000000001",
                 "C1000000-0000-0000-0000-000000000001",
                 5, "Outstanding class, learned so much!"),
                ("A1000000-0000-0000-0000-000000000002",
                 "B1000000-0000-0000-0000-000000000002",
                 "C1000000-0000-0000-0000-000000000002",
                 4, "Great content, a bit fast-paced."),
                ("A1000000-0000-0000-0000-000000000003",
                 "B1000000-0000-0000-0000-000000000003",
                 "C1000000-0000-0000-0000-000000000003",
                 3, "Good overall but audio quality could improve."),
                ("A1000000-0000-0000-0000-000000000004",
                 "B1000000-0000-0000-0000-000000000004",
                 "C1000000-0000-0000-0000-000000000004",
                 5, "Absolutely loved the Italian pasta section!"),
                ("A1000000-0000-0000-0000-000000000005",
                 "B1000000-0000-0000-0000-000000000005",
                 "C1000000-0000-0000-0000-000000000005",
                 4, "Very informative, would recommend."),
            ]
            for reg_id, usr_id, cls_id, rating, comment in stubs:
                cur.execute("""
                    INSERT INTO Daniil_Tsitavets_feedback.feedbacks
                        (registration_id, user_id, class_id, rating, comment)
                    VALUES (?, ?, ?, ?, ?)
                """, reg_id, usr_id, cls_id, rating, comment)
            conn.commit()
            print("Stub data inserted.")

    print("FeedbackService DB initialized.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(poll_service_bus())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="FeedbackService", lifespan=lifespan)


def row_to_dict(cursor, row):
    return {col[0]: val for col, val in zip(cursor.description, row)}


@app.get("/feedbacks")
def list_feedbacks(classId: Optional[str] = None):
    with get_conn() as conn:
        cur = conn.cursor()
        if classId:
            cur.execute("""
                SELECT * FROM Daniil_Tsitavets_feedback.feedbacks
                WHERE class_id = ?
                ORDER BY created_at DESC
            """, classId)
        else:
            cur.execute("SELECT * FROM Daniil_Tsitavets_feedback.feedbacks ORDER BY created_at DESC")
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]
