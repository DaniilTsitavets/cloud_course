import pyodbc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional

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
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Daniil_Tsitavets_feedback')
                EXEC('CREATE SCHEMA Daniil_Tsitavets_feedback')
        """)

        # Drop legacy tables from old schema
        cur.execute("""
            IF OBJECT_ID('Daniil_Tsitavets_feedback.feedback_responses', 'U') IS NOT NULL
                DROP TABLE Daniil_Tsitavets_feedback.feedback_responses
        """)
        cur.execute("""
            IF OBJECT_ID('feedback.feedback', 'U') IS NOT NULL
                DROP TABLE Daniil_Tsitavets_feedback.feedback
        """)
        # Drop Feedbacks/feedbacks table if it has wrong schema (no registration_id column)
        cur.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'Daniil_Tsitavets_feedback'
                  AND TABLE_NAME IN ('feedbacks','Feedbacks')
                  AND COLUMN_NAME = 'registration_id'
            )
            BEGIN
                IF OBJECT_ID('Daniil_Tsitavets_feedback.Feedbacks', 'U') IS NOT NULL DROP TABLE Daniil_Tsitavets_feedback.Feedbacks;
                IF OBJECT_ID('Daniil_Tsitavets_feedback.feedbacks', 'U') IS NOT NULL DROP TABLE Daniil_Tsitavets_feedback.feedbacks;
            END
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
    yield


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
