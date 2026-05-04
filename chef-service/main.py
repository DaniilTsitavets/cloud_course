import logging
import os
import pyodbc
from contextlib import asynccontextmanager
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

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

        # Create schema if not exists
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Daniil_Tsitavets_chef')
                EXEC('CREATE SCHEMA Daniil_Tsitavets_chef')
        """)

        # Create chefs table if not exists
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('Daniil_Tsitavets_chef.chefs'))
            CREATE TABLE Daniil_Tsitavets_chef.chefs (
                chef_id   UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                name      NVARCHAR(100) NOT NULL,
                bio       NVARCHAR(500),
                specialization NVARCHAR(100),
                rating    DECIMAL(3,2) DEFAULT 0.00
            )
        """)

        # Create classes table if not exists
        cur.execute("""
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('Daniil_Tsitavets_chef.classes'))
            CREATE TABLE Daniil_Tsitavets_chef.classes (
                class_id        UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                chef_id         UNIQUEIDENTIFIER NOT NULL REFERENCES Daniil_Tsitavets_chef.chefs(chef_id),
                title           NVARCHAR(200) NOT NULL,
                schedule        DATETIME2 NOT NULL,
                max_capacity    INT NOT NULL,
                seats_available INT NOT NULL,
                price           DECIMAL(10,2) NOT NULL,
                description     NVARCHAR(500)
            )
        """)
        conn.commit()

        # Insert stub data if empty
        cur.execute("SELECT COUNT(*) FROM Daniil_Tsitavets_chef.chefs")
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO Daniil_Tsitavets_chef.chefs (name, bio, specialization, rating) VALUES
                (N'Gordon Ramsay',    N'World-renowned chef, Michelin-starred restaurants.', N'British cuisine',       4.95),
                (N'Julia Child',      N'Pioneer of French cuisine in America.',               N'French cuisine',        4.87),
                (N'Nobu Matsuhisa',   N'Iconic Japanese-Peruvian fusion chef.',               N'Japanese fusion',       4.92),
                (N'Massimo Bottura',  N'Italian chef with three Michelin stars.',             N'Italian cuisine',       4.89),
                (N'Yotam Ottolenghi', N'Israeli-British chef, vegetarian specialist.',        N'Middle Eastern cuisine',4.78)
            """)
            conn.commit()

            cur.execute("""
                INSERT INTO Daniil_Tsitavets_chef.classes (chef_id, title, schedule, max_capacity, seats_available, price, description)
                SELECT
                    chef_id,
                    N'Mastering ' + specialization,
                    DATEADD(day, ROW_NUMBER() OVER (ORDER BY name) * 3, GETDATE()),
                    20,
                    15,
                    CAST(ROW_NUMBER() OVER (ORDER BY name) * 25 + 50 AS DECIMAL(10,2)),
                    N'An immersive online class on ' + specialization
                FROM Daniil_Tsitavets_chef.chefs
            """)
            conn.commit()
            logger.info("Stub data inserted.")

    logger.info("ChefService DB initialized.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="ChefService", lifespan=lifespan)


def row_to_dict(cursor, row):
    return {col[0]: val for col, val in zip(cursor.description, row)}


@app.get("/chefs")
def list_chefs():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM Daniil_Tsitavets_chef.chefs ORDER BY rating DESC")
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]


@app.get("/classes")
def list_classes():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT cl.*, ch.name AS chef_name
            FROM Daniil_Tsitavets_chef.classes cl
            JOIN Daniil_Tsitavets_chef.chefs ch ON ch.chef_id = cl.chef_id
            ORDER BY cl.schedule
        """)
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]


@app.get("/classes/{class_id}")
def get_class(class_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT cl.*, ch.name AS chef_name, ch.bio AS chef_bio, ch.rating AS chef_rating
            FROM Daniil_Tsitavets_chef.classes cl
            JOIN Daniil_Tsitavets_chef.chefs ch ON ch.chef_id = cl.chef_id
            WHERE cl.class_id = ?
        """, class_id)
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Class not found")
        return row_to_dict(cur, row)


class ChefIn(BaseModel):
    name: str
    bio: Optional[str] = None
    specialization: Optional[str] = None
    rating: float = 0.0


@app.post("/chefs", status_code=201)
def create_chef(body: ChefIn):
    if not 0.0 <= body.rating <= 5.0:
        raise HTTPException(status_code=422, detail="Rating must be between 0 and 5")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO Daniil_Tsitavets_chef.chefs (name, bio, specialization, rating)
            OUTPUT INSERTED.chef_id
            VALUES (?, ?, ?, ?)
        """, body.name, body.bio, body.specialization, body.rating)
        new_id = cur.fetchone()[0]
        conn.commit()
    logger.info("Chef created: chef_id=%s", new_id)
    return {"chef_id": str(new_id)}
