from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient
import pytest


def make_mock_conn(rows, columns):
    mock_cursor = MagicMock()
    mock_cursor.description = [(col,) for col in columns]
    mock_cursor.fetchall.return_value = rows
    mock_cursor.fetchone.return_value = rows[0] if rows else None
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


with patch("main.init_db"), patch("main.poll_service_bus", new=AsyncMock()):
    from main import app, row_to_dict


@pytest.fixture
def client():
    with patch("main.init_db"), patch("main.poll_service_bus", new=AsyncMock()):
        with TestClient(app) as c:
            yield c


def test_row_to_dict():
    mock_cursor = MagicMock()
    mock_cursor.description = [("feedback_id",), ("rating",), ("comment",)]
    row = ("fb-1", 5, "Great!")
    result = row_to_dict(mock_cursor, row)
    assert result == {"feedback_id": "fb-1", "rating": 5, "comment": "Great!"}


def test_list_feedbacks_all(client):
    rows = [
        ("fb-1", "reg-1", "uid-1", "cls-1", 5, "Great!", "2026-01-01"),
        ("fb-2", "reg-2", "uid-2", "cls-2", 4, "Good", "2026-01-02"),
    ]
    columns = ["feedback_id", "registration_id", "user_id", "class_id", "rating", "comment", "created_at"]
    mock_conn, _ = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/feedbacks")

    assert response.status_code == 200
    assert len(response.json()) == 2
    assert response.json()[0]["rating"] == 5


def test_list_feedbacks_by_class(client):
    rows = [("fb-1", "reg-1", "uid-1", "cls-1", 5, "Great!", "2026-01-01")]
    columns = ["feedback_id", "registration_id", "user_id", "class_id", "rating", "comment", "created_at"]
    mock_conn, mock_cursor = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/feedbacks?classId=cls-1")

    assert response.status_code == 200
    sql_call = mock_cursor.execute.call_args[0][0]
    assert "class_id" in sql_call


def test_list_feedbacks_empty(client):
    mock_conn, mock_cursor = make_mock_conn([], ["feedback_id", "rating"])
    mock_cursor.fetchall.return_value = []

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/feedbacks")

    assert response.status_code == 200
    assert response.json() == []


def test_create_feedback_success(client):
    new_id = "fb-new-1"
    mock_conn, mock_cursor = make_mock_conn([(new_id,)], ["feedback_id"])

    with patch("main.get_conn", return_value=mock_conn):
        response = client.post("/feedbacks", json={
            "registration_id": "reg-1",
            "user_id": "uid-1",
            "class_id": "cls-1",
            "rating": 5,
            "comment": "Excellent!",
        })

    assert response.status_code == 201
    assert response.json()["feedback_id"] == new_id


def test_create_feedback_invalid_rating_too_high(client):
    response = client.post("/feedbacks", json={
        "registration_id": "reg-1",
        "user_id": "uid-1",
        "class_id": "cls-1",
        "rating": 6,
    })
    assert response.status_code == 422


def test_create_feedback_invalid_rating_too_low(client):
    response = client.post("/feedbacks", json={
        "registration_id": "reg-1",
        "user_id": "uid-1",
        "class_id": "cls-1",
        "rating": 0,
    })
    assert response.status_code == 422