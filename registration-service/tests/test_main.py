from unittest.mock import MagicMock, patch
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


with patch("main.init_db"):
    from main import app, row_to_dict


@pytest.fixture
def client():
    with patch("main.init_db"):
        with TestClient(app) as c:
            yield c


def test_row_to_dict():
    mock_cursor = MagicMock()
    mock_cursor.description = [("user_id",), ("name",), ("email",)]
    row = ("uid-1", "Alice", "alice@example.com")
    result = row_to_dict(mock_cursor, row)
    assert result == {"user_id": "uid-1", "name": "Alice", "email": "alice@example.com"}


def test_list_users(client):
    rows = [("uid-1", "Alice", "alice@example.com", "2026-01-01")]
    columns = ["user_id", "name", "email", "created_at"]
    mock_conn, _ = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/users")

    assert response.status_code == 200
    assert response.json()[0]["name"] == "Alice"


def test_list_users_empty(client):
    mock_conn, mock_cursor = make_mock_conn([], ["user_id", "name", "email", "created_at"])
    mock_cursor.fetchall.return_value = []

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/users")

    assert response.status_code == 200
    assert response.json() == []


def test_list_registrations(client):
    rows = [("reg-1", "uid-1", "cls-1", "CONFIRMED", "2026-01-01", None, "Alice", "alice@example.com")]
    columns = ["registration_id", "user_id", "class_id", "status",
               "registered_at", "cancelled_at", "user_name", "user_email"]
    mock_conn, _ = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/registrations")

    assert response.status_code == 200
    assert response.json()[0]["status"] == "CONFIRMED"


def test_create_registration_user_not_found(client):
    mock_conn, mock_cursor = make_mock_conn([], ["user_id"])
    mock_cursor.fetchone.return_value = None

    with patch("main.get_conn", return_value=mock_conn):
        response = client.post("/registrations", json={"user_id": "bad-id", "class_id": "cls-1"})

    assert response.status_code == 404


def test_create_registration_success(client):
    new_reg_id = "reg-new-1"
    call_count = {"n": 0}

    mock_cursor = MagicMock()
    mock_cursor.description = [("registration_id",)]

    def fetchone():
        call_count["n"] += 1
        return (1,) if call_count["n"] == 1 else (new_reg_id,)

    mock_cursor.fetchone = fetchone
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    with patch("main.get_conn", return_value=mock_conn), \
         patch("main.publish_registration_completed") as mock_publish:
        response = client.post("/registrations", json={"user_id": "uid-1", "class_id": "cls-1"})

    assert response.status_code == 201
    assert response.json()["registration_id"] == new_reg_id
    assert response.json()["status"] == "PENDING"


def test_create_registration_publishes_event(client):
    new_reg_id = "reg-new-2"
    call_count = {"n": 0}

    mock_cursor = MagicMock()

    def fetchone():
        call_count["n"] += 1
        return (1,) if call_count["n"] == 1 else (new_reg_id,)

    mock_cursor.fetchone = fetchone
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    with patch("main.get_conn", return_value=mock_conn), \
         patch("main.publish_registration_completed") as mock_publish:
        client.post("/registrations", json={"user_id": "uid-1", "class_id": "cls-99"})

    mock_publish.assert_called_once_with(new_reg_id, "uid-1", "cls-99")