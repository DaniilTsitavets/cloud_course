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
    mock_cursor.description = [("chef_id",), ("name",), ("rating",)]
    row = ("id-1", "Gordon Ramsay", 4.95)
    result = row_to_dict(mock_cursor, row)
    assert result == {"chef_id": "id-1", "name": "Gordon Ramsay", "rating": 4.95}


def test_list_chefs(client):
    rows = [("id-1", "Gordon Ramsay", None, "British", 4.95)]
    columns = ["chef_id", "name", "bio", "specialization", "rating"]
    mock_conn, _ = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/chefs")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == "Gordon Ramsay"


def test_list_classes(client):
    rows = [("cls-1", "chef-1", "Mastering French", "2026-06-01", 20, 15, 99.0, None, "Julia Child")]
    columns = ["class_id", "chef_id", "title", "schedule", "max_capacity", "seats_available", "price", "description", "chef_name"]
    mock_conn, _ = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/classes")

    assert response.status_code == 200
    data = response.json()
    assert data[0]["title"] == "Mastering French"


def test_get_class_found(client):
    rows = [("cls-1", "chef-1", "Mastering French", "2026-06-01", 20, 15, 99.0, None, "Julia Child", "Bio", 4.87)]
    columns = ["class_id", "chef_id", "title", "schedule", "max_capacity", "seats_available", "price", "description", "chef_name", "chef_bio", "chef_rating"]
    mock_conn, _ = make_mock_conn(rows, columns)

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/classes/cls-1")

    assert response.status_code == 200
    assert response.json()["title"] == "Mastering French"


def test_get_class_not_found(client):
    mock_conn, mock_cursor = make_mock_conn([], ["class_id"])
    mock_cursor.fetchone.return_value = None

    with patch("main.get_conn", return_value=mock_conn):
        response = client.get("/classes/nonexistent")

    assert response.status_code == 404