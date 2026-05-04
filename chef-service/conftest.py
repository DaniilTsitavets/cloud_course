import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("DB_SERVER", "test-server")
os.environ.setdefault("DB_NAME", "test-db")
os.environ.setdefault("DB_USER", "test-user")
os.environ.setdefault("DB_PASSWORD", "test-pass")

sys.modules["pyodbc"] = MagicMock()