import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("DB_SERVER", "test-server")
os.environ.setdefault("DB_NAME", "test-db")
os.environ.setdefault("DB_USER", "test-user")
os.environ.setdefault("DB_PASSWORD", "test-pass")
os.environ.setdefault("SB_SEND_CONN_STR", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=dGVzdA==")
os.environ.setdefault("SB_QUEUE_NAME", "test-queue")

sys.modules["pyodbc"] = MagicMock()
sys.modules["azure"] = MagicMock()
sys.modules["azure.servicebus"] = MagicMock()