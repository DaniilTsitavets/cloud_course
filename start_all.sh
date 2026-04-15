#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "Stopping any running services..."
pkill -f "uvicorn main:app" 2>/dev/null || true
sleep 1

echo "Starting ChefService on :3001..."
python3 -m uvicorn main:app --app-dir "$ROOT/chef-service" --port 3001 --log-level info > /tmp/chef.log 2>&1 &

echo "Starting RegistrationService on :3002..."
python3 -m uvicorn main:app --app-dir "$ROOT/registration-service" --port 3002 --log-level info > /tmp/registration.log 2>&1 &

echo "Starting FeedbackService on :3003..."
python3 -m uvicorn main:app --app-dir "$ROOT/feedback-service" --port 3003 --log-level info > /tmp/feedback.log 2>&1 &

echo "Waiting 15s for DB initialization..."
sleep 15

echo ""
echo "=== ChefService /chefs ==="
curl -s http://localhost:3001/chefs | python3 -m json.tool

echo ""
echo "=== ChefService /classes ==="
curl -s http://localhost:3001/classes | python3 -m json.tool

echo ""
echo "=== RegistrationService /users ==="
curl -s http://localhost:3002/users | python3 -m json.tool

echo ""
echo "=== RegistrationService /registrations ==="
curl -s http://localhost:3002/registrations | python3 -m json.tool

echo ""
echo "=== FeedbackService /feedbacks ==="
curl -s http://localhost:3003/feedbacks | python3 -m json.tool

echo ""
echo "All services running. Logs: /tmp/chef.log  /tmp/registration.log  /tmp/feedback.log"
