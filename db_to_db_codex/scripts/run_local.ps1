$ErrorActionPreference = "Stop"
python -m uvicorn app.db_to_db_action_service:app --reload --port 8000
