# Employee Attendance & Analytics API

FastAPI + PyMongo service for employee records, attendance punch-in/out, audited corrections, and MongoDB-backed analytics. The app logic is centered in `app/main.py`; the project expects Python 3.11+ and MongoDB 6.0+.

## Run locally (PowerShell)

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:MONGO_URI = "mongodb://localhost:27017"
$env:MONGO_DB = "attendance_db"
python sample_seed.py   # optional: loads sample documents
python -m uvicorn app.main:app --port 8000 --reload
```

Set `MONGO_URI` and `MONGO_DB` as environment variables or in a local `.env` file. Environment variables win. See [openapi.yaml](./openapi.yaml) for the API contract.
