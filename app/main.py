"""FastAPI application entry point and route registration."""

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Annotated, Any, AsyncGenerator, Literal

from fastapi import FastAPI, HTTPException, Path, Query
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

from app.database import client, create_indexes, db
from app.models.schemas import EmployeeIn, PunchInIn, PunchOutIn, RegularizeIn
from app.services.analytics import (
    department_summary_pipeline,
    department_trend_pipeline,
    employee_monthly_pipeline,
    explain_aggregate,
    explain_find,
    leaderboard_pipeline,
    run_aggregation,
    validate_date_order,
    validate_month,
)
from app.services.attendance import (
    attendance_date,
    attendance_response,
    derived_values,
    employee_response,
    from_epoch_millis,
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    create_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]


def get_employee_or_404(emp_code: str) -> dict[str, Any]:
    employee = db.employees.find_one({"emp_code": emp_code})
    if employee is None:
        raise HTTPException(404, "employee not found")
    return employee


def get_punched_at(value: int | None) -> datetime:
    if value is not None:
        return from_epoch_millis(value)
    return datetime.now(timezone.utc).replace(microsecond=0)


def build_attendance_query(
    emp_code: str | None,
    date_from: date | None,
    date_to: date | None,
    status: Status | None,
) -> dict[str, Any]:
    query: dict[str, Any] = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        query["date"] = {}
        if date_from is not None:
            query["date"]["$gte"] = date_from.isoformat()
        if date_to is not None:
            query["date"]["$lte"] = date_to.isoformat()
    if status is not None:
        query["status"] = status
    return query


def paginate_records(collection: Any, query: dict[str, Any], sort: list[tuple[str, int]] | list[tuple[str, int]] | None, page: int, page_size: int) -> dict[str, Any]:
    total = collection.count_documents(query)
    cursor = collection.find(query, {"_id": 0})
    if sort is not None:
        cursor = cursor.sort(sort)
    cursor = cursor.skip((page - 1) * page_size).limit(page_size)
    return {"items": list(cursor), "total": total, "page": page, "page_size": page_size}


@app.get("/health")
def health() -> dict[str, str]:
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        raise HTTPException(503, "MongoDB is unavailable") from exc
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn) -> dict[str, Any]:
    document = body.model_dump()
    document["created_at"] = datetime.now(timezone.utc)
    try:
        db.employees.insert_one(document)
    except DuplicateKeyError as exc:
        raise HTTPException(409, "emp_code already exists") from exc
    return employee_response(document)


@app.get("/employees")
def list_employees(
    department: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, Any]:
    query = {"department": department} if department is not None else {}
    raw = paginate_records(db.employees, query, [("emp_code", ASCENDING)], page, page_size)
    return {
        "items": [employee_response(item) for item in raw["items"]],
        "total": raw["total"],
        "page": raw["page"],
        "page_size": raw["page_size"],
    }


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn) -> dict[str, Any]:
    employee = get_employee_or_404(body.emp_code)
    instant = get_punched_at(body.punched_at)
    day = attendance_date(instant, employee["shift_start"], employee["shift_end"])
    document = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": instant,
        "punch_out": None,
        **derived_values(body.status, instant, None, employee, day),
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(document)
    except DuplicateKeyError as exc:
        raise HTTPException(409, "already punched in for this date") from exc
    return attendance_response(document)


@app.get("/attendance")
def list_attendance(
    emp_code: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    status: Status | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, Any]:
    validate_date_order(date_from, date_to)
    query = build_attendance_query(emp_code, date_from, date_to, status)
    raw = paginate_records(db.attendance_logs, query, [("date", DESCENDING), ("emp_code", ASCENDING)], page, page_size)
    return {
        "items": [attendance_response(item) for item in raw["items"]],
        "total": raw["total"],
        "page": raw["page"],
        "page_size": raw["page_size"],
    }


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn) -> dict[str, Any]:
    employee = get_employee_or_404(body.emp_code)
    instant = get_punched_at(body.punched_at)
    record = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": instant}},
        sort=[("punch_in", DESCENDING)],
    )
    if record is None:
        raise HTTPException(404, "no punch-in found")
    if record.get("punch_out") is not None:
        raise HTTPException(409, "record is already punched out")
    elapsed = (instant - record["punch_in"]).total_seconds()
    if elapsed <= 0 or elapsed > 24 * 60 * 60:
        raise HTTPException(422, "punched_at must be after punch_in and no more than 24 hours later")
    day = date.fromisoformat(record["date"])
    values = derived_values(record["status"], record["punch_in"], instant, employee, day)
    result = db.attendance_logs.update_one(
        {
            "_id": record["_id"],
            "punch_in": record["punch_in"],
            "$or": [{"punch_out": None}, {"punch_out": {"$exists": False}}],
        },
        {"$set": {"punch_out": instant, **values}},
    )
    if result.modified_count != 1:
        raise HTTPException(409, "record is already punched out")
    record.update({"punch_out": instant, **values})
    return attendance_response(record)


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(
    emp_code: str,
    body: RegularizeIn,
    record_date: date = Path(alias="date"),
) -> dict[str, Any]:
    employee = db.employees.find_one({"emp_code": emp_code})
    if employee is None:
        raise HTTPException(404, "employee not found")
    day_string = record_date.isoformat()
    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": day_string})
    if record is None:
        raise HTTPException(404, "attendance record not found")

    supplied = body.model_fields_set
    status: str = record["status"]
    if "status" in supplied:
        if body.status is None:
            raise HTTPException(422, "status cannot be null")
        status = body.status
    if status in ("ABSENT", "LEAVE"):
        if "punch_in" in supplied or "punch_out" in supplied:
            raise HTTPException(422, "ABSENT and LEAVE records cannot include punch times")
        punch_in_time = punch_out_time = None
    else:
        if "punch_in" in supplied:
            if body.punch_in is None:
                raise HTTPException(422, "punch_in cannot be null")
            punch_in_time = from_epoch_millis(body.punch_in)
        else:
            punch_in_time = record.get("punch_in")
        if "punch_out" in supplied:
            if body.punch_out is None:
                raise HTTPException(422, "punch_out cannot be null")
            punch_out_time = from_epoch_millis(body.punch_out)
        else:
            punch_out_time = record.get("punch_out")
        if punch_in_time is None:
            raise HTTPException(422, "presence statuses require a punch_in")
        if attendance_date(punch_in_time, employee["shift_start"], employee["shift_end"]) != record_date:
            raise HTTPException(422, "punch_in must belong to the record's attendance date")
        if punch_out_time is not None:
            elapsed = (punch_out_time - punch_in_time).total_seconds()
            if elapsed <= 0 or elapsed > 24 * 60 * 60:
                raise HTTPException(422, "punch_out must be after punch_in and no more than 24 hours later")

    new_values = {
        "status": status,
        "punch_in": punch_in_time,
        "punch_out": punch_out_time,
        **derived_values(status, punch_in_time, punch_out_time, employee, record_date),
    }
    old_values = {
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "work_hours": record.get("work_hours"),
        "late_minutes": record.get("late_minutes", 0),
        "overtime_minutes": record.get("overtime_minutes", 0),
        "half_day": record.get("half_day", False),
    }
    changes = {
        key: {"from": old_values[key], "to": new_values[key]}
        for key in new_values
        if old_values.get(key) != new_values.get(key)
    }
    if not changes:
        raise HTTPException(422, "regularization must change at least one field")

    history_entry = {
        "at": datetime.now(timezone.utc),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }
    optimistic_filter: dict[str, Any] = {"_id": record["_id"]}
    for key in ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day", "history"):
        optimistic_filter[key] = record[key] if key in record else {"$exists": False}
    result = db.attendance_logs.update_one(
        optimistic_filter,
        {"$set": new_values, "$push": {"history": history_entry}},
    )
    if result.modified_count != 1:
        raise HTTPException(409, "attendance record changed concurrently; retry the correction")
    record.update(new_values)
    record.setdefault("history", []).append(history_entry)
    return attendance_response(record)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(
    emp_code: str,
    month: Annotated[str, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")],
) -> dict[str, Any]:
    validate_month(month)
    if db.employees.find_one({"emp_code": emp_code}, {"_id": 1}) is None:
        raise HTTPException(404, "employee not found")
    return run_aggregation(db.employees, employee_monthly_pipeline(emp_code, month))[0]


@app.get("/analytics/departments/summary")
def department_summary(
    month: Annotated[str, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")],
    department: str | None = None,
) -> dict[str, Any]:
    validate_month(month)
    return {"month": month, "items": run_aggregation(db.employees, department_summary_pipeline(month, department))}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: Annotated[str, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    department: str | None = None,
) -> dict[str, Any]:
    validate_month(month)
    return {"month": month, "items": run_aggregation(db.attendance_logs, leaderboard_pipeline(month, department, limit))}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_date: date = Query(alias="from"),
    to_date: date = Query(alias="to"),
) -> dict[str, Any]:
    if to_date < from_date:
        raise HTTPException(422, "to must be on or after from")
    if (to_date - from_date).days + 1 > 92:
        raise HTTPException(422, "date range cannot exceed 92 days")
    if db.employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "department not found")
    pipeline = department_trend_pipeline(department, from_date, to_date)
    return {"department": department, "items": list(db.aggregate(pipeline, allowDiskUse=True))}


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: str | None = None,
    month: Annotated[str | None, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")] = None,
    department: str | None = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: date | None = None,
    date_to: date | None = None,
    status: Status | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
) -> dict[str, Any]:
    if endpoint == "attendance_list":
        validate_date_order(date_from, date_to)
        query: dict[str, Any] = {}
        if emp_code is not None:
            query["emp_code"] = emp_code
        if date_from is not None or date_to is not None:
            query["date"] = {}
            if date_from is not None:
                query["date"]["$gte"] = date_from.isoformat()
            if date_to is not None:
                query["date"]["$lte"] = date_to.isoformat()
        if status is not None:
            query["status"] = status
        raw = explain_find("attendance_logs", query, page, page_size)
        collection = "attendance_logs"
    elif endpoint == "employee_monthly":
        if not emp_code or not month:
            raise HTTPException(422, "emp_code and month are required")
        validate_month(month)
        raw = explain_aggregate("employees", employee_monthly_pipeline(emp_code, month))
        collection = "employees"
    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(422, "month is required")
        validate_month(month)
        raw = explain_aggregate("employees", department_summary_pipeline(month, department))
        collection = "employees"
    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(422, "month is required")
        validate_month(month)
        raw = explain_aggregate("attendance_logs", leaderboard_pipeline(month, department, limit))
        collection = "attendance_logs"
    else:
        if department is None or from_date is None or to_date is None:
            raise HTTPException(422, "department, from, and to are required")
        if to_date < from_date or (to_date - from_date).days + 1 > 92:
            raise HTTPException(422, "invalid department trend date range")
        raw = explain_aggregate(1, department_trend_pipeline(department, from_date, to_date))
        collection = "attendance_logs"
    return {"endpoint": endpoint, "collection": collection, "explain": raw}
