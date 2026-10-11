"""Attendance time rules and API response serialization."""

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from fastapi import HTTPException

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def to_epoch_millis(value: datetime | None) -> int | None:
    if value is None:
        return None
    delta = as_utc(value) - EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def from_epoch_millis(value: int | None) -> datetime:
    if value is None or isinstance(value, bool) or not isinstance(value, int) or not 100_000_000_000 <= value <= 4_102_444_800_000:
        raise HTTPException(422, "timestamp must be epoch milliseconds between 100000000000 and 4102444800000")
    try:
        return (EPOCH + timedelta(milliseconds=value)).replace(microsecond=0)
    except OverflowError as exc:
        raise HTTPException(422, "timestamp is out of range") from exc


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return to_epoch_millis(value)
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_value(item) for key, item in value.items() if key != "_id"}
    return value


def employee_response(document: dict[str, Any]) -> dict[str, Any]:
    fields = ("emp_code", "name", "email", "department", "shift_start", "shift_end", "joined_on", "created_at")
    return serialize_value({field: document[field] for field in fields})


def attendance_response(document: dict[str, Any]) -> dict[str, Any]:
    return serialize_value(
        {
            "emp_code": document["emp_code"],
            "date": document["date"],
            "status": document["status"],
            "punch_in": document.get("punch_in"),
            "punch_out": document.get("punch_out"),
            "work_hours": document.get("work_hours"),
            "late_minutes": document.get("late_minutes", 0),
            "overtime_minutes": document.get("overtime_minutes", 0),
            "half_day": document.get("half_day", False),
            "history": document.get("history", []),
        }
    )


def parse_shift_time(value: str) -> time:
    hour, minute = (int(part) for part in value.split(":"))
    return time(hour, minute)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    local = as_utc(punch_in).astimezone(IST)
    end = parse_shift_time(shift_end)
    day = local.date()
    if end <= parse_shift_time(shift_start) and local.time().replace(tzinfo=None) < end:
        day -= timedelta(days=1)
    return day


def shift_instant(day: date, shift: str, overnight_end: bool = False) -> datetime:
    instant = datetime.combine(day, parse_shift_time(shift), tzinfo=IST)
    return instant + (timedelta(days=1) if overnight_end else timedelta())


def compute_late_minutes(punch_in: datetime, shift_start: str, attendance_day: date | None = None) -> int:
    local = as_utc(punch_in).astimezone(IST)
    shift_day = attendance_day or local.date()
    elapsed = (local - shift_instant(shift_day, shift_start)).total_seconds()
    return int(elapsed // 60) if elapsed > 600 else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    seconds = int((as_utc(punch_out) - as_utc(punch_in)).total_seconds())
    hours = (Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours)


def compute_overtime(punch_out: datetime, shift_end: str, attendance_day: date, overnight: bool) -> int:
    elapsed = (as_utc(punch_out) - shift_instant(attendance_day, shift_end, overnight)).total_seconds()
    minutes = int(elapsed // 60)
    return minutes if minutes >= 30 else 0


def derived_values(
    status: str,
    punch_in: datetime | None,
    punch_out: datetime | None,
    employee: dict[str, Any],
    attendance_day: date,
) -> dict[str, Any]:
    if status not in PRESENCE_STATUSES:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, employee["shift_start"], attendance_day) if punch_in else 0
    if punch_out is None or punch_in is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    work_hours = compute_work_hours(punch_in, punch_out)
    overnight = parse_shift_time(employee["shift_end"]) <= parse_shift_time(employee["shift_start"])
    return {
        "work_hours": work_hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime(punch_out, employee["shift_end"], attendance_day, overnight),
        "half_day": work_hours < 4.50,
    }
