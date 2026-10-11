"""MongoDB aggregation pipelines and explain helpers."""

import json
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from bson import json_util
from fastapi import HTTPException
from pymongo import DESCENDING

from app.database import db
from app.services.attendance import PRESENCE_STATUSES


def month_bounds(month: str) -> tuple[str, str, date, date]:
    first = date.fromisoformat(f"{month}-01")
    following = date(first.year + (first.month == 12), 1 if first.month == 12 else first.month + 1, 1)
    return first.isoformat(), following.isoformat(), first, following - timedelta(days=1)


def rounded_expr(value: Any, places: int = 2) -> dict[str, Any]:
    scale = 10**places
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [value, scale]}, 0.5]}}, scale]}


def date_expr(date_value: Any) -> dict[str, Any]:
    return {"$dateFromString": {"dateString": date_value, "format": "%Y-%m-%d", "timezone": "UTC"}}


def day_of_week(date_value: Any) -> dict[str, Any]:
    return {"$dayOfWeek": date_expr(date_value)}


def monthly_working_days(joined_on: Any, month_start: str, month_end: str) -> dict[str, Any]:
    first_day = {"$max": [joined_on, month_start]}
    in_period = {"$lte": [first_day, month_end]}
    day_count = {
        "$add": [
            {"$dateDiff": {"startDate": date_expr(first_day), "endDate": date_expr(month_end), "unit": "day"}},
            1,
        ]
    }
    dates = {
        "$map": {
            "input": {"$range": [0, {"$cond": [in_period, day_count, 0]}]},
            "as": "offset",
            "in": {"$dateAdd": {"startDate": date_expr(first_day), "unit": "day", "amount": "$$offset"}},
        }
    }
    return {
        "$size": {
            "$filter": {
                "input": dates,
                "as": "day",
                "cond": {"$in": [{"$dayOfWeek": "$$day"}, [2, 3, 4, 5, 6]]},
            }
        }
    }


def employee_monthly_pipeline(emp_code: str, month: str) -> list[dict[str, Any]]:
    first, following, _, last = month_bounds(month)
    lookup = [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$eq": ["$emp_code", "$$code"]},
                        {"$gte": ["$date", first]},
                        {"$lt": ["$date", following]},
                    ]
                }
            }
        },
        {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1, "overtime_minutes": 1}},
    ]
    present_days = {
        "$sum": {
            "$map": {
                "input": {
                    "$filter": {
                        "input": "$logs",
                        "as": "log",
                        "cond": {
                            "$and": [
                                {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                                {"$in": [day_of_week("$$log.date"), [2, 3, 4, 5, 6]]},
                            ]
                        },
                    }
                },
                "as": "log",
                "in": {"$cond": [{"$eq": ["$$log.half_day", True]}, 0.5, 1]},
            }
        }
    }
    return [
        {"$match": {"emp_code": emp_code}},
        {"$lookup": {"from": "attendance_logs", "let": {"code": "$emp_code"}, "pipeline": lookup, "as": "logs"}},
        {
            "$project": {
                "_id": 0,
                "emp_code": 1,
                "month": {"$literal": month},
                "working_days": monthly_working_days("$joined_on", first, last.isoformat()),
                "present_days": present_days,
                "leave_days": {
                    "$size": {"$filter": {"input": "$logs", "as": "log", "cond": {"$eq": ["$$log.status", "LEAVE"]}}}
                },
                "late_count": {
                    "$size": {
                        "$filter": {
                            "input": "$logs",
                            "as": "log",
                            "cond": {"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]},
                        }
                    }
                },
                "total_late_minutes": {
                    "$sum": {"$map": {"input": "$logs", "as": "log", "in": {"$ifNull": ["$$log.late_minutes", 0]}}}
                },
                "total_overtime_minutes": {
                    "$sum": {"$map": {"input": "$logs", "as": "log", "in": {"$ifNull": ["$$log.overtime_minutes", 0]}}}
                },
            }
        },
        {
            "$set": {
                "attendance_pct": {
                    "$cond": [
                        {"$gt": ["$working_days", 0]},
                        rounded_expr({"$multiply": [{"$divide": ["$present_days", "$working_days"]}, 100]}),
                        None,
                    ]
                }
            }
        },
    ]


def department_summary_pipeline(month: str, department: str | None) -> list[dict[str, Any]]:
    first, following, _, last = month_bounds(month)
    match: dict[str, Any] = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    lookup = [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$eq": ["$emp_code", "$$code"]},
                        {"$gte": ["$date", first]},
                        {"$lt": ["$date", following]},
                    ]
                }
            }
        },
        {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1, "work_hours": 1}},
    ]
    present = {"$filter": {"input": "$logs", "as": "log", "cond": {"$in": ["$$log.status", list(PRESENCE_STATUSES)]}}}
    worked = {
        "$filter": {
            "input": "$logs",
            "as": "log",
            "cond": {
                "$and": [
                    {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                    {"$ne": [{"$ifNull": ["$$log.work_hours", None]}, None]},
                ]
            },
        }
    }
    employee_metrics = {
        "present_days": {
            "$sum": {
                "$map": {
                    "input": {
                        "$filter": {
                            "input": present,
                            "as": "log",
                            "cond": {"$in": [day_of_week("$$log.date"), [2, 3, 4, 5, 6]]},
                        }
                    },
                    "as": "log",
                    "in": {"$cond": [{"$eq": ["$$log.half_day", True]}, 0.5, 1]},
                }
            }
        },
        "late_count": {
            "$size": {
                "$filter": {
                    "input": "$logs",
                    "as": "log",
                    "cond": {"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]},
                }
            }
        },
        "total_late_minutes": {
            "$sum": {"$map": {"input": "$logs", "as": "log", "in": {"$ifNull": ["$$log.late_minutes", 0]}}}
        },
        "leave_count": {
            "$size": {"$filter": {"input": "$logs", "as": "log", "cond": {"$eq": ["$$log.status", "LEAVE"]}}}
        },
        "on_duty_count": {
            "$size": {"$filter": {"input": "$logs", "as": "log", "cond": {"$eq": ["$$log.status", "ON_DUTY"]}}}
        },
        "work_hours_total": {"$sum": {"$map": {"input": worked, "as": "log", "in": "$$log.work_hours"}}},
        "worked_records": {"$size": worked},
    }
    return [
        {"$match": match},
        {"$lookup": {"from": "attendance_logs", "let": {"code": "$emp_code"}, "pipeline": lookup, "as": "logs"}},
        {"$set": employee_metrics},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$present_days"},
                "late_count": {"$sum": "$late_count"},
                "total_late_minutes": {"$sum": "$total_late_minutes"},
                "leave_count": {"$sum": "$leave_count"},
                "on_duty_count": {"$sum": "$on_duty_count"},
                "work_hours_total": {"$sum": "$work_hours_total"},
                "worked_records": {"$sum": "$worked_records"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": 1,
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$worked_records", 0]},
                        rounded_expr({"$divide": ["$work_hours_total", "$worked_records"]}),
                        None,
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def leaderboard_pipeline(month: str, department: str | None, limit: int) -> list[dict[str, Any]]:
    first, following, _, _ = month_bounds(month)
    pipeline: list[dict[str, Any]] = [
        {"$match": {"date": {"$gte": first, "$lt": following}}},
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}},
            }
        },
        {"$match": {"total_late_minutes": {"$gt": 0}}},
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "employee",
            }
        },
        {"$unwind": "$employee"},
    ]
    if department is not None:
        pipeline.append({"$match": {"employee.department": department}})
    pipeline.extend(
        [
            {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
            {"$match": {"rank": {"$lte": limit}}},
            {"$sort": {"total_late_minutes": -1, "_id": 1}},
            {
                "$project": {
                    "_id": 0,
                    "rank": 1,
                    "emp_code": "$_id",
                    "name": "$employee.name",
                    "department": "$employee.department",
                    "total_late_minutes": 1,
                    "late_count": 1,
                }
            },
        ]
    )
    return pipeline


def department_trend_pipeline(department: str, start: date, end: date) -> list[dict[str, Any]]:
    end_exclusive = end + timedelta(days=1)
    day_string = {"$dateToString": {"date": "$day", "format": "%Y-%m-%d", "timezone": "UTC"}}
    return [
        {"$documents": [{"day": datetime.combine(start, time.min, tzinfo=timezone.utc)}]},
        {
            "$densify": {
                "field": "day",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [
                        datetime.combine(start, time.min, tzinfo=timezone.utc),
                        datetime.combine(end_exclusive, time.min, tzinfo=timezone.utc),
                    ],
                },
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "let": {"day": day_string},
                "pipeline": [
                    {"$match": {"$expr": {"$and": [{"$eq": ["$department", department]}, {"$lte": ["$joined_on", "$$day"]}]}}},
                    {"$count": "count"},
                ],
                "as": "heads",
            }
        },
        {
            "$lookup": {
                "from": "attendance_logs",
                "let": {"day": day_string},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$date", "$$day"]}}},
                    {
                        "$lookup": {
                            "from": "employees",
                            "localField": "emp_code",
                            "foreignField": "emp_code",
                            "as": "employee",
                        }
                    },
                    {"$unwind": "$employee"},
                    {"$match": {"employee.department": department}},
                    {"$project": {"_id": 0, "status": 1, "late_minutes": 1, "half_day": 1}},
                ],
                "as": "logs",
            }
        },
        {
            "$set": {
                "headcount": {"$ifNull": [{"$arrayElemAt": ["$heads.count", 0]}, 0]},
                "present_count": {
                    "$sum": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": "$logs",
                                    "as": "log",
                                    "cond": {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                                }
                            },
                            "as": "log",
                            "in": {"$cond": [{"$eq": ["$$log.half_day", True]}, 0.5, 1]},
                        }
                    }
                },
                "late_count": {
                    "$size": {
                        "$filter": {
                            "input": "$logs",
                            "as": "log",
                            "cond": {"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]},
                        }
                    }
                },
                "is_working_day": {"$in": [{"$isoDayOfWeek": "$day"}, [1, 2, 3, 4, 5]]},
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        rounded_expr({"$divide": ["$present_count", "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {"moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": day_string,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": {
                    "$cond": [
                        {"$ne": ["$moving_avg_7d", None]},
                        rounded_expr("$moving_avg_7d", 4),
                        None,
                    ]
                },
            }
        },
        {"$sort": {"date": 1}},
    ]


def run_aggregation(collection: Any, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def explain_find(collection_name: str, query: dict[str, Any], page: int, page_size: int) -> dict[str, Any]:
    command = {
        "find": collection_name,
        "filter": query,
        "sort": {"date": DESCENDING, "emp_code": 1},
        "skip": (page - 1) * page_size,
        "limit": page_size,
    }
    raw = db.command("explain", command, verbosity="executionStats")
    return json.loads(json_util.dumps(raw))


def explain_aggregate(collection_name: str | int, pipeline: list[dict[str, Any]]) -> dict[str, Any]:
    raw = db.command(
        "explain",
        {"aggregate": collection_name, "pipeline": pipeline, "cursor": {}, "allowDiskUse": True},
        verbosity="executionStats",
    )
    return json.loads(json_util.dumps(raw))


def validate_month(month: str) -> None:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise HTTPException(422, "month must use YYYY-MM format")


def validate_date_order(date_from: date | None, date_to: date | None) -> None:
    if date_from is not None and date_to is not None and date_from > date_to:
        raise HTTPException(422, "date_from must be on or before date_to")
