"""Validated request schemas."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints, ValidationInfo, field_validator

Code = Annotated[str, StringConstraints(pattern=r"^EMP\d{4,6}$")]
Shift = Annotated[str, StringConstraints(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]
ISODate = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]
EpochInput = Annotated[StrictInt, Field(ge=100_000_000_000, le=4_102_444_800_000)]


class EmployeeIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: Code
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: Shift = "09:30"
    shift_end: Shift = "18:30"
    joined_on: ISODate

    @field_validator("joined_on")
    @classmethod
    def valid_join_date(cls, value: str) -> str:
        from datetime import date

        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("joined_on must be a valid YYYY-MM-DD date") from exc
        return value

    @field_validator("shift_end")
    @classmethod
    def distinct_shift_times(cls, value: str, info: ValidationInfo) -> str:
        if info.data.get("shift_start") == value:
            raise ValueError("shift_start and shift_end must differ")
        return value


class PunchInIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: str
    punched_at: EpochInput | None = None
    status: Literal["PRESENT", "WFH", "ON_DUTY"] = "PRESENT"


class PunchOutIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: str
    punched_at: EpochInput | None = None


class RegularizeIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"] | None = None
    punch_in: EpochInput | None = None
    punch_out: EpochInput | None = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]

    @field_validator("status", "punch_in", "punch_out", mode="before")
    @classmethod
    def reject_explicit_nulls(cls, value: object) -> object:
        if value is None:
            raise ValueError("field cannot be null")
        return value
