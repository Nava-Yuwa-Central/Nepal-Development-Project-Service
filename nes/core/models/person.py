"""Person-specific models."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class Education(BaseModel):
    """Education record for a person."""

    model_config = {"extra": "forbid"}

    institution: str = Field(..., description="Name of the educational institution")
    degree: Optional[str] = Field(None, description="Degree or qualification obtained")
    field: Optional[str] = Field(None, description="Field of study")
    startYear: Optional[int] = Field(None, description="Year education started")
    endYear: Optional[int] = Field(None, description="Year education completed")


class Position(BaseModel):
    """Position or role held by a person."""

    model_config = {"extra": "forbid"}

    title: str = Field(..., description="Job title or position name")
    organization: Optional[str] = Field(
        None, description="Organization or company name"
    )
    startDate: Optional[datetime] = Field(
        None, description="Start date of the position"
    )
    endDate: Optional[datetime] = Field(None, description="End date of the position")
