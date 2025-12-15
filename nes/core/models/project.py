"""Project-specific models for nes.

This module is the single source of truth for development projects in Nepal.
It is designed to map:
- NPC Project Bank (spatial-first, minimal)
- World Bank (narrative + components)
- ADB (IATI, event-based)
- JICA (loan mechanics)
into a MoF DFMIS-compatible structure without distorting the core entity system.
"""

from datetime import date
from enum import Enum
from typing import Dict, List, Literal, Optional

from pydantic import AnyUrl, BaseModel, ConfigDict, Field

from .entity import Entity, EntitySubType, EntityType


# ---------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------

class ProjectStage(str, Enum):
    PIPELINE = "pipeline"
    PLANNING = "planning"
    APPROVED = "approved"
    ONGOING = "ongoing"
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    TERMINATED = "terminated"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class FinancingInstrumentType(str, Enum):
    GRANT = "grant"
    LOAN = "loan"
    MIXED = "mixed"
    OTHER = "other"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------
# Financing models (JICA-compatible, donor-agnostic)
# ---------------------------------------------------------------------

class FinancingInstrument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    instrument_type: FinancingInstrumentType = FinancingInstrumentType.UNKNOWN
    currency: Optional[str] = Field(None, description="ISO currency code")
    amount: Optional[float] = Field(None, description="Committed amount")

    interest_rate: Optional[float] = None
    repayment_period_years: Optional[int] = None
    grace_period_years: Optional[int] = None
    tying_status: Optional[str] = None


class FinancingComponent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Main, Consulting, TA, Emergency, etc.")
    financing: FinancingInstrument


# ---------------------------------------------------------------------
# Temporal model (ADB / WB compatible)
# ---------------------------------------------------------------------

class ProjectDateEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: date
    type: str = Field(..., description="APPROVAL, START, COMPLETION, CLOSING, etc.")
    source: Optional[str] = Field(None, description="WB, ADB, JICA, NPC")


# ---------------------------------------------------------------------
# Spatial model (NPC-first, donor-compatible)
# ---------------------------------------------------------------------

class ProjectLocation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latitude: float
    longitude: float

    province: Optional[str] = None
    district: Optional[str] = None
    municipality: Optional[str] = None
    ward: Optional[str] = None

    source: Optional[str] = Field(None, description="NPC, ADB, WB")


# ---------------------------------------------------------------------
# Classification & tagging
# ---------------------------------------------------------------------

class SectorMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    normalized_sector: Optional[str] = Field(
        None, description="MoF-normalized sector code or name"
    )
    donor_sector: Optional[str] = Field(
        None, description="Sector as reported by the donor"
    )
    donor_subsector: Optional[str] = None
    donor: Optional[str] = None


class CrossCuttingTag(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str = Field(
        ..., description="THEME, POLICY, GENDER, CLIMATE, GOVERNANCE"
    )
    normalized_tag: Optional[str] = None
    donor_tag: Optional[str] = None
    donor: Optional[str] = None


# ---------------------------------------------------------------------
# Donor extensions & raw preservation
# ---------------------------------------------------------------------

class DonorExtension(BaseModel):
    model_config = ConfigDict(extra="allow")

    donor: str = Field(..., description="WB, ADB, JICA, NPC")
    donor_project_id: Optional[str] = None
    raw_payload: Optional[Dict] = Field(
        None, description="Original donor payload for traceability"
    )


# ---------------------------------------------------------------------
# Core Project entity
# ---------------------------------------------------------------------

class Project(Entity):
    """Development project entity.

    This entity represents the national view of a project.
    Donors, financing, geography, and timelines are attached as structured extensions.
    """

    type: Literal["project"] = Field(
        default="project", description="Entity type, always project"
    )
    sub_type: EntitySubType = Field(
        default=EntitySubType.DEVELOPMENT_PROJECT,
        description="Project subtype classification"
    )

    stage: ProjectStage = Field(default=ProjectStage.UNKNOWN)

    implementing_agency: Optional[str] = Field(
        None, description="Primary implementing agency"
    )
    executing_agency: Optional[str] = Field(
        None, description="Executing agency (when distinct)"
    )

    financing: Optional[List[FinancingComponent]] = None
    dates: Optional[List[ProjectDateEvent]] = None
    locations: Optional[List[ProjectLocation]] = None

    sectors: Optional[List[SectorMapping]] = None
    tags: Optional[List[CrossCuttingTag]] = None

    donors: Optional[List[str]] = Field(
        None, description="List of contributing donors"
    )
    donor_extensions: Optional[List[DonorExtension]] = None

    project_url: Optional[AnyUrl] = None