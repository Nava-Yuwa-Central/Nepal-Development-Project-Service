"""Project-specific models for nes."""

from datetime import date
from enum import Enum
from typing import List, Literal, Optional

from pydantic import AnyUrl, BaseModel, ConfigDict, Field

from .base import Address, LangText
from .entity import Entity, EntitySubType


class ProjectStatus(str, Enum):
    """Project status enumeration."""
    
    PIPELINE = "pipeline"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DROPPED = "dropped"


class ProjectDetails(BaseModel):
    """Project-specific details."""

    model_config = ConfigDict(extra="forbid")

    # Financial information
    total_allocated_budget: Optional[str] = Field(
        None, description="Total allocated budget amount"
    )
    real_time_spending: Optional[str] = Field(
        None, description="Real-time spending amount"
    )
    funding_source: Optional[str] = Field(
        None, description="Source of funding (e.g., World Bank, ADB, Government)"
    )
    loan_amount: Optional[str] = Field(
        None, description="Loan component amount"
    )
    grant_amount: Optional[str] = Field(
        None, description="Grant component amount"
    )
    
    # Timeline information
    start_date: Optional[str] = Field(
        None, description="Project start date"
    )
    end_date: Optional[str] = Field(
        None, description="Project end date"
    )
    
    # Progress information
    physical_progress: Optional[str] = Field(
        None, description="Physical progress percentage"
    )
    financial_progress: Optional[str] = Field(
        None, description="Financial progress percentage"
    )
    
    # Project details
    implementing_agency: Optional[str] = Field(
        None, description="Implementing agency name"
    )
    borrower: Optional[str] = Field(
        None, description="Borrower/contractor information"
    )
    sector: Optional[str] = Field(
        None, description="Project sector classification"
    )
    major_theme: Optional[str] = Field(
        None, description="Major theme of the project"
    )
    
    # Documentation
    project_url: Optional[AnyUrl] = Field(
        None, description="URL to project documentation"
    )
    project_document_url: Optional[AnyUrl] = Field(
        None, description="URL to project documents"
    )
    
    # Additional attributes
    environmental_category: Optional[str] = Field(
        None, description="Environmental category rating"
    )
    implementation_status: Optional[str] = Field(
        None, description="Implementation status"
    )
    
    # Milestones and breakdown
    milestones: Optional[List[dict]] = Field(
        None, description="Project milestones"
    )
    yearly_budget_breakdown: Optional[List[dict]] = Field(
        None, description="Yearly budget breakdown"
    )
    cost_overruns: Optional[dict] = Field(
        None, description="Cost overrun information"
    )
    reports: Optional[List[dict]] = Field(
        None, description="Project reports"
    )
    verification_documents: Optional[List[str]] = Field(
        None, description="Verification documents"
    )


class Project(Entity):
    """Project entity. Projects for development, infrastructure, etc."""

    type: Literal["project"] = Field(
        default="project", description="Entity type, always project"
    )
    sub_type: Optional[EntitySubType] = Field(
        default=EntitySubType.DEVELOPMENT_PROJECT,
        description="Project subtype classification"
    )
    project_details: Optional[ProjectDetails] = Field(
        None, description="Project-specific details"
    )