"""Relationship model using Pydantic."""

from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, computed_field, field_validator

from nes.core.identifiers import build_relationship_id, validate_entity_id

from .version import VersionSummary

RelationshipType = Literal[
    "AFFILIATED_WITH",
    "EMPLOYED_BY",
    "MEMBER_OF",
    "PARENT_OF",
    "CHILD_OF",
    "SUPERVISES",
    "LOCATED_IN",
]


class Relationship(BaseModel):
    model_config = {"extra": "forbid"}

    sourceEntityId: str
    targetEntityId: str
    type: RelationshipType

    startDate: Optional[date] = None
    endDate: Optional[date] = None

    attributes: Optional[Dict[str, Any]] = None

    versionSummary: Optional[VersionSummary] = Field(
        None, description="Summary of the latest version information"
    )
    createdAt: Optional[datetime] = None
    attributions: Optional[List[str]] = Field(
        None, description="Sources and attributions for the relationship data"
    )

    @field_validator("sourceEntityId", "targetEntityId")
    @classmethod
    def validate_entity_ids(cls, v):
        return validate_entity_id(v)

    @computed_field
    @property
    def id(self) -> str:
        return build_relationship_id(
            self.sourceEntityId, self.targetEntityId, self.type
        )
