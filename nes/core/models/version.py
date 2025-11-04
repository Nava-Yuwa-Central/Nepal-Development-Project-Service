"""Version models using Pydantic."""

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, computed_field, field_validator

from nes.core.identifiers import build_version_id

from ..constraints import MAX_SLUG_LENGTH, MIN_SLUG_LENGTH, SLUG_PATTERN


class Actor(BaseModel):
    slug: str = Field(
        ...,
        min_length=MIN_SLUG_LENGTH,
        max_length=MAX_SLUG_LENGTH,
        pattern=SLUG_PATTERN,
        description="URL-friendly identifier for the actor",
    )
    name: Optional[str] = None

    @computed_field
    @property
    def id(self) -> str:
        return f"actor:{self.slug}"


VersionType = Literal["ENTITY", "RELATIONSHIP"]


class VersionSummary(BaseModel):
    model_config = {"extra": "forbid"}

    entityOrRelationshipId: str = Field(
        ..., description="ID of the entity or relationship this version belongs to"
    )
    type: VersionType
    versionNumber: int
    actor: Actor
    changeDescription: str
    createdAt: datetime

    @computed_field
    @property
    def id(self) -> str:
        return build_version_id(self.entityOrRelationshipId, self.versionNumber)


class Version(VersionSummary):
    changes: Optional[Dict[str, Any]] = None
    snapshot: Optional[Dict[str, Any]] = None
