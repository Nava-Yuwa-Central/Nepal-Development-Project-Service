"""Entity model using Pydantic."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import (BaseModel, Field, TypeAdapter, computed_field,
                      field_validator)

from nes.core.identifiers import build_entity_id

from ..constraints import (ENTITY_SUBTYPE_PATTERN, ENTITY_TYPE_PATTERN,
                           MAX_DESCRIPTION_LENGTH,
                           MAX_SHORT_DESCRIPTION_LENGTH, MAX_SLUG_LENGTH,
                           MAX_SUBTYPE_LENGTH, MAX_TYPE_LENGTH,
                           MIN_SLUG_LENGTH, SLUG_PATTERN)
from .base import ContactInfo, Name
from .person import Education, Position
from .version import VersionSummary

EntityType = Literal["person", "organization"]
ENTITY_TYPES = ["person", "organization"]


class GovernmentType(str, Enum):
    """Types of government entities."""

    FEDERAL = "federal"
    STATE = "state"
    LOCAL = "local"


class Entity(BaseModel):
    """Base entity model. At least one name with kind='DEFAULT' should be provided for all entities."""

    model_config = {"extra": "forbid"}

    slug: str = Field(
        ...,
        min_length=MIN_SLUG_LENGTH,
        max_length=MAX_SLUG_LENGTH,
        pattern=SLUG_PATTERN,
        description="URL-friendly identifier for the entity",
    )
    type: EntityType = Field(
        ...,
        max_length=MAX_TYPE_LENGTH,
        pattern=ENTITY_TYPE_PATTERN,
        description=f"Type of entity {ENTITY_TYPES}",
    )
    subType: Optional[str] = Field(
        None,
        max_length=MAX_SUBTYPE_LENGTH,
        pattern=ENTITY_SUBTYPE_PATTERN,
        description="Subtype classification for the entity",
    )
    names: List[Name] = Field(
        ..., description="List of names associated with the entity"
    )
    misspelledNames: Optional[List[Name]] = Field(
        None, description="List of misspelled or alternative name variations"
    )
    versionSummary: VersionSummary = Field(
        ..., description="Summary of the latest version information"
    )
    createdAt: datetime = Field(
        ..., description="Timestamp when the entity was created"
    )
    identifiers: Optional[Dict[str, Any]] = Field(
        None, description="External identifiers for the entity"
    )
    tags: Optional[List[str]] = Field(
        None, description="Tags for categorizing the entity"
    )
    attributes: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional attributes for the entity. Keys with 'sys:' prefix are reserved for special system fields.",
    )
    contacts: Optional[List[ContactInfo]] = Field(
        None, description="Contact information for the entity"
    )
    short_description: Optional[str] = Field(
        None,
        max_length=MAX_SHORT_DESCRIPTION_LENGTH,
        description="Brief description of the entity",
    )
    description: Optional[str] = Field(
        None,
        max_length=MAX_DESCRIPTION_LENGTH,
        description="Detailed description of the entity",
    )
    attributions: Optional[List[str]] = Field(
        None, description="Sources and attributions for the entity data"
    )

    @computed_field
    @property
    def id(self) -> str:
        return build_entity_id(self.type, self.subType, self.slug)

    @field_validator("names")
    @classmethod
    def validate_names(cls, v):
        if not any(name.kind == "DEFAULT" for name in v):
            raise ValueError('At least one name with kind="DEFAULT" is required')

        return v

    def _get_sys_attribute(self, key: str, type_hint):
        """Get a system attribute and cast to the specified type."""
        if not self.attributes:
            return None
        value = self.attributes.get(f"sys:{key}")
        if value is None:
            return None
        return TypeAdapter(type_hint).validate_python(value, strict=True)

    def _set_sys_attribute(self, key: str, value):
        """Set a system attribute."""
        if self.attributes is None:
            self.attributes = {}
        if value is None:
            self.attributes.pop(f"sys:{key}", None)
        else:
            self.attributes[f"sys:{key}"] = value

    @classmethod
    def _sys_prop(cls, key: str, type_hint):
        """Create a property for a system attribute with automatic getter/setter."""
        return property(
            lambda self: self._get_sys_attribute(key, type_hint),
            lambda self, value: self._set_sys_attribute(key, value),
        )


class Person(Entity):
    type: Literal["person"] = Field(
        default="person", description="Entity type, always person"
    )

    education = Entity._sys_prop("education", List[Education])
    positions = Entity._sys_prop("positions", List[Position])


class Organization(Entity):
    type: Literal["organization"] = Field(
        default="organization", description="Entity type, always organization"
    )


class PoliticalParty(Organization):
    subType: Literal["political_party"] = Field(
        default="political_party",
        description="Organization subtype, always political_party",
    )


class GovernmentBody(Organization):
    subType: Literal["government"] = Field(
        default="government",
        description="Organization subtype, always government",
    )

    # Type of government (federal, state, local)
    governmentType = Entity._sys_prop("governmentType", Optional[GovernmentType])


# Entity type/subtype to class mapping
# NOTE: When adding new subclasses of Entity, this dict must be updated for deserialization to work properly
ENTITY_TYPE_MAP = {
    "person": {
        None: Person,
    },
    "organization": {
        None: Organization,
        "political_party": PoliticalParty,
        "government": GovernmentBody,
    },
}
