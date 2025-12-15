"""Tests for Project models in nes."""

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from nes.core.models.base import Name, NameKind
from nes.core.models.entity import EntitySubType
from nes.core.models.project import (
    CrossCuttingTag,
    DonorExtension,
    FinancingComponent,
    FinancingInstrument,
    FinancingInstrumentType,
    Project,
    ProjectDateEvent,
    ProjectLocation,
    ProjectStage,
    SectorMapping,
)
from nes.core.models.version import Author, VersionSummary, VersionType


def test_project_basic_creation():
    """Test creating a basic Project entity."""

    project = Project(
        slug="test-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Test Project"})],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/test-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert project.type == "project"
    assert project.sub_type == EntitySubType.DEVELOPMENT_PROJECT
    assert project.slug == "test-project"
    assert project.id == "entity:project/development_project/test-project"
    assert project.stage == ProjectStage.UNKNOWN
    # Get the primary name from the names list
    primary_name = next((name for name in project.names if name.kind == NameKind.PRIMARY), None)
    assert primary_name is not None
    assert primary_name.en.full == "Test Project"


def test_project_with_stage():
    """Test creating a Project with specific stage."""

    project = Project(
        slug="ongoing-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Ongoing Project"})],
        stage=ProjectStage.ONGOING,
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/ongoing-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert project.stage == ProjectStage.ONGOING


def test_project_with_agencies():
    """Test creating a Project with implementing and executing agencies."""

    project = Project(
        slug="agency-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Agency Project"})],
        implementing_agency="Test Implementing Agency",
        executing_agency="Test Executing Agency",
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/agency-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert project.implementing_agency == "Test Implementing Agency"
    assert project.executing_agency == "Test Executing Agency"


def test_project_financing_instrument():
    """Test FinancingInstrument model."""

    instrument = FinancingInstrument(
        instrument_type=FinancingInstrumentType.GRANT,
        currency="USD",
        amount=1000000.0,
        interest_rate=0.0,
        repayment_period_years=20,
        grace_period_years=5,
        tying_status="Tied"
    )

    assert instrument.instrument_type == FinancingInstrumentType.GRANT
    assert instrument.currency == "USD"
    assert instrument.amount == 1000000.0
    assert instrument.interest_rate == 0.0
    assert instrument.repayment_period_years == 20
    assert instrument.grace_period_years == 5
    assert instrument.tying_status == "Tied"


def test_project_financing_component():
    """Test FinancingComponent model."""

    component = FinancingComponent(
        name="Main Component",
        financing=FinancingInstrument(
            instrument_type=FinancingInstrumentType.LOAN,
            currency="USD",
            amount=500000.0
        )
    )

    assert component.name == "Main Component"
    assert component.financing.instrument_type == FinancingInstrumentType.LOAN
    assert component.financing.amount == 500000.0


def test_project_with_financing():
    """Test creating a Project with financing information."""

    project = Project(
        slug="financed-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Financed Project"})],
        financing=[
            FinancingComponent(
                name="Grant Component",
                financing=FinancingInstrument(
                    instrument_type=FinancingInstrumentType.GRANT,
                    currency="USD",
                    amount=750000.0
                )
            ),
            FinancingComponent(
                name="Loan Component",
                financing=FinancingInstrument(
                    instrument_type=FinancingInstrumentType.LOAN,
                    currency="USD",
                    amount=250000.0
                )
            )
        ],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/financed-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert len(project.financing) == 2
    assert project.financing[0].name == "Grant Component"
    assert project.financing[0].financing.instrument_type == FinancingInstrumentType.GRANT
    assert project.financing[1].name == "Loan Component"
    assert project.financing[1].financing.instrument_type == FinancingInstrumentType.LOAN


def test_project_date_event():
    """Test ProjectDateEvent model."""

    event = ProjectDateEvent(
        date=date(2023, 1, 15),
        type="APPROVAL",
        source="MoF DFMIS"
    )

    assert event.date == date(2023, 1, 15)
    assert event.type == "APPROVAL"
    assert event.source == "MoF DFMIS"


def test_project_with_dates():
    """Test creating a Project with date events."""

    project = Project(
        slug="dated-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Dated Project"})],
        dates=[
            ProjectDateEvent(
                date=date(2023, 1, 15),
                type="APPROVAL",
                source="MoF DFMIS"
            ),
            ProjectDateEvent(
                date=date(2023, 3, 1),
                type="START",
                source="MoF DFMIS"
            ),
            ProjectDateEvent(
                date=date(2025, 12, 31),
                type="COMPLETION",
                source="MoF DFMIS"
            )
        ],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/dated-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert len(project.dates) == 3
    assert project.dates[0].type == "APPROVAL"
    assert project.dates[1].type == "START"
    assert project.dates[2].type == "COMPLETION"


def test_project_location():
    """Test ProjectLocation model."""

    location = ProjectLocation(
        latitude=27.7172,
        longitude=85.3240,
        province="Bagmati Province",
        district="Kathmandu",
        municipality="Kathmandu Metropolitan City",
        ward="12"
    )

    assert location.latitude == 27.7172
    assert location.longitude == 85.3240
    assert location.province == "Bagmati Province"
    assert location.district == "Kathmandu"
    assert location.municipality == "Kathmandu Metropolitan City"
    assert location.ward == "12"


def test_project_with_locations():
    """Test creating a Project with location information."""

    project = Project(
        slug="located-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Located Project"})],
        locations=[
            ProjectLocation(
                latitude=27.7172,
                longitude=85.3240,
                province="Bagmati Province",
                district="Kathmandu",
                municipality="Kathmandu Metropolitan City"
            )
        ],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/located-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert len(project.locations) == 1
    assert project.locations[0].latitude == 27.7172
    assert project.locations[0].longitude == 85.3240
    assert project.locations[0].district == "Kathmandu"


def test_sector_mapping():
    """Test SectorMapping model."""

    sector = SectorMapping(
        normalized_sector="Education",
        donor_sector="Primary Education",
        donor_subsector="School Infrastructure",
        donor="World Bank"
    )

    assert sector.normalized_sector == "Education"
    assert sector.donor_sector == "Primary Education"
    assert sector.donor_subsector == "School Infrastructure"
    assert sector.donor == "World Bank"


def test_project_with_sectors():
    """Test creating a Project with sector mappings."""

    project = Project(
        slug="sector-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Sector Project"})],
        sectors=[
            SectorMapping(
                normalized_sector="Education",
                donor_sector="Primary Education",
                donor_subsector="School Infrastructure"
            ),
            SectorMapping(
                normalized_sector="Health",
                donor_sector="Healthcare",
                donor="ADB"
            )
        ],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/sector-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert len(project.sectors) == 2
    assert project.sectors[0].normalized_sector == "Education"
    assert project.sectors[1].normalized_sector == "Health"


def test_cross_cutting_tag():
    """Test CrossCuttingTag model."""

    tag = CrossCuttingTag(
        category="GENDER",
        normalized_tag="Gender Equality",
        donor_tag="Women Empowerment",
        donor="ADF"
    )

    assert tag.category == "GENDER"
    assert tag.normalized_tag == "Gender Equality"
    assert tag.donor_tag == "Women Empowerment"
    assert tag.donor == "ADF"


def test_project_with_tags():
    """Test creating a Project with cross-cutting tags."""

    project = Project(
        slug="tagged-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Tagged Project"})],
        tags=[
            CrossCuttingTag(
                category="GENDER",
                normalized_tag="Gender Equality",
                donor_tag="Women Empowerment"
            ),
            CrossCuttingTag(
                category="CLIMATE",
                normalized_tag="Climate Adaptation",
                donor_tag="Environmental Resilience"
            )
        ],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/tagged-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert len(project.tags) == 2
    assert project.tags[0].category == "GENDER"
    assert project.tags[1].category == "CLIMATE"


def test_donor_extension():
    """Test DonorExtension model."""

    extension = DonorExtension(
        donor="World Bank",
        donor_project_id="WB-12345",
        raw_payload={"original_field": "original_value"}
    )

    assert extension.donor == "World Bank"
    assert extension.donor_project_id == "WB-12345"
    assert extension.raw_payload == {"original_field": "original_value"}


def test_project_with_donor_extensions():
    """Test creating a Project with donor extensions."""

    project = Project(
        slug="donor-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "Donor Project"})],
        donors=["World Bank", "ADB"],
        donor_extensions=[
            DonorExtension(
                donor="World Bank",
                donor_project_id="WB-12345",
                raw_payload={"wb_field": "wb_value"}
            ),
            DonorExtension(
                donor="ADB",
                donor_project_id="ADB-67890",
                raw_payload={"adb_field": "adb_value"}
            )
        ],
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/donor-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert project.donors == ["World Bank", "ADB"]
    assert len(project.donor_extensions) == 2
    assert project.donor_extensions[0].donor == "World Bank"
    assert project.donor_extensions[1].donor == "ADB"


def test_project_with_url():
    """Test creating a Project with a URL."""

    project = Project(
        slug="url-project",
        names=[Name(kind=NameKind.PRIMARY, en={"full": "URL Project"})],
        project_url="https://dfims.mof.gov.np/projects/123",
        version_summary=VersionSummary(
            entity_or_relationship_id="entity:project/development_project/url-project",
            type=VersionType.ENTITY,
            version_number=1,
            author=Author(slug="system"),
            change_description="Initial",
            created_at=datetime.now(UTC),
        ),
        created_at=datetime.now(UTC),
    )

    assert str(project.project_url) == "https://dfims.mof.gov.np/projects/123"