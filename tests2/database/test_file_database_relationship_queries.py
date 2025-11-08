"""Tests for FileDatabase relationship querying capabilities in nes2.

Following TDD approach: Write failing tests first (Red phase).
These tests define the expected behavior of relationship querying functionality.

Test Coverage:
- Listing relationships by entity (source or target)
- Listing relationships by type
- Temporal filtering (date ranges)
- Bidirectional queries
"""

import pytest
from datetime import datetime, date, UTC

from nes2.core.models.base import Name, NameKind
from nes2.core.models.person import Person
from nes2.core.models.organization import PoliticalParty
from nes2.core.models.location import Location
from nes2.core.models.entity import EntitySubType
from nes2.core.models.relationship import Relationship
from nes2.core.models.version import Actor, VersionSummary, VersionType


class TestListRelationshipsByEntity:
    """Test listing relationships by entity (source or target)."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database populated with entities and relationships."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities
        entities = [
            Person(
                slug="ram-chandra-poudel",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Ram Chandra Poudel"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/ram-chandra-poudel",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Person(
                slug="sher-bahadur-deuba",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Sher Bahadur Deuba"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/sher-bahadur-deuba",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            PoliticalParty(
                slug="nepali-congress",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Nepali Congress"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:organization/political_party/nepali-congress",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Location(
                slug="kathmandu-metropolitan-city",
                sub_type=EntitySubType.METROPOLITAN_CITY,
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Kathmandu"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Create relationships
        relationships = [
            # Ram Chandra Poudel -> Nepali Congress (MEMBER_OF)
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                start_date=date(2000, 1, 1),
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # Sher Bahadur Deuba -> Nepali Congress (MEMBER_OF)
            Relationship(
                source_entity_id="entity:person/sher-bahadur-deuba",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                start_date=date(1990, 1, 1),
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/sher-bahadur-deuba:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # Ram Chandra Poudel -> Kathmandu (LOCATED_IN)
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                type="LOCATED_IN",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:location/metropolitan_city/kathmandu-metropolitan-city:LOCATED_IN",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Store all entities and relationships
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
            for relationship in relationships:
                await db.put_relationship(relationship)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_list_relationships_by_source_entity(self, populated_db):
        """Test that list_relationships_by_entity can find relationships by source entity."""
        # This test will fail until list_relationships_by_entity method is implemented
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel"
        )
        
        # Should find 2 relationships where Ram Chandra Poudel is the source
        assert len(results) == 2
        assert all(r.source_entity_id == "entity:person/ram-chandra-poudel" for r in results)

    @pytest.mark.asyncio
    async def test_list_relationships_by_target_entity(self, populated_db):
        """Test that list_relationships_by_entity can find relationships by target entity."""
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress"
        )
        
        # Should find 2 relationships where Nepali Congress is the target
        assert len(results) == 2
        assert all(r.target_entity_id == "entity:organization/political_party/nepali-congress" for r in results)

    @pytest.mark.asyncio
    async def test_list_relationships_by_entity_returns_empty_for_no_match(self, populated_db):
        """Test that list_relationships_by_entity returns empty list when entity has no relationships."""
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/nonexistent-person"
        )
        
        # Should return empty list
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_list_relationships_by_entity_with_direction_source(self, populated_db):
        """Test filtering relationships by direction (source only)."""
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel",
            direction="source"
        )
        
        # Should only find relationships where entity is the source
        assert len(results) == 2
        assert all(r.source_entity_id == "entity:person/ram-chandra-poudel" for r in results)

    @pytest.mark.asyncio
    async def test_list_relationships_by_entity_with_direction_target(self, populated_db):
        """Test filtering relationships by direction (target only)."""
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            direction="target"
        )
        
        # Should only find relationships where entity is the target
        assert len(results) == 2
        assert all(r.target_entity_id == "entity:organization/political_party/nepali-congress" for r in results)

    @pytest.mark.asyncio
    async def test_list_relationships_by_entity_bidirectional(self, populated_db):
        """Test listing relationships in both directions (default behavior)."""
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel"
        )
        
        # Should find relationships where entity is either source or target
        assert len(results) >= 2
        assert any(r.source_entity_id == "entity:person/ram-chandra-poudel" for r in results)


class TestListRelationshipsByType:
    """Test listing relationships by relationship type."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with relationships of different types."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities
        entities = [
            Person(
                slug="ram-chandra-poudel",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Ram Chandra Poudel"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/ram-chandra-poudel",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Person(
                slug="sher-bahadur-deuba",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Sher Bahadur Deuba"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/sher-bahadur-deuba",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            PoliticalParty(
                slug="nepali-congress",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Nepali Congress"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:organization/political_party/nepali-congress",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Location(
                slug="kathmandu-metropolitan-city",
                sub_type=EntitySubType.METROPOLITAN_CITY,
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Kathmandu"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Create relationships of different types
        relationships = [
            # MEMBER_OF relationships
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Relationship(
                source_entity_id="entity:person/sher-bahadur-deuba",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/sher-bahadur-deuba:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # LOCATED_IN relationships
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                type="LOCATED_IN",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:location/metropolitan_city/kathmandu-metropolitan-city:LOCATED_IN",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Relationship(
                source_entity_id="entity:person/sher-bahadur-deuba",
                target_entity_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                type="LOCATED_IN",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/sher-bahadur-deuba:entity:location/metropolitan_city/kathmandu-metropolitan-city:LOCATED_IN",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Store all entities and relationships
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
            for relationship in relationships:
                await db.put_relationship(relationship)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_list_relationships_by_type_member_of(self, populated_db):
        """Test that list_relationships_by_type can filter by MEMBER_OF type."""
        # This test will fail until list_relationships_by_type method is implemented
        results = await populated_db.list_relationships_by_type(
            relationship_type="MEMBER_OF"
        )
        
        # Should find 2 MEMBER_OF relationships
        assert len(results) == 2
        assert all(r.type == "MEMBER_OF" for r in results)

    @pytest.mark.asyncio
    async def test_list_relationships_by_type_located_in(self, populated_db):
        """Test that list_relationships_by_type can filter by LOCATED_IN type."""
        results = await populated_db.list_relationships_by_type(
            relationship_type="LOCATED_IN"
        )
        
        # Should find 2 LOCATED_IN relationships
        assert len(results) == 2
        assert all(r.type == "LOCATED_IN" for r in results)

    @pytest.mark.asyncio
    async def test_list_relationships_by_type_returns_empty_for_no_match(self, populated_db):
        """Test that list_relationships_by_type returns empty list when no relationships match."""
        results = await populated_db.list_relationships_by_type(
            relationship_type="EMPLOYED_BY"
        )
        
        # Should return empty list
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_list_relationships_by_type_with_pagination(self, populated_db):
        """Test that list_relationships_by_type supports pagination."""
        # Get first page
        page1 = await populated_db.list_relationships_by_type(
            relationship_type="MEMBER_OF",
            limit=1,
            offset=0
        )
        
        # Get second page
        page2 = await populated_db.list_relationships_by_type(
            relationship_type="MEMBER_OF",
            limit=1,
            offset=1
        )
        
        # Should have different relationships
        assert len(page1) == 1
        assert len(page2) == 1
        assert page1[0].id != page2[0].id


class TestTemporalFiltering:
    """Test temporal filtering of relationships by date ranges."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with relationships having different date ranges."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities
        entities = [
            Person(
                slug="ram-chandra-poudel",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Ram Chandra Poudel"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/ram-chandra-poudel",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Person(
                slug="sher-bahadur-deuba",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Sher Bahadur Deuba"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/sher-bahadur-deuba",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            PoliticalParty(
                slug="nepali-congress",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Nepali Congress"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:organization/political_party/nepali-congress",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Create relationships with different temporal ranges
        relationships = [
            # Active relationship (started 2000, no end date)
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                start_date=date(2000, 1, 1),
                end_date=None,
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # Historical relationship (started 1990, ended 2010)
            Relationship(
                source_entity_id="entity:person/sher-bahadur-deuba",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                start_date=date(1990, 1, 1),
                end_date=date(2010, 12, 31),
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/sher-bahadur-deuba:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Store all entities and relationships
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
            for relationship in relationships:
                await db.put_relationship(relationship)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_filter_relationships_active_on_date(self, populated_db):
        """Test filtering relationships that were active on a specific date."""
        # This test will fail until temporal filtering is implemented
        # Query for relationships active on 2005-06-15
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            active_on=date(2005, 6, 15)
        )
        
        # Both relationships should be active on this date
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_filter_relationships_active_on_date_after_end(self, populated_db):
        """Test filtering relationships excludes those that ended before the date."""
        # Query for relationships active on 2015-01-01 (after Deuba's ended)
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            active_on=date(2015, 1, 1)
        )
        
        # Only Ram Chandra Poudel's relationship should be active
        assert len(results) == 1
        assert results[0].source_entity_id == "entity:person/ram-chandra-poudel"

    @pytest.mark.asyncio
    async def test_filter_relationships_active_on_date_before_start(self, populated_db):
        """Test filtering relationships excludes those that started after the date."""
        # Query for relationships active on 1985-01-01 (before both started)
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            active_on=date(1985, 1, 1)
        )
        
        # No relationships should be active
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_filter_relationships_by_date_range(self, populated_db):
        """Test filtering relationships by date range (start and end)."""
        # Query for relationships active between 2000-2010
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            start_date_from=date(2000, 1, 1),
            start_date_to=date(2010, 12, 31)
        )
        
        # Should find Ram Chandra Poudel's relationship (started in 2000)
        assert len(results) >= 1
        assert any(r.source_entity_id == "entity:person/ram-chandra-poudel" for r in results)

    @pytest.mark.asyncio
    async def test_filter_relationships_currently_active(self, populated_db):
        """Test filtering for currently active relationships (no end date)."""
        # Query for relationships with no end date
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            currently_active=True
        )
        
        # Should only find Ram Chandra Poudel's relationship (no end date)
        assert len(results) == 1
        assert results[0].source_entity_id == "entity:person/ram-chandra-poudel"
        assert results[0].end_date is None


class TestBidirectionalQueries:
    """Test bidirectional relationship queries."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with bidirectional relationships."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities
        entities = [
            Person(
                slug="ram-chandra-poudel",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Ram Chandra Poudel"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/ram-chandra-poudel",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Person(
                slug="sher-bahadur-deuba",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Sher Bahadur Deuba"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/sher-bahadur-deuba",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            PoliticalParty(
                slug="nepali-congress",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Nepali Congress"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:organization/political_party/nepali-congress",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Location(
                slug="kathmandu-metropolitan-city",
                sub_type=EntitySubType.METROPOLITAN_CITY,
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Kathmandu"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Create relationships in both directions
        relationships = [
            # Ram -> Nepali Congress (MEMBER_OF)
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # Sher -> Nepali Congress (MEMBER_OF)
            Relationship(
                source_entity_id="entity:person/sher-bahadur-deuba",
                target_entity_id="entity:organization/political_party/nepali-congress",
                type="MEMBER_OF",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/sher-bahadur-deuba:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # Ram -> Kathmandu (LOCATED_IN)
            Relationship(
                source_entity_id="entity:person/ram-chandra-poudel",
                target_entity_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                type="LOCATED_IN",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:location/metropolitan_city/kathmandu-metropolitan-city:LOCATED_IN",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            # Kathmandu -> Bagmati Province (LOCATED_IN) - reverse direction example
            Relationship(
                source_entity_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                target_entity_id="entity:person/ram-chandra-poudel",
                type="AFFILIATED_WITH",
                version_summary=VersionSummary(
                    entity_or_relationship_id="relationship:entity:location/metropolitan_city/kathmandu-metropolitan-city:entity:person/ram-chandra-poudel:AFFILIATED_WITH",
                    type=VersionType.RELATIONSHIP,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Store all entities and relationships
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
            for relationship in relationships:
                await db.put_relationship(relationship)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_bidirectional_query_finds_both_directions(self, populated_db):
        """Test that bidirectional query finds relationships in both directions."""
        # This test will fail until bidirectional querying is implemented
        # Query for Ram Chandra Poudel's relationships (both as source and target)
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel",
            direction="both"
        )
        
        # Should find relationships where Ram is both source and target
        assert len(results) == 3
        source_count = sum(1 for r in results if r.source_entity_id == "entity:person/ram-chandra-poudel")
        target_count = sum(1 for r in results if r.target_entity_id == "entity:person/ram-chandra-poudel")
        assert source_count == 2  # Ram as source
        assert target_count == 1  # Ram as target

    @pytest.mark.asyncio
    async def test_bidirectional_query_with_type_filter(self, populated_db):
        """Test bidirectional query with relationship type filter."""
        # Query for Ram's MEMBER_OF relationships in both directions
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel",
            direction="both",
            relationship_type="MEMBER_OF"
        )
        
        # Should only find MEMBER_OF relationships
        assert len(results) >= 1
        assert all(r.type == "MEMBER_OF" for r in results)

    @pytest.mark.asyncio
    async def test_query_incoming_relationships_only(self, populated_db):
        """Test querying only incoming relationships (entity as target)."""
        # Query for relationships where Ram is the target
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel",
            direction="target"
        )
        
        # Should only find relationships where Ram is the target
        assert len(results) == 1
        assert all(r.target_entity_id == "entity:person/ram-chandra-poudel" for r in results)

    @pytest.mark.asyncio
    async def test_query_outgoing_relationships_only(self, populated_db):
        """Test querying only outgoing relationships (entity as source)."""
        # Query for relationships where Ram is the source
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel",
            direction="source"
        )
        
        # Should only find relationships where Ram is the source
        assert len(results) == 2
        assert all(r.source_entity_id == "entity:person/ram-chandra-poudel" for r in results)

    @pytest.mark.asyncio
    async def test_bidirectional_query_for_organization(self, populated_db):
        """Test bidirectional query for an organization entity."""
        # Query for Nepali Congress relationships (should find members)
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            direction="both"
        )
        
        # Should find all relationships involving Nepali Congress
        assert len(results) == 2
        # All should have Nepali Congress as target (members pointing to party)
        assert all(r.target_entity_id == "entity:organization/political_party/nepali-congress" for r in results)

    @pytest.mark.asyncio
    async def test_combined_bidirectional_and_temporal_filtering(self, populated_db):
        """Test combining bidirectional query with temporal filtering."""
        # Add a relationship with dates for testing
        relationship_with_dates = Relationship(
            source_entity_id="entity:person/sher-bahadur-deuba",
            target_entity_id="entity:person/ram-chandra-poudel",
            type="AFFILIATED_WITH",
            start_date=date(2010, 1, 1),
            end_date=date(2020, 12, 31),
            version_summary=VersionSummary(
                entity_or_relationship_id="relationship:entity:person/sher-bahadur-deuba:entity:person/ram-chandra-poudel:AFFILIATED_WITH",
                type=VersionType.RELATIONSHIP,
                version_number=1,
                actor=Actor(slug="system"),
                change_description="Initial",
                created_at=datetime.now(UTC)
            ),
            created_at=datetime.now(UTC)
        )
        await populated_db.put_relationship(relationship_with_dates)
        
        # Query for Ram's relationships active on 2015-06-15
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:person/ram-chandra-poudel",
            direction="both",
            active_on=date(2015, 6, 15)
        )
        
        # Should find relationships active on that date
        assert len(results) >= 1
        # Should include the relationship with Sher (Ram as target)
        assert any(
            r.source_entity_id == "entity:person/sher-bahadur-deuba" and 
            r.target_entity_id == "entity:person/ram-chandra-poudel"
            for r in results
        )


class TestRelationshipQueryPagination:
    """Test pagination in relationship queries."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with many relationships for pagination testing."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities
        entities = [
            PoliticalParty(
                slug="nepali-congress",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Nepali Congress"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:organization/political_party/nepali-congress",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        # Create 10 person entities
        for i in range(10):
            entities.append(
                Person(
                    slug=f"person-{i}",
                    names=[Name(kind=NameKind.PRIMARY, en={"full": f"Person {i}"})],
                    version_summary=VersionSummary(
                        entity_or_relationship_id=f"entity:person/person-{i}",
                        type=VersionType.ENTITY,
                        version_number=1,
                        actor=Actor(slug="system"),
                        change_description="Initial",
                        created_at=datetime.now(UTC)
                    ),
                    created_at=datetime.now(UTC)
                )
            )
        
        # Create 10 MEMBER_OF relationships
        relationships = []
        for i in range(10):
            relationships.append(
                Relationship(
                    source_entity_id=f"entity:person/person-{i}",
                    target_entity_id="entity:organization/political_party/nepali-congress",
                    type="MEMBER_OF",
                    version_summary=VersionSummary(
                        entity_or_relationship_id=f"relationship:entity:person/person-{i}:entity:organization/political_party/nepali-congress:MEMBER_OF",
                        type=VersionType.RELATIONSHIP,
                        version_number=1,
                        actor=Actor(slug="system"),
                        change_description="Initial",
                        created_at=datetime.now(UTC)
                    ),
                    created_at=datetime.now(UTC)
                )
            )
        
        # Store all entities and relationships
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
            for relationship in relationships:
                await db.put_relationship(relationship)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_relationship_query_with_limit(self, populated_db):
        """Test that relationship queries respect limit parameter."""
        results = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            limit=5
        )
        
        # Should return at most 5 results
        assert len(results) <= 5

    @pytest.mark.asyncio
    async def test_relationship_query_with_offset(self, populated_db):
        """Test that relationship queries respect offset parameter."""
        # Get first page
        page1 = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            limit=3,
            offset=0
        )
        
        # Get second page
        page2 = await populated_db.list_relationships_by_entity(
            entity_id="entity:organization/political_party/nepali-congress",
            limit=3,
            offset=3
        )
        
        # Pages should have different relationships
        page1_ids = [r.id for r in page1]
        page2_ids = [r.id for r in page2]
        assert len(set(page1_ids) & set(page2_ids)) == 0

    @pytest.mark.asyncio
    async def test_relationship_type_query_pagination(self, populated_db):
        """Test pagination in list_relationships_by_type."""
        # Get all results
        all_results = await populated_db.list_relationships_by_type(
            relationship_type="MEMBER_OF",
            limit=100
        )
        
        # Get results in pages
        page1 = await populated_db.list_relationships_by_type(
            relationship_type="MEMBER_OF",
            limit=5,
            offset=0
        )
        page2 = await populated_db.list_relationships_by_type(
            relationship_type="MEMBER_OF",
            limit=5,
            offset=5
        )
        
        # Combined pages should cover the results
        combined_ids = [r.id for r in page1] + [r.id for r in page2]
        all_ids = [r.id for r in all_results]
        
        # All IDs from pages should be in the full result set
        assert all(id in all_ids for id in combined_ids)
