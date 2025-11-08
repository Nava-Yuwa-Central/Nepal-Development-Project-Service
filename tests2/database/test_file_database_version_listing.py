"""Tests for FileDatabase version listing capabilities in nes2.

Following TDD approach: Write failing tests first (Red phase).
These tests define the expected behavior of version listing functionality.

Test Coverage:
- Listing versions by entity
- Listing versions by relationship
- Version filtering
- Efficient version retrieval
"""

import pytest
from datetime import datetime, date, UTC

from nes2.core.models.base import Name, NameKind
from nes2.core.models.person import Person
from nes2.core.models.organization import PoliticalParty
from nes2.core.models.relationship import Relationship
from nes2.core.models.version import Actor, Version, VersionSummary, VersionType


class TestListVersionsByEntity:
    """Test listing versions by entity ID."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database populated with entities and their versions."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entity
        entity = Person(
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
        )
        
        # Create multiple versions for the entity
        versions = [
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial import",
                created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={
                    "slug": "ram-chandra-poudel",
                    "type": "person",
                    "names": [{"kind": "PRIMARY", "en": {"full": "Ram Chandra Poudel"}}]
                }
            ),
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=2,
                actor=Actor(slug="data-maintainer", name="Data Maintainer"),
                change_description="Updated party affiliation",
                created_at=datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={
                    "slug": "ram-chandra-poudel",
                    "type": "person",
                    "names": [{"kind": "PRIMARY", "en": {"full": "Ram Chandra Poudel"}}],
                    "attributes": {"party": "nepali-congress"}
                }
            ),
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=3,
                actor=Actor(slug="data-maintainer", name="Data Maintainer"),
                change_description="Added constituency information",
                created_at=datetime(2024, 3, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={
                    "slug": "ram-chandra-poudel",
                    "type": "person",
                    "names": [{"kind": "PRIMARY", "en": {"full": "Ram Chandra Poudel"}}],
                    "attributes": {"party": "nepali-congress", "constituency": "Tanahun-1"}
                }
            )
        ]
        
        # Create another entity with versions
        another_entity = Person(
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
        )
        
        another_version = Version(
            entity_or_relationship_id="entity:person/sher-bahadur-deuba",
            type=VersionType.ENTITY,
            version_number=1,
            actor=Actor(slug="system-importer", name="System Importer"),
            change_description="Initial import",
            created_at=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
            snapshot={
                "slug": "sher-bahadur-deuba",
                "type": "person",
                "names": [{"kind": "PRIMARY", "en": {"full": "Sher Bahadur Deuba"}}]
            }
        )
        
        # Store all entities and versions
        async def populate():
            await db.put_entity(entity)
            await db.put_entity(another_entity)
            for version in versions:
                await db.put_version(version)
            await db.put_version(another_version)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_list_versions_by_entity_id(self, populated_db):
        """Test that list_versions_by_entity can find all versions for an entity."""
        # This test will fail until list_versions_by_entity method is implemented
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel"
        )
        
        # Should find 3 versions for Ram Chandra Poudel
        assert len(results) == 3
        assert all(v.entity_or_relationship_id == "entity:person/ram-chandra-poudel" for v in results)
        assert all(v.type == VersionType.ENTITY for v in results)

    @pytest.mark.asyncio
    async def test_list_versions_by_entity_returns_chronological_order(self, populated_db):
        """Test that versions are returned in chronological order (oldest first)."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel"
        )
        
        # Should be in chronological order by version number
        assert len(results) == 3
        assert results[0].version_number == 1
        assert results[1].version_number == 2
        assert results[2].version_number == 3

    @pytest.mark.asyncio
    async def test_list_versions_by_entity_returns_empty_for_no_versions(self, populated_db):
        """Test that list_versions_by_entity returns empty list when entity has no versions."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/nonexistent-person"
        )
        
        # Should return empty list
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_list_versions_by_entity_with_pagination(self, populated_db):
        """Test that list_versions_by_entity supports pagination."""
        # Get first page
        page1 = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            limit=2,
            offset=0
        )
        
        # Get second page
        page2 = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            limit=2,
            offset=2
        )
        
        # Should have different versions
        assert len(page1) == 2
        assert len(page2) == 1
        assert page1[0].version_number == 1
        assert page1[1].version_number == 2
        assert page2[0].version_number == 3


class TestListVersionsByRelationship:
    """Test listing versions by relationship ID."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database populated with relationships and their versions."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create relationship
        relationship = Relationship(
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
        )
        
        # Create multiple versions for the relationship
        versions = [
            Version(
                entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                type=VersionType.RELATIONSHIP,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial relationship",
                created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={
                    "source_entity_id": "entity:person/ram-chandra-poudel",
                    "target_entity_id": "entity:organization/political_party/nepali-congress",
                    "type": "MEMBER_OF",
                    "start_date": "2000-01-01"
                }
            ),
            Version(
                entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                type=VersionType.RELATIONSHIP,
                version_number=2,
                actor=Actor(slug="data-maintainer", name="Data Maintainer"),
                change_description="Added position attribute",
                created_at=datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={
                    "source_entity_id": "entity:person/ram-chandra-poudel",
                    "target_entity_id": "entity:organization/political_party/nepali-congress",
                    "type": "MEMBER_OF",
                    "start_date": "2000-01-01",
                    "attributes": {"position": "President"}
                }
            )
        ]
        
        # Store relationship and versions
        async def populate():
            await db.put_relationship(relationship)
            for version in versions:
                await db.put_version(version)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_list_versions_by_relationship_id(self, populated_db):
        """Test that list_versions_by_relationship can find all versions for a relationship."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF"
        )
        
        # Should find 2 versions for the relationship
        assert len(results) == 2
        assert all(v.entity_or_relationship_id == "relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF" for v in results)
        assert all(v.type == VersionType.RELATIONSHIP for v in results)

    @pytest.mark.asyncio
    async def test_list_versions_by_relationship_returns_chronological_order(self, populated_db):
        """Test that relationship versions are returned in chronological order."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF"
        )
        
        # Should be in chronological order by version number
        assert len(results) == 2
        assert results[0].version_number == 1
        assert results[1].version_number == 2
        assert results[0].created_at < results[1].created_at


class TestVersionFiltering:
    """Test filtering versions by various criteria."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with versions from different actors and time periods."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create versions with different actors and dates
        versions = [
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial import",
                created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={"version": 1}
            ),
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=2,
                actor=Actor(slug="data-maintainer", name="Data Maintainer"),
                change_description="Updated by maintainer",
                created_at=datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={"version": 2}
            ),
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=3,
                actor=Actor(slug="data-maintainer", name="Data Maintainer"),
                change_description="Another update by maintainer",
                created_at=datetime(2024, 3, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={"version": 3}
            ),
            Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=4,
                actor=Actor(slug="automated-scraper", name="Automated Scraper"),
                change_description="Automated update from scraper",
                created_at=datetime(2024, 4, 1, 0, 0, 0, tzinfo=UTC),
                snapshot={"version": 4}
            )
        ]
        
        # Store all versions
        async def populate():
            for version in versions:
                await db.put_version(version)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_filter_versions_by_actor(self, populated_db):
        """Test filtering versions by actor slug."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            actor_slug="data-maintainer"
        )
        
        # Should find 2 versions by data-maintainer
        assert len(results) == 2
        assert all(v.actor.slug == "data-maintainer" for v in results)
        assert results[0].version_number == 2
        assert results[1].version_number == 3

    @pytest.mark.asyncio
    async def test_filter_versions_by_date_range(self, populated_db):
        """Test filtering versions by date range."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            created_after=datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC),
            created_before=datetime(2024, 4, 1, 0, 0, 0, tzinfo=UTC)
        )
        
        # Should find versions 2 and 3 (created in Feb and Mar)
        assert len(results) == 2
        assert results[0].version_number == 2
        assert results[1].version_number == 3

    @pytest.mark.asyncio
    async def test_filter_versions_created_after(self, populated_db):
        """Test filtering versions created after a specific date."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            created_after=datetime(2024, 2, 15, 0, 0, 0, tzinfo=UTC)
        )
        
        # Should find versions 3 and 4 (created after Feb 15)
        assert len(results) == 2
        assert results[0].version_number == 3
        assert results[1].version_number == 4

    @pytest.mark.asyncio
    async def test_filter_versions_created_before(self, populated_db):
        """Test filtering versions created before a specific date."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            created_before=datetime(2024, 2, 15, 0, 0, 0, tzinfo=UTC)
        )
        
        # Should find versions 1 and 2 (created before Feb 15)
        assert len(results) == 2
        assert results[0].version_number == 1
        assert results[1].version_number == 2

    @pytest.mark.asyncio
    async def test_filter_versions_by_version_number_range(self, populated_db):
        """Test filtering versions by version number range."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            min_version=2,
            max_version=3
        )
        
        # Should find versions 2 and 3
        assert len(results) == 2
        assert results[0].version_number == 2
        assert results[1].version_number == 3


class TestEfficientVersionRetrieval:
    """Test efficient version retrieval patterns."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with many versions for performance testing."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create many versions for multiple entities
        versions = []
        
        # Entity 1: 10 versions
        for i in range(1, 11):
            versions.append(
                Version(
                    entity_or_relationship_id="entity:person/ram-chandra-poudel",
                    type=VersionType.ENTITY,
                    version_number=i,
                    actor=Actor(slug="system"),
                    change_description=f"Update {i}",
                    created_at=datetime(2024, 1, i, 0, 0, 0, tzinfo=UTC),
                    snapshot={"version": i}
                )
            )
        
        # Entity 2: 5 versions
        for i in range(1, 6):
            versions.append(
                Version(
                    entity_or_relationship_id="entity:person/sher-bahadur-deuba",
                    type=VersionType.ENTITY,
                    version_number=i,
                    actor=Actor(slug="system"),
                    change_description=f"Update {i}",
                    created_at=datetime(2024, 2, i, 0, 0, 0, tzinfo=UTC),
                    snapshot={"version": i}
                )
            )
        
        # Relationship: 3 versions
        for i in range(1, 4):
            versions.append(
                Version(
                    entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                    type=VersionType.RELATIONSHIP,
                    version_number=i,
                    actor=Actor(slug="system"),
                    change_description=f"Update {i}",
                    created_at=datetime(2024, 3, i, 0, 0, 0, tzinfo=UTC),
                    snapshot={"version": i}
                )
            )
        
        # Store all versions
        async def populate():
            for version in versions:
                await db.put_version(version)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_get_latest_version_efficiently(self, populated_db):
        """Test retrieving only the latest version efficiently."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            limit=1,
            order="desc"
        )
        
        # Should return only the latest version
        assert len(results) == 1
        assert results[0].version_number == 10

    @pytest.mark.asyncio
    async def test_get_specific_version_number(self, populated_db):
        """Test retrieving a specific version number efficiently."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            min_version=5,
            max_version=5
        )
        
        # Should return only version 5
        assert len(results) == 1
        assert results[0].version_number == 5

    @pytest.mark.asyncio
    async def test_list_versions_with_limit(self, populated_db):
        """Test that limit parameter works correctly."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            limit=3
        )
        
        # Should return only 3 versions
        assert len(results) == 3
        assert results[0].version_number == 1
        assert results[1].version_number == 2
        assert results[2].version_number == 3

    @pytest.mark.asyncio
    async def test_list_versions_with_offset(self, populated_db):
        """Test that offset parameter works correctly."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            limit=3,
            offset=5
        )
        
        # Should return versions 6, 7, 8
        assert len(results) == 3
        assert results[0].version_number == 6
        assert results[1].version_number == 7
        assert results[2].version_number == 8

    @pytest.mark.asyncio
    async def test_count_versions_for_entity(self, populated_db):
        """Test counting total versions for an entity."""
        # Get all versions to count them
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            limit=1000
        )
        
        # Should have 10 versions
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_list_versions_by_type_entity(self, populated_db):
        """Test filtering versions by type (entity vs relationship)."""
        # This would require a new method or parameter
        # For now, we test that we can distinguish entity versions
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="entity:person/ram-chandra-poudel"
        )
        
        # All should be entity versions
        assert len(results) == 10
        assert all(v.type == VersionType.ENTITY for v in results)

    @pytest.mark.asyncio
    async def test_list_versions_by_type_relationship(self, populated_db):
        """Test filtering versions by type (relationship)."""
        results = await populated_db.list_versions_by_entity(
            entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF"
        )
        
        # All should be relationship versions
        assert len(results) == 3
        assert all(v.type == VersionType.RELATIONSHIP for v in results)
