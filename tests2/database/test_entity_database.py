"""Tests for EntityDatabase abstract interface in nes2.

Following TDD approach: Write failing tests first (Red phase).
These tests define the expected behavior of the EntityDatabase interface.
"""

import pytest
from abc import ABC
from datetime import datetime, UTC, date

from nes2.core.models.base import Name, NameKind
from nes2.core.models.person import Person
from nes2.core.models.organization import PoliticalParty
from nes2.core.models.location import Location
from nes2.core.models.entity import EntitySubType
from nes2.core.models.relationship import Relationship
from nes2.core.models.version import Actor, Version, VersionSummary, VersionType


class TestEntityDatabaseInterface:
    """Test that EntityDatabase defines the correct abstract interface."""

    def test_entity_database_is_abstract(self):
        """Test that EntityDatabase cannot be instantiated directly."""
        from nes2.database.entity_database import EntityDatabase
        
        # Should not be able to instantiate abstract class
        with pytest.raises(TypeError):
            EntityDatabase()

    def test_entity_database_has_required_methods(self):
        """Test that EntityDatabase defines all required abstract methods."""
        from nes2.database.entity_database import EntityDatabase
        
        # Check that all required methods are defined
        required_methods = [
            'put_entity',
            'get_entity',
            'delete_entity',
            'list_entities',
            'put_relationship',
            'get_relationship',
            'delete_relationship',
            'list_relationships',
            'put_version',
            'get_version',
            'delete_version',
            'list_versions',
            'put_actor',
            'get_actor',
            'delete_actor',
            'list_actors',
        ]
        
        for method_name in required_methods:
            assert hasattr(EntityDatabase, method_name), \
                f"EntityDatabase must define {method_name} method"
            method = getattr(EntityDatabase, method_name)
            assert callable(method), f"{method_name} must be callable"


class TestEntityDatabaseEntityOperations:
    """Test entity CRUD operations through EntityDatabase interface."""

    @pytest.fixture
    def sample_person_entity(self):
        """Create a sample person entity for testing."""
        return Person(
            slug="ram-chandra-poudel",
            names=[
                Name(
                    kind=NameKind.PRIMARY,
                    en={"full": "Ram Chandra Poudel", "first": "Ram Chandra", "last": "Poudel"},
                    ne={"full": "राम चन्द्र पौडेल", "first": "राम चन्द्र", "last": "पौडेल"}
                )
            ],
            version_summary=VersionSummary(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial import",
                created_at=datetime.now(UTC)
            ),
            created_at=datetime.now(UTC),
            attributes={"party": "nepali-congress", "constituency": "Tanahun-1"}
        )

    @pytest.fixture
    def sample_organization_entity(self):
        """Create a sample organization entity for testing."""
        return PoliticalParty(
            slug="nepali-congress",
            names=[
                Name(
                    kind=NameKind.PRIMARY,
                    en={"full": "Nepali Congress"},
                    ne={"full": "नेपाली कांग्रेस"}
                )
            ],
            version_summary=VersionSummary(
                entity_or_relationship_id="entity:organization/political_party/nepali-congress",
                type=VersionType.ENTITY,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial import",
                created_at=datetime.now(UTC)
            ),
            created_at=datetime.now(UTC),
            attributes={"founded": "1947", "ideology": "social-democracy"}
        )

    @pytest.fixture
    def sample_location_entity(self):
        """Create a sample location entity for testing."""
        return Location(
            slug="kathmandu-metropolitan-city",
            sub_type=EntitySubType.METROPOLITAN_CITY,
            names=[
                Name(
                    kind=NameKind.PRIMARY,
                    en={"full": "Kathmandu Metropolitan City"},
                    ne={"full": "काठमाडौं महानगरपालिका"}
                )
            ],
            version_summary=VersionSummary(
                entity_or_relationship_id="entity:location/metropolitan_city/kathmandu-metropolitan-city",
                type=VersionType.ENTITY,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial import",
                created_at=datetime.now(UTC)
            ),
            created_at=datetime.now(UTC),
            attributes={"province": "Bagmati", "district": "Kathmandu"}
        )

    @pytest.mark.asyncio
    async def test_put_entity_stores_entity(self, temp_db_path, sample_person_entity):
        """Test that put_entity stores an entity and returns it."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store entity
        result = await db.put_entity(sample_person_entity)
        
        # Should return the same entity
        assert result.id == sample_person_entity.id
        assert result.slug == sample_person_entity.slug
        assert result.names[0].en.full == "Ram Chandra Poudel"

    @pytest.mark.asyncio
    async def test_get_entity_retrieves_stored_entity(self, temp_db_path, sample_person_entity):
        """Test that get_entity retrieves a previously stored entity."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store entity
        await db.put_entity(sample_person_entity)
        
        # Retrieve entity
        retrieved = await db.get_entity(sample_person_entity.id)
        
        # Should retrieve the same entity
        assert retrieved is not None
        assert retrieved.id == sample_person_entity.id
        assert retrieved.slug == sample_person_entity.slug
        assert retrieved.names[0].en.full == "Ram Chandra Poudel"
        assert retrieved.names[0].ne.full == "राम चन्द्र पौडेल"

    @pytest.mark.asyncio
    async def test_get_entity_returns_none_for_nonexistent(self, temp_db_path):
        """Test that get_entity returns None for non-existent entity."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Try to retrieve non-existent entity
        result = await db.get_entity("entity:person/nonexistent")
        
        # Should return None
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_entity_removes_entity(self, temp_db_path, sample_person_entity):
        """Test that delete_entity removes an entity."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store entity
        await db.put_entity(sample_person_entity)
        
        # Delete entity
        result = await db.delete_entity(sample_person_entity.id)
        
        # Should return True
        assert result is True
        
        # Entity should no longer exist
        retrieved = await db.get_entity(sample_person_entity.id)
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_delete_entity_returns_false_for_nonexistent(self, temp_db_path):
        """Test that delete_entity returns False for non-existent entity."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Try to delete non-existent entity
        result = await db.delete_entity("entity:person/nonexistent")
        
        # Should return False
        assert result is False

    @pytest.mark.asyncio
    async def test_list_entities_returns_all_entities(self, temp_db_path, sample_person_entity, 
                                                      sample_organization_entity, sample_location_entity):
        """Test that list_entities returns all stored entities."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store multiple entities
        await db.put_entity(sample_person_entity)
        await db.put_entity(sample_organization_entity)
        await db.put_entity(sample_location_entity)
        
        # List all entities
        entities = await db.list_entities(limit=100)
        
        # Should return all 3 entities
        assert len(entities) == 3
        entity_ids = [e.id for e in entities]
        assert sample_person_entity.id in entity_ids
        assert sample_organization_entity.id in entity_ids
        assert sample_location_entity.id in entity_ids

    @pytest.mark.asyncio
    async def test_list_entities_filters_by_type(self, temp_db_path, sample_person_entity, 
                                                  sample_organization_entity):
        """Test that list_entities can filter by entity type."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store entities of different types
        await db.put_entity(sample_person_entity)
        await db.put_entity(sample_organization_entity)
        
        # List only person entities
        entities = await db.list_entities(entity_type="person", limit=100)
        
        # Should return only person entities
        assert len(entities) == 1
        assert entities[0].type == "person"
        assert entities[0].id == sample_person_entity.id

    @pytest.mark.asyncio
    async def test_list_entities_filters_by_subtype(self, temp_db_path, sample_organization_entity):
        """Test that list_entities can filter by entity subtype."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store organization entity
        await db.put_entity(sample_organization_entity)
        
        # List only political_party entities
        entities = await db.list_entities(
            entity_type="organization",
            sub_type="political_party",
            limit=100
        )
        
        # Should return only political party entities
        assert len(entities) == 1
        assert entities[0].sub_type == EntitySubType.POLITICAL_PARTY
        assert entities[0].id == sample_organization_entity.id

    @pytest.mark.asyncio
    async def test_list_entities_supports_pagination(self, temp_db_path):
        """Test that list_entities supports pagination with limit and offset."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store multiple entities
        for i in range(5):
            entity = Person(
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
            await db.put_entity(entity)
        
        # Get first page
        page1 = await db.list_entities(limit=2, offset=0)
        assert len(page1) == 2
        
        # Get second page
        page2 = await db.list_entities(limit=2, offset=2)
        assert len(page2) == 2
        
        # Pages should have different entities
        page1_ids = [e.id for e in page1]
        page2_ids = [e.id for e in page2]
        assert len(set(page1_ids) & set(page2_ids)) == 0


class TestEntityDatabaseRelationshipOperations:
    """Test relationship CRUD operations through EntityDatabase interface."""

    @pytest.fixture
    def sample_relationship(self):
        """Create a sample relationship for testing."""
        return Relationship(
            source_entity_id="entity:person/ram-chandra-poudel",
            target_entity_id="entity:organization/political_party/nepali-congress",
            type="MEMBER_OF",
            start_date=date(2000, 1, 1),
            version_summary=VersionSummary(
                entity_or_relationship_id="relationship:entity:person/ram-chandra-poudel:entity:organization/political_party/nepali-congress:MEMBER_OF",
                type=VersionType.RELATIONSHIP,
                version_number=1,
                actor=Actor(slug="system-importer", name="System Importer"),
                change_description="Initial import",
                created_at=datetime.now(UTC)
            ),
            created_at=datetime.now(UTC),
            attributes={"position": "President"}
        )

    @pytest.mark.asyncio
    async def test_put_relationship_stores_relationship(self, temp_db_path, sample_relationship):
        """Test that put_relationship stores a relationship and returns it."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store relationship
        result = await db.put_relationship(sample_relationship)
        
        # Should return the same relationship
        assert result.id == sample_relationship.id
        assert result.source_entity_id == sample_relationship.source_entity_id
        assert result.target_entity_id == sample_relationship.target_entity_id
        assert result.type == "MEMBER_OF"

    @pytest.mark.asyncio
    async def test_get_relationship_retrieves_stored_relationship(self, temp_db_path, sample_relationship):
        """Test that get_relationship retrieves a previously stored relationship."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store relationship
        await db.put_relationship(sample_relationship)
        
        # Retrieve relationship
        retrieved = await db.get_relationship(sample_relationship.id)
        
        # Should retrieve the same relationship
        assert retrieved is not None
        assert retrieved.id == sample_relationship.id
        assert retrieved.source_entity_id == sample_relationship.source_entity_id
        assert retrieved.target_entity_id == sample_relationship.target_entity_id
        assert retrieved.type == "MEMBER_OF"

    @pytest.mark.asyncio
    async def test_get_relationship_returns_none_for_nonexistent(self, temp_db_path):
        """Test that get_relationship returns None for non-existent relationship."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Try to retrieve non-existent relationship
        result = await db.get_relationship("relationship:nonexistent")
        
        # Should return None
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_relationship_removes_relationship(self, temp_db_path, sample_relationship):
        """Test that delete_relationship removes a relationship."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store relationship
        await db.put_relationship(sample_relationship)
        
        # Delete relationship
        result = await db.delete_relationship(sample_relationship.id)
        
        # Should return True
        assert result is True
        
        # Relationship should no longer exist
        retrieved = await db.get_relationship(sample_relationship.id)
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_list_relationships_returns_all_relationships(self, temp_db_path):
        """Test that list_relationships returns all stored relationships."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store multiple relationships
        for i in range(3):
            rel = Relationship(
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
            await db.put_relationship(rel)
        
        # List all relationships
        relationships = await db.list_relationships(limit=100)
        
        # Should return all 3 relationships
        assert len(relationships) == 3


class TestEntityDatabaseVersionOperations:
    """Test version CRUD operations through EntityDatabase interface."""

    @pytest.fixture
    def sample_version(self):
        """Create a sample version for testing."""
        return Version(
            entity_or_relationship_id="entity:person/ram-chandra-poudel",
            type=VersionType.ENTITY,
            version_number=1,
            actor=Actor(slug="system-importer", name="System Importer"),
            change_description="Initial import",
            created_at=datetime.now(UTC),
            snapshot={
                "slug": "ram-chandra-poudel",
                "type": "person",
                "names": [{"kind": "PRIMARY", "en": {"full": "Ram Chandra Poudel"}}]
            }
        )

    @pytest.mark.asyncio
    async def test_put_version_stores_version(self, temp_db_path, sample_version):
        """Test that put_version stores a version and returns it."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store version
        result = await db.put_version(sample_version)
        
        # Should return the same version
        assert result.id == sample_version.id
        assert result.version_number == 1
        assert result.entity_or_relationship_id == "entity:person/ram-chandra-poudel"

    @pytest.mark.asyncio
    async def test_get_version_retrieves_stored_version(self, temp_db_path, sample_version):
        """Test that get_version retrieves a previously stored version."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store version
        await db.put_version(sample_version)
        
        # Retrieve version
        retrieved = await db.get_version(sample_version.id)
        
        # Should retrieve the same version
        assert retrieved is not None
        assert retrieved.id == sample_version.id
        assert retrieved.version_number == 1
        assert retrieved.snapshot is not None

    @pytest.mark.asyncio
    async def test_list_versions_returns_all_versions(self, temp_db_path):
        """Test that list_versions returns all stored versions."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store multiple versions
        for i in range(3):
            version = Version(
                entity_or_relationship_id="entity:person/ram-chandra-poudel",
                type=VersionType.ENTITY,
                version_number=i + 1,
                actor=Actor(slug="system"),
                change_description=f"Update {i + 1}",
                created_at=datetime.now(UTC),
                snapshot={"version": i + 1}
            )
            await db.put_version(version)
        
        # List all versions
        versions = await db.list_versions(limit=100)
        
        # Should return all 3 versions
        assert len(versions) == 3


class TestEntityDatabaseActorOperations:
    """Test actor CRUD operations through EntityDatabase interface."""

    @pytest.fixture
    def sample_actor(self):
        """Create a sample actor for testing."""
        return Actor(
            slug="system-importer",
            name="System Importer"
        )

    @pytest.mark.asyncio
    async def test_put_actor_stores_actor(self, temp_db_path, sample_actor):
        """Test that put_actor stores an actor and returns it."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store actor
        result = await db.put_actor(sample_actor)
        
        # Should return the same actor
        assert result.id == sample_actor.id
        assert result.slug == sample_actor.slug
        assert result.name == "System Importer"

    @pytest.mark.asyncio
    async def test_get_actor_retrieves_stored_actor(self, temp_db_path, sample_actor):
        """Test that get_actor retrieves a previously stored actor."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store actor
        await db.put_actor(sample_actor)
        
        # Retrieve actor
        retrieved = await db.get_actor(sample_actor.id)
        
        # Should retrieve the same actor
        assert retrieved is not None
        assert retrieved.id == sample_actor.id
        assert retrieved.slug == sample_actor.slug
        assert retrieved.name == "System Importer"

    @pytest.mark.asyncio
    async def test_list_actors_returns_all_actors(self, temp_db_path):
        """Test that list_actors returns all stored actors."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Store multiple actors
        for i in range(3):
            actor = Actor(slug=f"actor-{i}", name=f"Actor {i}")
            await db.put_actor(actor)
        
        # List all actors
        actors = await db.list_actors(limit=100)
        
        # Should return all 3 actors
        assert len(actors) == 3
