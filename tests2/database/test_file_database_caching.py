"""Tests for FileDatabase caching capabilities in nes2.

Following TDD approach: Write failing tests first (Red phase).
These tests define the expected behavior of caching functionality.

Test Coverage:
- Cache hit/miss behavior
- Cache TTL expiration
- Cache invalidation on updates
- Cache warming
"""

import pytest
import asyncio
from datetime import datetime, UTC
from time import sleep

from nes2.core.models.base import Name, NameKind
from nes2.core.models.person import Person
from nes2.core.models.version import Actor, VersionSummary, VersionType


class TestCacheHitMissBehavior:
    """Test cache hit and miss behavior for entity retrieval."""

    @pytest.fixture
    def db_with_cache(self, temp_db_path):
        """Create a database with caching enabled."""
        from nes2.database.file_database import FileDatabase
        
        # Initialize database with caching enabled
        db = FileDatabase(base_path=str(temp_db_path), enable_cache=True)
        return db

    @pytest.mark.asyncio
    async def test_cache_miss_on_first_access(self, db_with_cache):
        """Test that first access results in cache miss."""
        # Create and store an entity
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
        await db_with_cache.put_entity(entity)
        
        # Clear cache to ensure clean state
        db_with_cache.clear_cache()
        
        # First access should be a cache miss
        result = await db_with_cache.get_entity("entity:person/ram-chandra-poudel")
        
        # Verify entity was retrieved
        assert result is not None
        assert result.slug == "ram-chandra-poudel"
        
        # Verify cache miss was recorded
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["misses"] == 1
        assert cache_stats["hits"] == 0

    @pytest.mark.asyncio
    async def test_cache_hit_on_second_access(self, db_with_cache):
        """Test that second access results in cache hit."""
        # Create and store an entity
        entity = Person(
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
        await db_with_cache.put_entity(entity)
        
        # Clear cache stats
        db_with_cache.clear_cache_stats()
        
        # First access (cache miss)
        await db_with_cache.get_entity("entity:person/sher-bahadur-deuba")
        
        # Second access (should be cache hit)
        result = await db_with_cache.get_entity("entity:person/sher-bahadur-deuba")
        
        # Verify entity was retrieved
        assert result is not None
        assert result.slug == "sher-bahadur-deuba"
        
        # Verify cache hit was recorded
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 1
        assert cache_stats["misses"] == 1

    @pytest.mark.asyncio
    async def test_cache_miss_for_nonexistent_entity(self, db_with_cache):
        """Test that accessing nonexistent entity results in cache miss."""
        # Clear cache stats
        db_with_cache.clear_cache_stats()
        
        # Try to access nonexistent entity
        result = await db_with_cache.get_entity("entity:person/nonexistent")
        
        # Verify entity was not found
        assert result is None
        
        # Verify cache miss was recorded
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["misses"] == 1

    @pytest.mark.asyncio
    async def test_cache_hit_rate_calculation(self, db_with_cache):
        """Test that cache hit rate is calculated correctly."""
        # Create and store entities
        entities = [
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
            for i in range(3)
        ]
        
        for entity in entities:
            await db_with_cache.put_entity(entity)
        
        # Clear cache stats
        db_with_cache.clear_cache_stats()
        
        # Access entities: 3 misses, then 3 hits
        for i in range(3):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        for i in range(3):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Verify hit rate is 50% (3 hits out of 6 total accesses)
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 3
        assert cache_stats["misses"] == 3
        assert cache_stats["hit_rate"] == 0.5


class TestCacheTTLExpiration:
    """Test cache TTL (Time To Live) expiration behavior."""

    @pytest.fixture
    def db_with_short_ttl(self, temp_db_path):
        """Create a database with short cache TTL for testing."""
        from nes2.database.file_database import FileDatabase
        
        # Initialize database with 1 second TTL
        db = FileDatabase(base_path=str(temp_db_path), enable_cache=True, cache_ttl_seconds=1)
        return db

    @pytest.mark.asyncio
    async def test_cache_entry_expires_after_ttl(self, db_with_short_ttl):
        """Test that cache entries expire after TTL."""
        # Create and store an entity
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
        await db_with_short_ttl.put_entity(entity)
        
        # Clear cache stats
        db_with_short_ttl.clear_cache_stats()
        
        # First access (cache miss)
        await db_with_short_ttl.get_entity("entity:person/ram-chandra-poudel")
        
        # Second access immediately (cache hit)
        await db_with_short_ttl.get_entity("entity:person/ram-chandra-poudel")
        
        # Wait for TTL to expire
        await asyncio.sleep(1.5)
        
        # Third access after TTL (should be cache miss)
        await db_with_short_ttl.get_entity("entity:person/ram-chandra-poudel")
        
        # Verify cache behavior
        cache_stats = db_with_short_ttl.get_cache_stats()
        assert cache_stats["hits"] == 1  # Only the second access was a hit
        assert cache_stats["misses"] == 2  # First and third accesses were misses

    @pytest.mark.asyncio
    async def test_cache_entry_refreshed_on_access(self, db_with_short_ttl):
        """Test that cache entry TTL is refreshed on access."""
        # Create and store an entity
        entity = Person(
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
        await db_with_short_ttl.put_entity(entity)
        
        # Clear cache stats
        db_with_short_ttl.clear_cache_stats()
        
        # First access (cache miss)
        await db_with_short_ttl.get_entity("entity:person/sher-bahadur-deuba")
        
        # Access every 0.5 seconds for 2 seconds (should keep refreshing TTL)
        for _ in range(4):
            await asyncio.sleep(0.5)
            await db_with_short_ttl.get_entity("entity:person/sher-bahadur-deuba")
        
        # All accesses after first should be cache hits
        cache_stats = db_with_short_ttl.get_cache_stats()
        assert cache_stats["hits"] == 4
        assert cache_stats["misses"] == 1

    @pytest.mark.asyncio
    async def test_expired_entries_removed_from_cache(self, db_with_short_ttl):
        """Test that expired entries are removed from cache."""
        # Create and store multiple entities
        entities = [
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
            for i in range(3)
        ]
        
        for entity in entities:
            await db_with_short_ttl.put_entity(entity)
        
        # Access all entities to populate cache
        for i in range(3):
            await db_with_short_ttl.get_entity(f"entity:person/person-{i}")
        
        # Verify cache size
        cache_stats = db_with_short_ttl.get_cache_stats()
        assert cache_stats["size"] == 3
        
        # Wait for TTL to expire
        await asyncio.sleep(1.5)
        
        # Trigger cache cleanup
        db_with_short_ttl.cleanup_expired_cache_entries()
        
        # Verify expired entries were removed
        cache_stats = db_with_short_ttl.get_cache_stats()
        assert cache_stats["size"] == 0


class TestCacheInvalidationOnUpdates:
    """Test cache invalidation when entities are updated."""

    @pytest.fixture
    def db_with_cache(self, temp_db_path):
        """Create a database with caching enabled."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path), enable_cache=True)
        return db

    @pytest.mark.asyncio
    async def test_cache_invalidated_on_entity_update(self, db_with_cache):
        """Test that cache is invalidated when entity is updated."""
        # Create and store an entity
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
        await db_with_cache.put_entity(entity)
        
        # Access entity to populate cache
        result1 = await db_with_cache.get_entity("entity:person/ram-chandra-poudel")
        assert result1.names[0].en.full == "Ram Chandra Poudel"
        
        # Update entity
        entity.names[0].en.full = "Ram Chandra Poudel (Updated)"
        entity.version_summary.version_number = 2
        await db_with_cache.put_entity(entity)
        
        # Access entity again (should get updated version, not cached)
        result2 = await db_with_cache.get_entity("entity:person/ram-chandra-poudel")
        assert result2.names[0].en.full == "Ram Chandra Poudel (Updated)"

    @pytest.mark.asyncio
    async def test_cache_invalidated_on_entity_deletion(self, db_with_cache):
        """Test that cache is invalidated when entity is deleted."""
        # Create and store an entity
        entity = Person(
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
        await db_with_cache.put_entity(entity)
        
        # Access entity to populate cache
        result1 = await db_with_cache.get_entity("entity:person/sher-bahadur-deuba")
        assert result1 is not None
        
        # Delete entity
        await db_with_cache.delete_entity("entity:person/sher-bahadur-deuba")
        
        # Access entity again (should return None, not cached version)
        result2 = await db_with_cache.get_entity("entity:person/sher-bahadur-deuba")
        assert result2 is None

    @pytest.mark.asyncio
    async def test_cache_invalidated_on_relationship_update(self, db_with_cache):
        """Test that cache is invalidated when relationship is updated."""
        from nes2.core.models.relationship import Relationship
        from nes2.core.models.organization import PoliticalParty
        from datetime import date
        
        # Create entities
        person = Person(
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
        
        party = PoliticalParty(
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
        
        await db_with_cache.put_entity(person)
        await db_with_cache.put_entity(party)
        
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
        await db_with_cache.put_relationship(relationship)
        
        # Access relationship to populate cache
        result1 = await db_with_cache.get_relationship(relationship.id)
        assert result1.start_date == date(2000, 1, 1)
        
        # Update relationship
        relationship.start_date = date(2005, 1, 1)
        relationship.version_summary.version_number = 2
        await db_with_cache.put_relationship(relationship)
        
        # Access relationship again (should get updated version)
        result2 = await db_with_cache.get_relationship(relationship.id)
        assert result2.start_date == date(2005, 1, 1)

    @pytest.mark.asyncio
    async def test_cache_invalidation_does_not_affect_other_entries(self, db_with_cache):
        """Test that invalidating one entry doesn't affect others."""
        # Create and store multiple entities
        entities = [
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
            for i in range(3)
        ]
        
        for entity in entities:
            await db_with_cache.put_entity(entity)
        
        # Access all entities to populate cache
        for i in range(3):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Clear cache stats
        db_with_cache.clear_cache_stats()
        
        # Update one entity
        entities[0].names[0].en.full = "Person 0 (Updated)"
        entities[0].version_summary.version_number = 2
        await db_with_cache.put_entity(entities[0])
        
        # Access all entities
        await db_with_cache.get_entity("entity:person/person-0")  # Miss (invalidated)
        await db_with_cache.get_entity("entity:person/person-1")  # Hit (not invalidated)
        await db_with_cache.get_entity("entity:person/person-2")  # Hit (not invalidated)
        
        # Verify cache behavior
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 2
        assert cache_stats["misses"] == 1


class TestCacheWarming:
    """Test cache warming functionality."""

    @pytest.fixture
    def db_with_cache(self, temp_db_path):
        """Create a database with caching enabled."""
        from nes2.database.file_database import FileDatabase
        
        db = FileDatabase(base_path=str(temp_db_path), enable_cache=True)
        return db

    @pytest.mark.asyncio
    async def test_warm_cache_with_entity_list(self, db_with_cache):
        """Test warming cache with a list of entity IDs."""
        # Create and store multiple entities
        entities = [
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
            for i in range(5)
        ]
        
        for entity in entities:
            await db_with_cache.put_entity(entity)
        
        # Clear cache
        db_with_cache.clear_cache()
        db_with_cache.clear_cache_stats()
        
        # Warm cache with specific entities
        entity_ids = [f"entity:person/person-{i}" for i in range(3)]
        await db_with_cache.warm_cache(entity_ids=entity_ids)
        
        # Access warmed entities (should be cache hits)
        for i in range(3):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Access non-warmed entities (should be cache misses)
        for i in range(3, 5):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Verify cache behavior
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 3  # Warmed entities
        assert cache_stats["misses"] == 2  # Non-warmed entities

    @pytest.mark.asyncio
    async def test_warm_cache_with_entity_type(self, db_with_cache):
        """Test warming cache with all entities of a specific type."""
        from nes2.core.models.organization import PoliticalParty
        
        # Create and store entities of different types
        persons = [
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
            for i in range(3)
        ]
        
        parties = [
            PoliticalParty(
                slug=f"party-{i}",
                names=[Name(kind=NameKind.PRIMARY, en={"full": f"Party {i}"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id=f"entity:organization/political_party/party-{i}",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
            for i in range(2)
        ]
        
        for entity in persons + parties:
            await db_with_cache.put_entity(entity)
        
        # Clear cache
        db_with_cache.clear_cache()
        db_with_cache.clear_cache_stats()
        
        # Warm cache with person entities only
        await db_with_cache.warm_cache(entity_type="person")
        
        # Access person entities (should be cache hits)
        for i in range(3):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Access party entities (should be cache misses)
        for i in range(2):
            await db_with_cache.get_entity(f"entity:organization/political_party/party-{i}")
        
        # Verify cache behavior
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 3  # Person entities
        assert cache_stats["misses"] == 2  # Party entities

    @pytest.mark.asyncio
    async def test_warm_cache_on_startup(self, db_with_cache):
        """Test automatic cache warming on database initialization."""
        from nes2.database.file_database import FileDatabase
        
        # Create and store frequently accessed entities
        entities = [
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
                created_at=datetime.now(UTC),
                attributes={"popular": "true"}
            )
            for i in range(3)
        ]
        
        for entity in entities:
            await db_with_cache.put_entity(entity)
        
        # Create new database instance with cache warming enabled
        new_db = FileDatabase(
            base_path=str(db_with_cache.base_path),
            enable_cache=True,
            warm_cache_on_startup=True,
            warm_cache_filter={"attributes.popular": "true"}
        )
        
        # Clear cache stats
        new_db.clear_cache_stats()
        
        # Access entities (should be cache hits if warmed)
        for i in range(3):
            await new_db.get_entity(f"entity:person/person-{i}")
        
        # Verify cache was warmed
        cache_stats = new_db.get_cache_stats()
        assert cache_stats["hits"] >= 1  # At least some entities were warmed

    @pytest.mark.asyncio
    async def test_warm_cache_with_most_accessed_entities(self, db_with_cache):
        """Test warming cache with most frequently accessed entities."""
        # Create and store multiple entities
        entities = [
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
            for i in range(10)
        ]
        
        for entity in entities:
            await db_with_cache.put_entity(entity)
        
        # Simulate access patterns (access first 3 entities more frequently)
        for _ in range(10):
            for i in range(3):
                await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        for _ in range(2):
            for i in range(3, 10):
                await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Clear cache
        db_with_cache.clear_cache()
        db_with_cache.clear_cache_stats()
        
        # Warm cache with top 5 most accessed entities
        await db_with_cache.warm_cache_most_accessed(limit=5)
        
        # Access top 3 entities (should be cache hits)
        for i in range(3):
            await db_with_cache.get_entity(f"entity:person/person-{i}")
        
        # Verify cache behavior
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 3
