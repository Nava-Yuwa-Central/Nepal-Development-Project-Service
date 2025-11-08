"""Tests for FileDatabase file I/O optimization capabilities in nes2.

Following TDD approach: Write failing tests first (Red phase).
These tests define the expected behavior of file I/O optimizations.

Test Coverage:
- Batch read operations
- Concurrent read support
- Directory traversal optimization
- Index file usage
"""

import pytest
import asyncio
from datetime import datetime, UTC
from pathlib import Path

from nes2.core.models.base import Name, NameKind
from nes2.core.models.person import Person
from nes2.core.models.organization import PoliticalParty
from nes2.core.models.location import Location
from nes2.core.models.entity import EntitySubType
from nes2.core.models.version import Actor, VersionSummary, VersionType


class TestBatchReadOperations:
    """Test batch read operations for improved I/O performance."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database populated with multiple entities."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create multiple entities
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
            for i in range(20)
        ]
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_batch_get_entities_by_ids(self, populated_db):
        """Test that batch_get_entities can retrieve multiple entities efficiently."""
        # Prepare list of entity IDs to fetch
        entity_ids = [f"entity:person/person-{i}" for i in range(5)]
        
        # Batch get entities (should be more efficient than individual gets)
        results = await populated_db.batch_get_entities(entity_ids)
        
        # Verify all entities were retrieved
        assert len(results) == 5
        assert all(entity is not None for entity in results)
        assert all(entity.slug == f"person-{i}" for i, entity in enumerate(results))

    @pytest.mark.asyncio
    async def test_batch_get_entities_handles_missing_entities(self, populated_db):
        """Test that batch_get_entities handles missing entities gracefully."""
        # Mix of existing and non-existing entity IDs
        entity_ids = [
            "entity:person/person-0",
            "entity:person/nonexistent-1",
            "entity:person/person-1",
            "entity:person/nonexistent-2",
            "entity:person/person-2"
        ]
        
        # Batch get entities
        results = await populated_db.batch_get_entities(entity_ids)
        
        # Verify results (None for missing entities)
        assert len(results) == 5
        assert results[0] is not None and results[0].slug == "person-0"
        assert results[1] is None  # Missing entity
        assert results[2] is not None and results[2].slug == "person-1"
        assert results[3] is None  # Missing entity
        assert results[4] is not None and results[4].slug == "person-2"

    @pytest.mark.asyncio
    async def test_batch_get_entities_performance(self, populated_db):
        """Test that batch_get_entities is faster than individual gets."""
        import time
        
        entity_ids = [f"entity:person/person-{i}" for i in range(10)]
        
        # Measure time for individual gets
        start_individual = time.time()
        individual_results = []
        for entity_id in entity_ids:
            entity = await populated_db.get_entity(entity_id)
            individual_results.append(entity)
        end_individual = time.time()
        individual_time = end_individual - start_individual
        
        # Measure time for batch get
        start_batch = time.time()
        batch_results = await populated_db.batch_get_entities(entity_ids)
        end_batch = time.time()
        batch_time = end_batch - start_batch
        
        # Verify results are the same
        assert len(individual_results) == len(batch_results)
        
        # Batch should be faster (or at least not significantly slower)
        # Allow some tolerance for test variability
        assert batch_time <= individual_time * 1.5

    @pytest.mark.asyncio
    async def test_batch_get_entities_empty_list(self, populated_db):
        """Test that batch_get_entities handles empty list."""
        results = await populated_db.batch_get_entities([])
        
        # Should return empty list
        assert results == []

    @pytest.mark.asyncio
    async def test_batch_get_entities_preserves_order(self, populated_db):
        """Test that batch_get_entities preserves the order of requested IDs."""
        # Request entities in specific order
        entity_ids = [
            "entity:person/person-5",
            "entity:person/person-2",
            "entity:person/person-8",
            "entity:person/person-1"
        ]
        
        results = await populated_db.batch_get_entities(entity_ids)
        
        # Verify order is preserved
        assert len(results) == 4
        assert results[0].slug == "person-5"
        assert results[1].slug == "person-2"
        assert results[2].slug == "person-8"
        assert results[3].slug == "person-1"


class TestConcurrentReadSupport:
    """Test concurrent read operations for improved throughput."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database populated with entities."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities
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
            for i in range(30)
        ]
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_concurrent_entity_reads(self, populated_db):
        """Test that multiple concurrent reads can be performed safely."""
        # Create multiple concurrent read tasks
        entity_ids = [f"entity:person/person-{i}" for i in range(10)]
        
        # Execute reads concurrently
        tasks = [populated_db.get_entity(entity_id) for entity_id in entity_ids]
        results = await asyncio.gather(*tasks)
        
        # Verify all reads succeeded
        assert len(results) == 10
        assert all(entity is not None for entity in results)
        assert all(entity.slug == f"person-{i}" for i, entity in enumerate(results))

    @pytest.mark.asyncio
    async def test_concurrent_search_operations(self, populated_db):
        """Test that multiple concurrent search operations work correctly."""
        # Create multiple concurrent search tasks
        search_queries = ["Person 1", "Person 2", "Person 3"]
        
        # Execute searches concurrently
        tasks = [populated_db.search_entities(query=query) for query in search_queries]
        results = await asyncio.gather(*tasks)
        
        # Verify all searches succeeded
        assert len(results) == 3
        assert all(len(result) > 0 for result in results)

    @pytest.mark.asyncio
    async def test_concurrent_list_operations(self, populated_db):
        """Test that multiple concurrent list operations work correctly."""
        # Create multiple concurrent list tasks with different parameters
        tasks = [
            populated_db.list_entities(limit=5, offset=0),
            populated_db.list_entities(limit=5, offset=5),
            populated_db.list_entities(limit=5, offset=10)
        ]
        
        # Execute lists concurrently
        results = await asyncio.gather(*tasks)
        
        # Verify all lists succeeded
        assert len(results) == 3
        assert all(len(result) == 5 for result in results)
        
        # Verify no overlap in results (different offsets)
        all_slugs = [entity.slug for result in results for entity in result]
        assert len(all_slugs) == len(set(all_slugs))  # All unique

    @pytest.mark.asyncio
    async def test_concurrent_mixed_operations(self, populated_db):
        """Test that mixed concurrent operations (get, search, list) work correctly."""
        # Create mixed concurrent tasks
        tasks = [
            populated_db.get_entity("entity:person/person-0"),
            populated_db.search_entities(query="Person 5"),
            populated_db.list_entities(limit=3),
            populated_db.get_entity("entity:person/person-10"),
            populated_db.search_entities(query="Person 15")
        ]
        
        # Execute concurrently
        results = await asyncio.gather(*tasks)
        
        # Verify all operations succeeded
        assert len(results) == 5
        assert results[0] is not None  # get_entity
        assert len(results[1]) > 0  # search_entities
        assert len(results[2]) == 3  # list_entities
        assert results[3] is not None  # get_entity
        assert len(results[4]) > 0  # search_entities

    @pytest.mark.asyncio
    async def test_concurrent_reads_with_high_concurrency(self, populated_db):
        """Test that high concurrency reads work correctly."""
        # Create many concurrent read tasks
        entity_ids = [f"entity:person/person-{i}" for i in range(30)]
        
        # Execute all reads concurrently
        tasks = [populated_db.get_entity(entity_id) for entity_id in entity_ids]
        results = await asyncio.gather(*tasks)
        
        # Verify all reads succeeded
        assert len(results) == 30
        assert all(entity is not None for entity in results)


class TestDirectoryTraversalOptimization:
    """Test optimizations for directory traversal operations."""

    @pytest.fixture
    def complex_db(self, temp_db_path):
        """Create a database with complex directory structure."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create entities of different types and subtypes
        entities = []
        
        # Add persons
        for i in range(10):
            entities.append(Person(
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
            ))
        
        # Add political parties
        for i in range(5):
            entities.append(PoliticalParty(
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
            ))
        
        # Add locations
        for i in range(5):
            entities.append(Location(
                slug=f"location-{i}",
                sub_type=EntitySubType.METROPOLITAN_CITY,
                names=[Name(kind=NameKind.PRIMARY, en={"full": f"Location {i}"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id=f"entity:location/metropolitan_city/location-{i}",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ))
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_optimized_list_entities_by_type(self, complex_db):
        """Test that listing entities by type uses optimized directory traversal."""
        import time
        
        # List entities by type (should only traverse person directory)
        start = time.time()
        results = await complex_db.list_entities(entity_type="person", limit=100)
        end = time.time()
        
        # Verify results
        assert len(results) == 10
        assert all(entity.type == "person" for entity in results)
        
        # Should be fast (< 100ms for small dataset)
        assert (end - start) < 0.1

    @pytest.mark.asyncio
    async def test_optimized_list_entities_by_subtype(self, complex_db):
        """Test that listing entities by subtype uses optimized directory traversal."""
        # List entities by type and subtype (should only traverse specific subdirectory)
        results = await complex_db.list_entities(
            entity_type="organization",
            sub_type="political_party",
            limit=100
        )
        
        # Verify results
        assert len(results) == 5
        assert all(entity.type == "organization" for entity in results)
        assert all(entity.sub_type == EntitySubType.POLITICAL_PARTY for entity in results)

    @pytest.mark.asyncio
    async def test_list_all_entities_traverses_all_directories(self, complex_db):
        """Test that listing all entities traverses all directories."""
        # List all entities (should traverse all directories)
        results = await complex_db.list_entities(limit=100)
        
        # Verify results include all types
        assert len(results) == 20
        types = set(entity.type for entity in results)
        assert "person" in types
        assert "organization" in types
        assert "location" in types

    @pytest.mark.asyncio
    async def test_directory_traversal_respects_limit(self, complex_db):
        """Test that directory traversal stops early when limit is reached."""
        import time
        
        # List with small limit (should stop early)
        start = time.time()
        results = await complex_db.list_entities(limit=3)
        end = time.time()
        
        # Verify results
        assert len(results) == 3
        
        # Should be fast since it stops early
        assert (end - start) < 0.1

    @pytest.mark.asyncio
    async def test_directory_traversal_with_pagination(self, complex_db):
        """Test that directory traversal works correctly with pagination."""
        # Get first page
        page1 = await complex_db.list_entities(limit=5, offset=0)
        
        # Get second page
        page2 = await complex_db.list_entities(limit=5, offset=5)
        
        # Get third page
        page3 = await complex_db.list_entities(limit=5, offset=10)
        
        # Verify no overlap
        all_ids = [e.id for e in page1] + [e.id for e in page2] + [e.id for e in page3]
        assert len(all_ids) == len(set(all_ids))  # All unique


class TestIndexFileUsage:
    """Test index file usage for improved query performance."""

    @pytest.fixture
    def db_with_indexes(self, temp_db_path):
        """Create a database with index support enabled."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        # Initialize database with indexing enabled
        db = FileDatabase(base_path=str(temp_db_path), enable_indexes=True)
        
        # Create entities
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
                attributes={"party": "party-a" if i < 5 else "party-b"}
            )
            for i in range(10)
        ]
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_index_file_created_on_entity_insert(self, db_with_indexes):
        """Test that index files are created when entities are inserted."""
        # Check if index files exist
        index_path = db_with_indexes.base_path / "_indexes"
        
        # Index directory should exist
        assert index_path.exists()
        
        # Type index should exist
        type_index_path = index_path / "by_type.json"
        assert type_index_path.exists()

    @pytest.mark.asyncio
    async def test_index_file_contains_entity_references(self, db_with_indexes):
        """Test that index files contain correct entity references."""
        import json
        
        # Read type index
        index_path = db_with_indexes.base_path / "_indexes" / "by_type.json"
        
        if index_path.exists():
            with open(index_path, "r") as f:
                type_index = json.load(f)
            
            # Verify person type is indexed
            assert "person" in type_index
            assert len(type_index["person"]) == 10

    @pytest.mark.asyncio
    async def test_search_uses_index_for_performance(self, db_with_indexes):
        """Test that search operations use indexes for better performance."""
        import time
        
        # Search with attribute filter (should use index if available)
        start = time.time()
        results = await db_with_indexes.search_entities(
            attr_filters={"party": "party-a"}
        )
        end = time.time()
        
        # Verify results
        assert len(results) == 5
        assert all(entity.attributes.get("party") == "party-a" for entity in results)
        
        # Should be fast with index
        assert (end - start) < 0.1

    @pytest.mark.asyncio
    async def test_index_updated_on_entity_update(self, db_with_indexes):
        """Test that indexes are updated when entities are modified."""
        # Get an entity
        entity = await db_with_indexes.get_entity("entity:person/person-0")
        
        # Update entity attributes
        entity.attributes["party"] = "party-c"
        entity.version_summary.version_number = 2
        await db_with_indexes.put_entity(entity)
        
        # Search for updated attribute
        results = await db_with_indexes.search_entities(
            attr_filters={"party": "party-c"}
        )
        
        # Should find the updated entity
        assert len(results) == 1
        assert results[0].slug == "person-0"

    @pytest.mark.asyncio
    async def test_index_updated_on_entity_deletion(self, db_with_indexes):
        """Test that indexes are updated when entities are deleted."""
        # Delete an entity
        await db_with_indexes.delete_entity("entity:person/person-0")
        
        # Search for all persons
        results = await db_with_indexes.list_entities(entity_type="person")
        
        # Should have one less entity
        assert len(results) == 9
        assert not any(entity.slug == "person-0" for entity in results)

    @pytest.mark.asyncio
    async def test_rebuild_indexes_command(self, db_with_indexes):
        """Test that indexes can be rebuilt from scratch."""
        # Rebuild indexes
        await db_with_indexes.rebuild_indexes()
        
        # Verify indexes are functional
        results = await db_with_indexes.list_entities(entity_type="person")
        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_index_file_format_is_valid_json(self, db_with_indexes):
        """Test that index files are valid JSON."""
        import json
        
        index_path = db_with_indexes.base_path / "_indexes" / "by_type.json"
        
        if index_path.exists():
            # Should be able to parse as JSON
            with open(index_path, "r") as f:
                data = json.load(f)
            
            # Should be a dictionary
            assert isinstance(data, dict)

    @pytest.mark.asyncio
    async def test_index_improves_list_performance(self, db_with_indexes):
        """Test that indexes improve list operation performance."""
        import time
        
        # List entities by type (should use index)
        start_with_index = time.time()
        results_with_index = await db_with_indexes.list_entities(entity_type="person")
        end_with_index = time.time()
        time_with_index = end_with_index - start_with_index
        
        # Verify results
        assert len(results_with_index) == 10
        
        # Should be fast with index
        assert time_with_index < 0.1


class TestBatchReadWithCaching:
    """Test interaction between batch reads and caching."""

    @pytest.fixture
    def db_with_cache(self, temp_db_path):
        """Create a database with caching enabled."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path), enable_cache=True)
        
        # Create entities
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
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_batch_read_populates_cache(self, db_with_cache):
        """Test that batch reads populate the cache."""
        # Clear cache
        db_with_cache.clear_cache()
        db_with_cache.clear_cache_stats()
        
        # Batch read entities
        entity_ids = [f"entity:person/person-{i}" for i in range(5)]
        await db_with_cache.batch_get_entities(entity_ids)
        
        # Subsequent individual reads should hit cache
        for entity_id in entity_ids:
            await db_with_cache.get_entity(entity_id)
        
        # Verify cache hits
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 5

    @pytest.mark.asyncio
    async def test_batch_read_uses_cached_entities(self, db_with_cache):
        """Test that batch reads use cached entities when available."""
        # Clear cache and stats
        db_with_cache.clear_cache()
        db_with_cache.clear_cache_stats()
        
        # Load some entities into cache
        entity_ids = [f"entity:person/person-{i}" for i in range(5)]
        for entity_id in entity_ids:
            await db_with_cache.get_entity(entity_id)
        
        # Clear stats
        db_with_cache.clear_cache_stats()
        
        # Batch read (should use cache for already loaded entities)
        await db_with_cache.batch_get_entities(entity_ids)
        
        # Should have cache hits
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] > 0

    @pytest.mark.asyncio
    async def test_batch_read_mixed_cached_and_uncached(self, db_with_cache):
        """Test batch reads with mix of cached and uncached entities."""
        # Clear cache
        db_with_cache.clear_cache()
        db_with_cache.clear_cache_stats()
        
        # Load some entities into cache
        cached_ids = [f"entity:person/person-{i}" for i in range(3)]
        for entity_id in cached_ids:
            await db_with_cache.get_entity(entity_id)
        
        # Clear stats
        db_with_cache.clear_cache_stats()
        
        # Batch read with mix of cached and uncached
        all_ids = [f"entity:person/person-{i}" for i in range(6)]
        results = await db_with_cache.batch_get_entities(all_ids)
        
        # Verify all entities retrieved
        assert len(results) == 6
        assert all(entity is not None for entity in results)
        
        # Should have some cache hits and some misses
        cache_stats = db_with_cache.get_cache_stats()
        assert cache_stats["hits"] == 3  # First 3 were cached
        assert cache_stats["misses"] == 3  # Last 3 were not cached
