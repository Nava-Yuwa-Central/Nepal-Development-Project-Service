"""File-based implementation of EntityDatabase for nes2.

This module provides a file-based storage implementation using JSON files
organized in a directory structure. The default database path is 'nes-db/v2'.

Features:
- JSON-based storage with human-readable format
- Optional in-memory caching with TTL
- Batch operations for improved performance
- Index support for faster queries
- Comprehensive error handling

Performance Characteristics:
- Read-optimized with caching support
- Concurrent read operations via asyncio
- Lazy cache warming on first access
- Index-based queries when enabled
"""

import asyncio
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

from nes2.core.models.entity import Entity, EntityType, EntitySubType
from nes2.core.models.entity_type_map import ENTITY_TYPE_MAP
from nes2.core.models.person import Person
from nes2.core.models.organization import Organization, PoliticalParty, GovernmentBody
from nes2.core.models.location import Location
from nes2.core.models.relationship import Relationship
from nes2.core.models.version import Actor, Version

from .entity_database import EntityDatabase

# Configure logger for this module
logger = logging.getLogger(__name__)


class FileDatabase(EntityDatabase):
    """File-based implementation of EntityDatabase.
    
    Stores entities, relationships, versions, and actors as JSON files
    in a directory structure organized by type and subtype.
    
    Default database path: nes-db/v2
    
    Directory structure:
        nes-db/v2/
            entity/
                person/
                    {slug}.json
                organization/
                    political_party/
                        {slug}.json
                    government_body/
                        {slug}.json
                location/
                    province/
                        {slug}.json
                    district/
                        {slug}.json
            relationship/
                {relationship-id}.json
            version/
                entity/
                    {entity-id}/
                        {version-number}.json
                relationship/
                    {relationship-id}/
                        {version-number}.json
            actor/
                {slug}.json
    """

    def __init__(
        self,
        base_path: str = "nes-db/v2",
        enable_cache: bool = False,
        cache_ttl_seconds: int = 300,
        warm_cache_on_startup: bool = False,
        warm_cache_filter: Optional[Dict[str, Any]] = None,
        enable_indexes: bool = False,
    ):
        """Initialize FileDatabase with the specified base path.
        
        Args:
            base_path: Root directory for database storage (default: nes-db/v2)
            enable_cache: Enable in-memory caching (default: False)
            cache_ttl_seconds: Cache entry TTL in seconds (default: 300)
            warm_cache_on_startup: Warm cache on initialization (default: False)
            warm_cache_filter: Filter for cache warming (default: None)
            enable_indexes: Enable index file usage for improved query performance (default: False)
            
        Raises:
            OSError: If base_path cannot be created
        """
        try:
            self.base_path = Path(base_path)
            self.base_path.mkdir(exist_ok=True, parents=True)
        except OSError as e:
            logger.error(f"Failed to create database directory at {base_path}: {e}")
            raise
        
        # Cache configuration
        self._enable_cache = enable_cache
        self._cache_ttl_seconds = cache_ttl_seconds
        
        # Cache storage: {key: (value, expiry_timestamp)}
        self._cache: Dict[str, tuple[Any, float]] = {}
        
        # Cache statistics
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Access tracking for cache warming
        self._access_counts: Dict[str, int] = {}
        
        # Store warm cache configuration for lazy initialization
        self._warm_cache_on_startup = warm_cache_on_startup
        self._warm_cache_filter = warm_cache_filter
        self._cache_warmed = False
        
        # Index configuration
        self._enable_indexes = enable_indexes
        self._indexes_path = self.base_path / "_indexes"
        if self._enable_indexes:
            try:
                self._indexes_path.mkdir(exist_ok=True, parents=True)
            except OSError as e:
                logger.warning(f"Failed to create indexes directory: {e}. Indexes will be disabled.")
                self._enable_indexes = False
        
        logger.info(
            f"FileDatabase initialized at {base_path} "
            f"(cache={'enabled' if enable_cache else 'disabled'}, "
            f"indexes={'enabled' if self._enable_indexes else 'disabled'})"
        )

    async def _ensure_cache_warmed(self):
        """Ensure cache is warmed on first access if configured."""
        if not self._warm_cache_on_startup or self._cache_warmed or not self._enable_cache:
            return
        
        self._cache_warmed = True
        
        if not self._warm_cache_filter:
            return
        
        # Extract attribute filters from the filter dict
        attr_filters = {}
        for key, value in self._warm_cache_filter.items():
            if key.startswith("attributes."):
                attr_key = key.replace("attributes.", "")
                attr_filters[attr_key] = value
        
        # Load entities matching the filter
        entities = await self.list_entities(
            limit=100,
            attr_filters=attr_filters if attr_filters else None
        )
        
        # Warm cache with these entities
        entity_ids = [entity.id for entity in entities]
        await self.warm_cache(entity_ids=entity_ids)

    # ========================================================================
    # Cache Management Methods
    # ========================================================================
    
    def _get_from_cache(self, key: str) -> Optional[Any]:
        """Get a value from cache if it exists and hasn't expired.
        
        This method implements a read-through cache with automatic TTL refresh
        on access. Cache hits refresh the TTL to keep frequently accessed items
        in cache longer.
        
        Args:
            key: Cache key (typically an entity or relationship ID)
            
        Returns:
            Cached value or None if not found or expired
        """
        if not self._enable_cache:
            return None
        
        if key not in self._cache:
            self._cache_misses += 1
            return None
        
        value, expiry = self._cache[key]
        current_time = time.time()
        
        # Check if entry has expired
        if current_time >= expiry:
            # Remove expired entry
            del self._cache[key]
            self._cache_misses += 1
            logger.debug(f"Cache expired for key: {key}")
            return None
        
        # Cache hit - refresh TTL to keep hot items in cache
        self._cache[key] = (value, current_time + self._cache_ttl_seconds)
        self._cache_hits += 1
        
        # Track access for cache warming
        self._access_counts[key] = self._access_counts.get(key, 0) + 1
        
        return value

    def _put_in_cache(self, key: str, value: Any):
        """Put a value in cache with TTL.
        
        Args:
            key: Cache key (typically an entity or relationship ID)
            value: Value to cache (Entity, Relationship, etc.)
        """
        if not self._enable_cache:
            return
        
        expiry = time.time() + self._cache_ttl_seconds
        self._cache[key] = (value, expiry)
        logger.debug(f"Cached key: {key}")

    def _invalidate_cache(self, key: str):
        """Invalidate a cache entry.
        
        This should be called whenever an entity or relationship is updated
        or deleted to ensure cache consistency.
        
        Args:
            key: Cache key to invalidate
        """
        if not self._enable_cache:
            return
        
        if key in self._cache:
            del self._cache[key]
            logger.debug(f"Invalidated cache for key: {key}")

    def clear_cache(self):
        """Clear all cache entries.
        
        This removes all cached entities and relationships but does not
        reset cache statistics. Use this when you need to force a full
        cache refresh.
        """
        self._cache.clear()
        logger.debug("Cache cleared")

    def clear_cache_stats(self):
        """Clear cache statistics.
        
        Resets hit/miss counters to zero. This does not affect cached
        entries themselves.
        """
        self._cache_hits = 0
        self._cache_misses = 0
        logger.debug("Cache statistics cleared")

    def get_cache_stats(self) -> Dict[str, Union[int, float]]:
        """Get cache statistics.
        
        Returns:
            Dictionary with cache statistics:
            - hits: Number of cache hits
            - misses: Number of cache misses
            - hit_rate: Cache hit rate (0.0 to 1.0)
            - size: Current cache size
        """
        total_accesses = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total_accesses if total_accesses > 0 else 0.0
        
        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "hit_rate": hit_rate,
            "size": len(self._cache),
        }

    def cleanup_expired_cache_entries(self):
        """Remove expired entries from cache.
        
        This method scans the cache and removes all entries that have
        exceeded their TTL. It's useful for periodic cleanup to free
        memory, though expired entries are also removed lazily on access.
        
        Returns:
            Number of entries removed
        """
        if not self._enable_cache:
            return 0
        
        current_time = time.time()
        expired_keys = [
            key for key, (value, expiry) in self._cache.items()
            if current_time >= expiry
        ]
        
        for key in expired_keys:
            del self._cache[key]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        return len(expired_keys)

    async def warm_cache(
        self,
        entity_ids: Optional[List[str]] = None,
        entity_type: Optional[str] = None,
    ):
        """Warm cache with specified entities.
        
        Pre-loads entities into the cache to improve subsequent access times.
        This is useful for warming the cache with frequently accessed entities
        at application startup or after cache clearing.
        
        Args:
            entity_ids: List of entity IDs to warm (optional)
            entity_type: Entity type to warm all entities of (optional)
            
        Note:
            Only one of entity_ids or entity_type should be specified.
            If both are provided, entity_ids takes precedence.
        """
        if not self._enable_cache:
            logger.debug("Cache warming skipped - caching is disabled")
            return
        
        if entity_ids:
            # Warm cache with specific entity IDs
            for entity_id in entity_ids:
                entity = await self._get_entity_from_disk(entity_id)
                if entity:
                    self._put_in_cache(entity_id, entity)
        
        elif entity_type:
            # Warm cache with all entities of a specific type
            entities = await self.list_entities(entity_type=entity_type, limit=1000)
            for entity in entities:
                self._put_in_cache(entity.id, entity)

    async def warm_cache_most_accessed(self, limit: int = 10):
        """Warm cache with most frequently accessed entities.
        
        Args:
            limit: Number of most accessed entities to warm
        """
        if not self._enable_cache:
            return
        
        # Sort entities by access count
        sorted_entities = sorted(
            self._access_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Get top N entity IDs
        top_entity_ids = [entity_id for entity_id, count in sorted_entities[:limit]]
        
        # Warm cache with these entities
        await self.warm_cache(entity_ids=top_entity_ids)

    async def _get_entity_from_disk(self, entity_id: str) -> Optional[Entity]:
        """Get entity directly from disk without cache.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Entity or None if not found
        """
        file_path = self._id_to_path(entity_id)
        
        if not file_path.exists():
            return None
        
        with open(file_path, "r") as f:
            data = json.load(f)
        
        return self._entity_from_dict(data)

    async def _get_relationship_from_disk(self, relationship_id: str) -> Optional[Relationship]:
        """Get relationship directly from disk without cache.
        
        Args:
            relationship_id: Relationship ID
            
        Returns:
            Relationship or None if not found
        """
        file_path = self._id_to_path(relationship_id)
        
        if not file_path.exists():
            return None
        
        with open(file_path, "r") as f:
            data = json.load(f)
        
        return Relationship.model_validate(data)

    async def batch_get_entities(self, entity_ids: List[str]) -> List[Optional[Entity]]:
        """Batch retrieve multiple entities by their IDs.
        
        This method is optimized for retrieving multiple entities at once,
        reducing I/O overhead compared to individual get_entity calls.
        
        Args:
            entity_ids: List of entity IDs to retrieve
            
        Returns:
            List of entities in the same order as entity_ids.
            None is returned for entities that don't exist.
        """
        if not entity_ids:
            return []
        
        # Ensure cache is warmed on first access
        await self._ensure_cache_warmed()
        
        results = []
        entities_to_load = []
        entity_indices = []
        
        # First pass: check cache for all entities
        for i, entity_id in enumerate(entity_ids):
            cached_entity = self._get_from_cache(entity_id)
            if cached_entity is not None:
                results.append(cached_entity)
            else:
                # Mark this position for loading from disk
                results.append(None)
                entities_to_load.append(entity_id)
                entity_indices.append(i)
        
        # Second pass: batch load uncached entities from disk
        if entities_to_load:
            # Use concurrent file reads for better performance
            import asyncio
            
            async def load_entity(entity_id: str) -> Optional[Entity]:
                """Load a single entity from disk."""
                file_path = self._id_to_path(entity_id)
                
                if not file_path.exists():
                    return None
                
                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)
                    
                    entity = self._entity_from_dict(data)
                    
                    # Put in cache
                    self._put_in_cache(entity_id, entity)
                    
                    return entity
                except (json.JSONDecodeError, ValueError, KeyError):
                    return None
            
            # Load all entities concurrently
            loaded_entities = await asyncio.gather(
                *[load_entity(entity_id) for entity_id in entities_to_load]
            )
            
            # Fill in the results at the correct positions
            for idx, entity in zip(entity_indices, loaded_entities):
                results[idx] = entity
        
        return results

    def _update_type_index(self, entity: Entity):
        """Update the type index with an entity.
        
        Args:
            entity: Entity to add to the index
        """
        if not self._enable_indexes:
            return
        
        index_path = self._indexes_path / "by_type.json"
        
        # Load existing index
        if index_path.exists():
            with open(index_path, "r") as f:
                type_index = json.load(f)
        else:
            type_index = {}
        
        # Add entity to type index
        entity_type = entity.type
        if entity_type not in type_index:
            type_index[entity_type] = []
        
        # Remove existing entry if present (for updates)
        type_index[entity_type] = [
            eid for eid in type_index[entity_type] if eid != entity.id
        ]
        
        # Add new entry
        type_index[entity_type].append(entity.id)
        
        # Save index
        with open(index_path, "w") as f:
            json.dump(type_index, f, indent=2, ensure_ascii=False)

    def _remove_from_type_index(self, entity_id: str, entity_type: str):
        """Remove an entity from the type index.
        
        Args:
            entity_id: Entity ID to remove
            entity_type: Entity type
        """
        if not self._enable_indexes:
            return
        
        index_path = self._indexes_path / "by_type.json"
        
        if not index_path.exists():
            return
        
        # Load existing index
        with open(index_path, "r") as f:
            type_index = json.load(f)
        
        # Remove entity from type index
        if entity_type in type_index:
            type_index[entity_type] = [
                eid for eid in type_index[entity_type] if eid != entity_id
            ]
        
        # Save index
        with open(index_path, "w") as f:
            json.dump(type_index, f, indent=2, ensure_ascii=False)

    async def rebuild_indexes(self):
        """Rebuild all indexes from scratch.
        
        This method scans all entities in the database and rebuilds
        the index files. Useful for recovering from index corruption
        or after bulk data imports.
        """
        if not self._enable_indexes:
            return
        
        # Clear existing indexes
        if self._indexes_path.exists():
            import shutil
            shutil.rmtree(self._indexes_path)
        self._indexes_path.mkdir(exist_ok=True, parents=True)
        
        # Build type index
        type_index = {}
        
        # Scan all entities
        entity_path = self.base_path / "entity"
        if entity_path.exists():
            for file_path in entity_path.rglob("*.json"):
                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)
                    
                    # Check if this is an entity (has 'type' field)
                    if "type" not in data:
                        continue
                    
                    entity = self._entity_from_dict(data)
                    
                    # Add to type index
                    entity_type = entity.type
                    if entity_type not in type_index:
                        type_index[entity_type] = []
                    type_index[entity_type].append(entity.id)
                    
                except (json.JSONDecodeError, ValueError, KeyError):
                    # Skip invalid files
                    continue
        
        # Save type index
        index_path = self._indexes_path / "by_type.json"
        with open(index_path, "w") as f:
            json.dump(type_index, f, indent=2, ensure_ascii=False)

    # ========================================================================
    # File System Helper Methods
    # ========================================================================
    
    def _id_to_path(self, obj_id: str) -> Path:
        """Convert an object ID to a file path.
        
        Converts IDs like 'entity:person/ram-chandra-poudel' to file paths
        like 'nes-db/v2/entity/person/ram-chandra-poudel.json'.
        
        Args:
            obj_id: Object identifier following the pattern 'type:subtype/slug'
            
        Returns:
            Path to the JSON file for this object
            
        Examples:
            >>> db._id_to_path('entity:person/ram-chandra-poudel')
            Path('nes-db/v2/entity/person/ram-chandra-poudel.json')
            >>> db._id_to_path('relationship:rel-123')
            Path('nes-db/v2/relationship/rel-123.json')
        """
        # Replace colons with slashes and add .json extension
        file_path = obj_id.replace(":", "/") + ".json"
        return self.base_path / file_path

    def _ensure_dir(self, file_path: Path):
        """Ensure the directory for a file path exists.
        
        Creates all parent directories if they don't exist. This is called
        before writing any file to ensure the directory structure is in place.
        
        Args:
            file_path: Path to a file
            
        Raises:
            OSError: If directory creation fails
        """
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create directory for {file_path}: {e}")
            raise

    # ========================================================================
    # Entity CRUD Operations
    # ========================================================================
    
    async def put_entity(self, entity: Entity) -> Entity:
        """Store an entity in the database.
        
        Writes the entity to a JSON file, updates indexes, and invalidates cache.
        Computed fields are automatically removed before serialization.
        
        Args:
            entity: The entity to store
            
        Returns:
            The stored entity
            
        Raises:
            OSError: If file write fails
            ValueError: If entity serialization fails
        """
        try:
            file_path = self._id_to_path(entity.id)
            self._ensure_dir(file_path)
            
            # Serialize entity and remove computed fields
            data = self._serialize_entity(entity)
            
            # Write to file with atomic operation (write to temp, then rename)
            self._write_json_file(file_path, data)
            
            # Invalidate cache for this entity
            self._invalidate_cache(entity.id)
            
            # Update indexes
            self._update_type_index(entity)
            
            logger.debug(f"Stored entity: {entity.id}")
            return entity
            
        except Exception as e:
            logger.error(f"Failed to store entity {entity.id}: {e}")
            raise
    
    def _serialize_entity(self, entity: Entity) -> dict:
        """Serialize an entity to a dictionary, removing computed fields.
        
        Args:
            entity: Entity to serialize
            
        Returns:
            Dictionary representation suitable for JSON storage
        """
        data = entity.model_dump(mode="json")
        
        # Remove computed fields from entity
        data.pop("id", None)
        data.pop("location_type", None)  # Location computed field
        data.pop("administrative_level", None)  # Location computed field
        
        # Remove computed fields from nested version_summary
        if "version_summary" in data and isinstance(data["version_summary"], dict):
            data["version_summary"].pop("id", None)
        
        return data
    
    def _write_json_file(self, file_path: Path, data: dict):
        """Write data to a JSON file with consistent formatting.
        
        Args:
            file_path: Path to write to
            data: Data to serialize
            
        Raises:
            OSError: If file write fails
            ValueError: If JSON serialization fails
        """
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                default=str,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )

    async def get_entity(self, entity_id: str) -> Optional[Entity]:
        """Retrieve an entity by its ID.
        
        Uses cache if enabled, otherwise reads from disk. Cache is automatically
        warmed on first access if configured.
        
        Args:
            entity_id: The unique identifier of the entity
            
        Returns:
            The entity if found, None otherwise
            
        Raises:
            ValueError: If entity data is invalid
            json.JSONDecodeError: If JSON file is malformed
        """
        try:
            # Ensure cache is warmed on first access
            await self._ensure_cache_warmed()
            
            # Try to get from cache first
            cached_entity = self._get_from_cache(entity_id)
            if cached_entity is not None:
                return cached_entity
            
            # Cache miss - load from disk
            entity = await self._load_entity_from_disk(entity_id)
            
            # Put in cache if found
            if entity:
                self._put_in_cache(entity_id, entity)
            
            return entity
            
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            raise
    
    async def _load_entity_from_disk(self, entity_id: str) -> Optional[Entity]:
        """Load an entity from disk without cache.
        
        Args:
            entity_id: Entity ID to load
            
        Returns:
            Entity if found, None otherwise
            
        Raises:
            ValueError: If entity data is invalid
            json.JSONDecodeError: If JSON file is malformed
        """
        file_path = self._id_to_path(entity_id)
        
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            return self._entity_from_dict(data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in {file_path}: {e}")
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid entity data in {file_path}: {e}")
            raise

    async def delete_entity(self, entity_id: str) -> bool:
        """Delete an entity from the database.
        
        Removes the entity file, invalidates cache, and updates indexes.
        
        Args:
            entity_id: The unique identifier of the entity to delete
            
        Returns:
            True if the entity was deleted, False if it didn't exist
            
        Raises:
            OSError: If file deletion fails
        """
        try:
            file_path = self._id_to_path(entity_id)
            
            if not file_path.exists():
                return False
            
            # Get entity type before deletion for index update
            entity_type = None
            if self._enable_indexes:
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    entity_type = data.get("type")
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.warning(f"Could not read entity type before deletion: {e}")
            
            # Delete the file
            file_path.unlink()
            
            # Invalidate cache for this entity
            self._invalidate_cache(entity_id)
            
            # Update indexes
            if entity_type:
                self._remove_from_type_index(entity_id, entity_type)
            
            logger.debug(f"Deleted entity: {entity_id}")
            return True
            
        except OSError as e:
            logger.error(f"Failed to delete entity {entity_id}: {e}")
            raise

    async def list_entities(
        self,
        limit: int = 100,
        offset: int = 0,
        entity_type: Optional[str] = None,
        sub_type: Optional[str] = None,
        attr_filters: Optional[Dict[str, Union[str, int, float, bool]]] = None,
    ) -> List[Entity]:
        """List entities with optional filtering and pagination.
        
        Args:
            limit: Maximum number of entities to return
            offset: Number of entities to skip
            entity_type: Filter by entity type (person, organization, location)
            sub_type: Filter by entity subtype
            attr_filters: Filter by entity attributes (AND logic)
            
        Returns:
            List of entities matching the criteria
            
        Note:
            Results are not sorted. For sorted results, use search_entities.
        """
        # Build search path based on type/subtype
        search_path = self._build_entity_search_path(entity_type, sub_type)
        
        # If search path doesn't exist, return empty list
        if not search_path.exists():
            logger.debug(f"Search path does not exist: {search_path}")
            return []
        
        entities = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            # Skip if we already have enough entities
            if len(entities) >= limit + offset:
                break
            
            try:
                entity = self._load_and_filter_entity(file_path, attr_filters)
                if entity:
                    entities.append(entity)
                    
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Skip invalid files but log the error
                logger.warning(f"Skipping invalid entity file {file_path}: {e}")
                continue
        
        # Apply pagination
        return entities[offset : offset + limit]
    
    def _build_entity_search_path(
        self, 
        entity_type: Optional[str] = None, 
        sub_type: Optional[str] = None
    ) -> Path:
        """Build the search path for entity queries.
        
        Args:
            entity_type: Optional entity type filter
            sub_type: Optional entity subtype filter
            
        Returns:
            Path to search for entities
        """
        if sub_type and entity_type:
            return self.base_path / "entity" / entity_type / sub_type
        elif entity_type:
            return self.base_path / "entity" / entity_type
        else:
            return self.base_path / "entity"
    
    def _load_and_filter_entity(
        self, 
        file_path: Path, 
        attr_filters: Optional[Dict[str, Union[str, int, float, bool]]] = None
    ) -> Optional[Entity]:
        """Load an entity from a file and apply attribute filters.
        
        Args:
            file_path: Path to the entity JSON file
            attr_filters: Optional attribute filters to apply
            
        Returns:
            Entity if it passes filters, None otherwise
            
        Raises:
            json.JSONDecodeError: If JSON is malformed
            ValueError: If entity data is invalid
        """
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Check if this is an entity (has 'type' field)
        if "type" not in data:
            return None
        
        # Apply attribute filters if provided
        if attr_filters and not self._matches_attribute_filters(data, attr_filters):
            return None
        
        # Parse entity
        return self._entity_from_dict(data)
    
    def _matches_attribute_filters(
        self, 
        data: dict, 
        attr_filters: Dict[str, Union[str, int, float, bool]]
    ) -> bool:
        """Check if entity data matches attribute filters.
        
        Args:
            data: Entity data dictionary
            attr_filters: Attribute filters to check (AND logic)
            
        Returns:
            True if all filters match, False otherwise
        """
        attributes = data.get("attributes") or {}
        # Check if all filter criteria match (AND logic)
        return all(attributes.get(k) == v for k, v in attr_filters.items())

    async def search_entities(
        self,
        query: Optional[str] = None,
        entity_type: Optional[str] = None,
        sub_type: Optional[str] = None,
        attr_filters: Optional[Dict[str, Union[str, int, float, bool]]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Entity]:
        """Search entities with text query and optional filtering.
        
        Performs case-insensitive text search across entity name fields
        (both English and Nepali). Supports filtering by type, subtype,
        and attributes. Results are ranked by relevance.
        
        Args:
            query: Text query to search for in entity names (case-insensitive)
            entity_type: Filter by entity type (person, organization, location)
            sub_type: Filter by entity subtype
            attr_filters: Filter by entity attributes (AND logic)
            limit: Maximum number of entities to return
            offset: Number of entities to skip
            
        Returns:
            List of entities matching the search criteria, ranked by relevance
        """
        # Build search path based on type/subtype
        search_path = self._build_entity_search_path(entity_type, sub_type)
        
        # If search path doesn't exist, return empty list
        if not search_path.exists():
            logger.debug(f"Search path does not exist: {search_path}")
            return []
        
        # Normalize query for case-insensitive search
        normalized_query = query.lower() if query else None
        
        # Collect entities with relevance scores
        entities_with_scores = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            try:
                entity = self._load_and_filter_entity(file_path, attr_filters)
                if not entity:
                    continue
                
                # If no query, include all entities (filtered by type/attributes)
                if not normalized_query:
                    entities_with_scores.append((entity, 0))
                    continue
                
                # Calculate relevance score based on name matches
                score = self._calculate_relevance_score(entity, normalized_query)
                
                # Only include entities with positive scores (matches found)
                if score > 0:
                    entities_with_scores.append((entity, score))
                
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Skip invalid files but log the error
                logger.warning(f"Skipping invalid entity file {file_path}: {e}")
                continue
        
        # Sort by relevance score (higher is better)
        entities_with_scores.sort(key=lambda x: x[1], reverse=True)
        
        # Extract entities and apply pagination
        entities = [entity for entity, score in entities_with_scores]
        return entities[offset : offset + limit]

    def _calculate_relevance_score(self, entity: Entity, normalized_query: str) -> int:
        """Calculate relevance score for an entity based on query match.
        
        Scoring logic:
        - Exact full name match: 100 points
        - Full name contains query: 50 points
        - First/last name exact match: 75 points
        - First/last name contains query: 25 points
        - Primary name match: bonus 20 points
        - Alias/alternate name match: bonus 10 points
        
        Args:
            entity: Entity to score
            normalized_query: Lowercase query string
            
        Returns:
            Relevance score (higher is better, 0 means no match)
        """
        score = 0
        
        for name in entity.names:
            # Determine name kind bonus
            name_kind_bonus = 20 if name.kind.value == "PRIMARY" else 10
            
            # Check English names
            if name.en:
                # Check full name
                if name.en.full:
                    full_name_lower = name.en.full.lower()
                    if full_name_lower == normalized_query:
                        score += 100 + name_kind_bonus
                    elif normalized_query in full_name_lower:
                        score += 50 + name_kind_bonus
                
                # Check first name
                if name.en.first:
                    first_name_lower = name.en.first.lower()
                    if first_name_lower == normalized_query:
                        score += 75 + name_kind_bonus
                    elif normalized_query in first_name_lower:
                        score += 25 + name_kind_bonus
                
                # Check last name
                if name.en.last:
                    last_name_lower = name.en.last.lower()
                    if last_name_lower == normalized_query:
                        score += 75 + name_kind_bonus
                    elif normalized_query in last_name_lower:
                        score += 25 + name_kind_bonus
                
                # Check middle name
                if name.en.middle:
                    middle_name_lower = name.en.middle.lower()
                    if middle_name_lower == normalized_query:
                        score += 75 + name_kind_bonus
                    elif normalized_query in middle_name_lower:
                        score += 25 + name_kind_bonus
            
            # Check Nepali names (Devanagari)
            if name.ne:
                # Check full name
                if name.ne.full:
                    full_name_lower = name.ne.full.lower()
                    if full_name_lower == normalized_query:
                        score += 100 + name_kind_bonus
                    elif normalized_query in full_name_lower:
                        score += 50 + name_kind_bonus
                
                # Check first name
                if name.ne.first:
                    first_name_lower = name.ne.first.lower()
                    if first_name_lower == normalized_query:
                        score += 75 + name_kind_bonus
                    elif normalized_query in first_name_lower:
                        score += 25 + name_kind_bonus
                
                # Check last name
                if name.ne.last:
                    last_name_lower = name.ne.last.lower()
                    if last_name_lower == normalized_query:
                        score += 75 + name_kind_bonus
                    elif normalized_query in last_name_lower:
                        score += 25 + name_kind_bonus
                
                # Check middle name
                if name.ne.middle:
                    middle_name_lower = name.ne.middle.lower()
                    if middle_name_lower == normalized_query:
                        score += 75 + name_kind_bonus
                    elif normalized_query in middle_name_lower:
                        score += 25 + name_kind_bonus
        
        return score

    def _entity_from_dict(self, data: dict) -> Entity:
        """Convert a dictionary to an Entity instance.
        
        Args:
            data: Dictionary representation of an entity
            
        Returns:
            Entity instance of the appropriate subclass
            
        Raises:
            ValueError: If entity type is invalid
            KeyError: If entity type/subtype combination is not found
        """
        if "type" not in data:
            raise ValueError("Entity must have a 'type' field")
        
        entity_type = EntityType(data["type"])
        entity_subtype = (
            EntitySubType(data["sub_type"]) if data.get("sub_type") else None
        )
        
        # Determine the correct entity class based on type and subtype
        if entity_type == EntityType.PERSON:
            return Person.model_validate(data)
        elif entity_type == EntityType.ORGANIZATION:
            if entity_subtype == EntitySubType.POLITICAL_PARTY:
                return PoliticalParty.model_validate(data)
            elif entity_subtype == EntitySubType.GOVERNMENT_BODY:
                return GovernmentBody.model_validate(data)
            else:
                return Organization.model_validate(data)
        elif entity_type == EntityType.LOCATION:
            return Location.model_validate(data)
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")

    # ========================================================================
    # Relationship CRUD Operations
    # ========================================================================
    
    async def put_relationship(self, relationship: Relationship) -> Relationship:
        """Store a relationship in the database.
        
        Args:
            relationship: The relationship to store
            
        Returns:
            The stored relationship
            
        Raises:
            OSError: If file write fails
            ValueError: If relationship serialization fails
        """
        try:
            file_path = self._id_to_path(relationship.id)
            self._ensure_dir(file_path)
            
            # Serialize relationship and remove computed fields
            data = self._serialize_relationship(relationship)
            
            # Write to file
            self._write_json_file(file_path, data)
            
            # Invalidate cache for this relationship
            self._invalidate_cache(relationship.id)
            
            logger.debug(f"Stored relationship: {relationship.id}")
            return relationship
            
        except Exception as e:
            logger.error(f"Failed to store relationship {relationship.id}: {e}")
            raise
    
    def _serialize_relationship(self, relationship: Relationship) -> dict:
        """Serialize a relationship to a dictionary, removing computed fields.
        
        Args:
            relationship: Relationship to serialize
            
        Returns:
            Dictionary representation suitable for JSON storage
        """
        data = relationship.model_dump(mode="json")
        data.pop("id", None)
        if "version_summary" in data and isinstance(data["version_summary"], dict):
            data["version_summary"].pop("id", None)
        return data

    async def get_relationship(self, relationship_id: str) -> Optional[Relationship]:
        """Retrieve a relationship by its ID.
        
        Uses cache if enabled, otherwise reads from disk.
        
        Args:
            relationship_id: The unique identifier of the relationship
            
        Returns:
            The relationship if found, None otherwise
            
        Raises:
            ValueError: If relationship data is invalid
            json.JSONDecodeError: If JSON file is malformed
        """
        try:
            # Try to get from cache first
            cached_relationship = self._get_from_cache(relationship_id)
            if cached_relationship is not None:
                return cached_relationship
            
            # Cache miss - load from disk
            relationship = await self._load_relationship_from_disk(relationship_id)
            
            # Put in cache if found
            if relationship:
                self._put_in_cache(relationship_id, relationship)
            
            return relationship
            
        except Exception as e:
            logger.error(f"Failed to get relationship {relationship_id}: {e}")
            raise
    
    async def _load_relationship_from_disk(self, relationship_id: str) -> Optional[Relationship]:
        """Load a relationship from disk without cache.
        
        Args:
            relationship_id: Relationship ID to load
            
        Returns:
            Relationship if found, None otherwise
            
        Raises:
            ValueError: If relationship data is invalid
            json.JSONDecodeError: If JSON file is malformed
        """
        file_path = self._id_to_path(relationship_id)
        
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            return Relationship.model_validate(data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in {file_path}: {e}")
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid relationship data in {file_path}: {e}")
            raise

    async def delete_relationship(self, relationship_id: str) -> bool:
        """Delete a relationship from the database.
        
        Args:
            relationship_id: The unique identifier of the relationship to delete
            
        Returns:
            True if the relationship was deleted, False if it didn't exist
            
        Raises:
            OSError: If file deletion fails
        """
        try:
            file_path = self._id_to_path(relationship_id)
            
            if not file_path.exists():
                return False
            
            file_path.unlink()
            
            # Invalidate cache for this relationship
            self._invalidate_cache(relationship_id)
            
            logger.debug(f"Deleted relationship: {relationship_id}")
            return True
            
        except OSError as e:
            logger.error(f"Failed to delete relationship {relationship_id}: {e}")
            raise

    async def list_relationships(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Relationship]:
        """List relationships with pagination.
        
        Args:
            limit: Maximum number of relationships to return
            offset: Number of relationships to skip
            
        Returns:
            List of relationships
        """
        search_path = self.base_path / "relationship"
        
        if not search_path.exists():
            logger.debug(f"Relationship path does not exist: {search_path}")
            return []
        
        relationships = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            if len(relationships) >= limit + offset:
                break
            
            try:
                relationship = self._load_relationship_from_file(file_path)
                if relationship:
                    relationships.append(relationship)
                    
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Skip invalid files but log the error
                logger.warning(f"Skipping invalid relationship file {file_path}: {e}")
                continue
        
        # Apply pagination
        return relationships[offset : offset + limit]
    
    def _load_relationship_from_file(self, file_path: Path) -> Optional[Relationship]:
        """Load a relationship from a file.
        
        Args:
            file_path: Path to the relationship JSON file
            
        Returns:
            Relationship if valid, None otherwise
            
        Raises:
            json.JSONDecodeError: If JSON is malformed
            ValueError: If relationship data is invalid
        """
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Check if this is a relationship (has source_entity_id)
        if "source_entity_id" not in data:
            return None
        
        return Relationship.model_validate(data)

    async def list_relationships_by_entity(
        self,
        entity_id: str,
        direction: str = "both",
        relationship_type: Optional[str] = None,
        active_on: Optional["date"] = None,
        start_date_from: Optional["date"] = None,
        start_date_to: Optional["date"] = None,
        currently_active: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Relationship]:
        """List relationships by entity (source or target).
        
        Args:
            entity_id: The entity ID to search for
            direction: Direction filter - "source", "target", or "both" (default)
            relationship_type: Optional filter by relationship type
            active_on: Optional filter for relationships active on a specific date
            start_date_from: Optional filter for relationships starting from this date
            start_date_to: Optional filter for relationships starting before this date
            currently_active: Optional filter for relationships with no end date
            limit: Maximum number of relationships to return
            offset: Number of relationships to skip
            
        Returns:
            List of relationships matching the criteria
        """
        from datetime import date
        
        search_path = self.base_path / "relationship"
        
        if not search_path.exists():
            return []
        
        relationships = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                # Check if this is a relationship (has source_entity_id)
                if "source_entity_id" not in data:
                    continue
                
                # Apply direction filter
                if direction == "source" and data.get("source_entity_id") != entity_id:
                    continue
                elif direction == "target" and data.get("target_entity_id") != entity_id:
                    continue
                elif direction == "both":
                    if data.get("source_entity_id") != entity_id and data.get("target_entity_id") != entity_id:
                        continue
                
                # Apply relationship type filter
                if relationship_type and data.get("type") != relationship_type:
                    continue
                
                # Parse relationship for temporal filtering
                relationship = Relationship.model_validate(data)
                
                # Apply temporal filters
                if active_on is not None:
                    # Check if relationship was active on the specified date
                    if relationship.start_date and relationship.start_date > active_on:
                        continue
                    if relationship.end_date and relationship.end_date < active_on:
                        continue
                
                if start_date_from is not None:
                    # Filter relationships starting from this date
                    if not relationship.start_date or relationship.start_date < start_date_from:
                        continue
                
                if start_date_to is not None:
                    # Filter relationships starting before this date
                    if not relationship.start_date or relationship.start_date > start_date_to:
                        continue
                
                if currently_active is not None:
                    # Filter for currently active relationships (no end date)
                    if currently_active and relationship.end_date is not None:
                        continue
                    if not currently_active and relationship.end_date is None:
                        continue
                
                relationships.append(relationship)
                
            except (json.JSONDecodeError, ValueError, KeyError):
                # Skip invalid files
                continue
        
        # Apply pagination
        return relationships[offset : offset + limit]

    async def list_relationships_by_type(
        self,
        relationship_type: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Relationship]:
        """List relationships by relationship type.
        
        Args:
            relationship_type: The relationship type to filter by
            limit: Maximum number of relationships to return
            offset: Number of relationships to skip
            
        Returns:
            List of relationships matching the type
        """
        search_path = self.base_path / "relationship"
        
        if not search_path.exists():
            return []
        
        relationships = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                # Check if this is a relationship (has source_entity_id)
                if "source_entity_id" not in data:
                    continue
                
                # Apply type filter
                if data.get("type") != relationship_type:
                    continue
                
                relationship = Relationship.model_validate(data)
                relationships.append(relationship)
                
            except (json.JSONDecodeError, ValueError, KeyError):
                # Skip invalid files
                continue
        
        # Apply pagination
        return relationships[offset : offset + limit]

    async def put_version(self, version: Version) -> Version:
        """Store a version in the database."""
        file_path = self._id_to_path(version.id)
        self._ensure_dir(file_path)
        
        # Serialize version and remove computed fields
        data = version.model_dump(mode="json")
        data.pop("id", None)
        
        with open(file_path, "w") as f:
            json.dump(
                data,
                f,
                default=str,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        
        return version

    async def get_version(self, version_id: str) -> Optional[Version]:
        """Retrieve a version by its ID."""
        file_path = self._id_to_path(version_id)
        
        if not file_path.exists():
            return None
        
        with open(file_path, "r") as f:
            data = json.load(f)
        
        return Version.model_validate(data)

    async def delete_version(self, version_id: str) -> bool:
        """Delete a version from the database."""
        file_path = self._id_to_path(version_id)
        
        if file_path.exists():
            file_path.unlink()
            return True
        
        return False

    async def list_versions(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Version]:
        """List versions with pagination."""
        search_path = self.base_path / "version"
        
        if not search_path.exists():
            return []
        
        versions = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            if len(versions) >= limit + offset:
                break
            
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                # Check if this is a version (has version_number)
                if "version_number" not in data:
                    continue
                
                version = Version.model_validate(data)
                versions.append(version)
                
            except (json.JSONDecodeError, ValueError, KeyError):
                # Skip invalid files
                continue
        
        # Apply pagination
        return versions[offset : offset + limit]

    async def list_versions_by_entity(
        self,
        entity_or_relationship_id: str,
        limit: int = 100,
        offset: int = 0,
        actor_slug: Optional[str] = None,
        created_after: Optional["datetime"] = None,
        created_before: Optional["datetime"] = None,
        min_version: Optional[int] = None,
        max_version: Optional[int] = None,
        order: str = "asc",
    ) -> List[Version]:
        """List versions for a specific entity or relationship with filtering.
        
        Args:
            entity_or_relationship_id: The entity or relationship ID to filter by
            limit: Maximum number of versions to return
            offset: Number of versions to skip
            actor_slug: Optional filter by actor slug
            created_after: Optional filter for versions created after this datetime
            created_before: Optional filter for versions created before this datetime
            min_version: Optional filter for minimum version number (inclusive)
            max_version: Optional filter for maximum version number (inclusive)
            order: Sort order - "asc" (default) or "desc"
            
        Returns:
            List of versions matching the criteria, sorted by version number
        """
        from datetime import datetime
        
        # Determine the search path based on entity or relationship ID
        # Version files are stored in: version/entity/{entity-id}/{version-number}.json
        # or: version/relationship/{relationship-id}/{version-number}.json
        
        # Use the _id_to_path method to get the correct path structure
        # This will handle the colon-to-slash conversion properly
        version_base_path = self._id_to_path(f"version:{entity_or_relationship_id}")
        
        # Remove the .json extension since we're looking for a directory
        search_path = version_base_path.parent / version_base_path.stem
        
        # If search path doesn't exist, return empty list
        if not search_path.exists():
            return []
        
        versions = []
        
        # Find all JSON files in the entity/relationship version directory
        for file_path in search_path.glob("*.json"):
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                # Check if this is a version (has version_number)
                if "version_number" not in data:
                    continue
                
                # Parse version
                version = Version.model_validate(data)
                
                # Apply actor filter
                if actor_slug and version.actor.slug != actor_slug:
                    continue
                
                # Apply date range filters
                # created_after: inclusive (>=)
                if created_after and version.created_at < created_after:
                    continue
                # created_before: exclusive (<)
                if created_before and version.created_at >= created_before:
                    continue
                
                # Apply version number range filters
                if min_version is not None and version.version_number < min_version:
                    continue
                if max_version is not None and version.version_number > max_version:
                    continue
                
                versions.append(version)
                
            except (json.JSONDecodeError, ValueError, KeyError):
                # Skip invalid files
                continue
        
        # Sort by version number
        reverse_order = order.lower() == "desc"
        versions.sort(key=lambda v: v.version_number, reverse=reverse_order)
        
        # Apply pagination
        return versions[offset : offset + limit]

    async def put_actor(self, actor: Actor) -> Actor:
        """Store an actor in the database."""
        file_path = self._id_to_path(actor.id)
        self._ensure_dir(file_path)
        
        # Serialize actor and remove computed fields
        data = actor.model_dump(mode="json")
        data.pop("id", None)
        
        with open(file_path, "w") as f:
            json.dump(
                data,
                f,
                default=str,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        
        return actor

    async def get_actor(self, actor_id: str) -> Optional[Actor]:
        """Retrieve an actor by its ID."""
        file_path = self._id_to_path(actor_id)
        
        if not file_path.exists():
            return None
        
        with open(file_path, "r") as f:
            data = json.load(f)
        
        return Actor.model_validate(data)

    async def delete_actor(self, actor_id: str) -> bool:
        """Delete an actor from the database."""
        file_path = self._id_to_path(actor_id)
        
        if file_path.exists():
            file_path.unlink()
            return True
        
        return False

    async def list_actors(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Actor]:
        """List actors with pagination."""
        search_path = self.base_path / "actor"
        
        if not search_path.exists():
            return []
        
        actors = []
        
        # Recursively find all JSON files
        for file_path in search_path.rglob("*.json"):
            if len(actors) >= limit + offset:
                break
            
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                
                # Check if this is an actor (has slug)
                if "slug" not in data:
                    continue
                
                actor = Actor.model_validate(data)
                actors.append(actor)
                
            except (json.JSONDecodeError, ValueError, KeyError):
                # Skip invalid files
                continue
        
        # Apply pagination
        return actors[offset : offset + limit]
