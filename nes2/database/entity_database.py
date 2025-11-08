"""Abstract EntityDatabase class for CRUD operations in nes2.

This module defines the abstract interface that all database implementations
must follow. It provides a consistent API for entity, relationship, version,
and actor operations.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from nes2.core.models.entity import Entity
from nes2.core.models.relationship import Relationship
from nes2.core.models.version import Actor, Version


class EntityDatabase(ABC):
    """Abstract base class for entity database operations.
    
    All database implementations must inherit from this class and implement
    all abstract methods. This ensures a consistent interface across different
    storage backends (file-based, SQL, NoSQL, etc.).
    """

    @abstractmethod
    async def put_entity(self, entity: Entity) -> Entity:
        """Store an entity in the database.
        
        Args:
            entity: The entity to store
            
        Returns:
            The stored entity
        """
        pass

    @abstractmethod
    async def get_entity(self, entity_id: str) -> Optional[Entity]:
        """Retrieve an entity by its ID.
        
        Args:
            entity_id: The unique identifier of the entity
            
        Returns:
            The entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def delete_entity(self, entity_id: str) -> bool:
        """Delete an entity from the database.
        
        Args:
            entity_id: The unique identifier of the entity to delete
            
        Returns:
            True if the entity was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
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
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def put_relationship(self, relationship: Relationship) -> Relationship:
        """Store a relationship in the database.
        
        Args:
            relationship: The relationship to store
            
        Returns:
            The stored relationship
        """
        pass

    @abstractmethod
    async def get_relationship(self, relationship_id: str) -> Optional[Relationship]:
        """Retrieve a relationship by its ID.
        
        Args:
            relationship_id: The unique identifier of the relationship
            
        Returns:
            The relationship if found, None otherwise
        """
        pass

    @abstractmethod
    async def delete_relationship(self, relationship_id: str) -> bool:
        """Delete a relationship from the database.
        
        Args:
            relationship_id: The unique identifier of the relationship to delete
            
        Returns:
            True if the relationship was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def put_version(self, version: Version) -> Version:
        """Store a version in the database.
        
        Args:
            version: The version to store
            
        Returns:
            The stored version
        """
        pass

    @abstractmethod
    async def get_version(self, version_id: str) -> Optional[Version]:
        """Retrieve a version by its ID.
        
        Args:
            version_id: The unique identifier of the version
            
        Returns:
            The version if found, None otherwise
        """
        pass

    @abstractmethod
    async def delete_version(self, version_id: str) -> bool:
        """Delete a version from the database.
        
        Args:
            version_id: The unique identifier of the version to delete
            
        Returns:
            True if the version was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
    async def list_versions(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Version]:
        """List versions with pagination.
        
        Note: This method will be enhanced in future tasks to require
        entity_id or relationship_id parameter for filtering.
        
        Args:
            limit: Maximum number of versions to return
            offset: Number of versions to skip
            
        Returns:
            List of versions
        """
        pass

    @abstractmethod
    async def put_actor(self, actor: Actor) -> Actor:
        """Store an actor in the database.
        
        Args:
            actor: The actor to store
            
        Returns:
            The stored actor
        """
        pass

    @abstractmethod
    async def get_actor(self, actor_id: str) -> Optional[Actor]:
        """Retrieve an actor by its ID.
        
        Args:
            actor_id: The unique identifier of the actor
            
        Returns:
            The actor if found, None otherwise
        """
        pass

    @abstractmethod
    async def delete_actor(self, actor_id: str) -> bool:
        """Delete an actor from the database.
        
        Args:
            actor_id: The unique identifier of the actor to delete
            
        Returns:
            True if the actor was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
    async def list_actors(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Actor]:
        """List actors with pagination.
        
        Args:
            limit: Maximum number of actors to return
            offset: Number of actors to skip
            
        Returns:
            List of actors
        """
        pass
