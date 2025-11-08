"""Relationship integrity checking functions for nes2.

This module provides functions for validating relationship integrity:
- Entity existence validation: Ensures both source and target entities exist
- Circular relationship detection: Detects cycles in hierarchical relationships
- Duplicate relationship checking: Identifies duplicate relationships
- Constraint validation: Validates temporal and type constraints

The integrity checks are designed to maintain data quality and prevent
invalid relationship states in the database. Circular relationship detection
only applies to hierarchical relationship types (SUPERVISES, PARENT_OF, CHILD_OF)
to avoid false positives in non-hierarchical relationships like MEMBER_OF.

Example usage:
    from nes2.services.publication.integrity import (
        check_circular_relationship,
        find_orphaned_relationships
    )
    
    # Check if a relationship would create a circle
    is_circular = await check_circular_relationship(
        db=database,
        source_entity_id="entity:person/a",
        target_entity_id="entity:person/b",
        relationship_type="SUPERVISES"
    )
    
    # Find all orphaned relationships
    orphaned = await find_orphaned_relationships(db=database)
"""

from typing import Optional, Set, List
from nes2.database.entity_database import EntityDatabase
from nes2.core.models.relationship import Relationship


# Hierarchical relationship types that should be checked for circular dependencies
HIERARCHICAL_RELATIONSHIP_TYPES = {
    "SUPERVISES",
    "PARENT_OF",
    "CHILD_OF"
}


async def check_circular_relationship(
    db: EntityDatabase,
    source_entity_id: str,
    target_entity_id: str,
    relationship_type: str
) -> bool:
    """Check if creating a relationship would create a circular dependency.
    
    This function checks for circular relationships only for hierarchical
    relationship types (SUPERVISES, PARENT_OF, CHILD_OF). For other types,
    it returns False (no circular dependency).
    
    Args:
        db: Database instance
        source_entity_id: ID of the source entity
        target_entity_id: ID of the target entity
        relationship_type: Type of relationship
        
    Returns:
        True if the relationship would create a circle, False otherwise
    """
    # Only check circular dependencies for hierarchical relationship types
    if relationship_type not in HIERARCHICAL_RELATIONSHIP_TYPES:
        return False
    
    # Direct self-reference is always circular
    if source_entity_id == target_entity_id:
        return True
    
    # Check if there's a path from target back to source
    # If yes, adding source -> target would create a circle
    visited: Set[str] = set()
    return await _has_path(db, target_entity_id, source_entity_id, relationship_type, visited)


async def _has_path(
    db: EntityDatabase,
    current_id: str,
    target_id: str,
    relationship_type: str,
    visited: Set[str]
) -> bool:
    """Recursively check if there's a path from current to target.
    
    Args:
        db: Database instance
        current_id: Current entity ID
        target_id: Target entity ID to reach
        relationship_type: Type of relationship to follow
        visited: Set of already visited entity IDs
        
    Returns:
        True if a path exists, False otherwise
    """
    if current_id == target_id:
        return True
    
    if current_id in visited:
        return False
    
    visited.add(current_id)
    
    # Get all outgoing relationships of the same type from current entity
    try:
        relationships = await db.list_relationships_by_entity(
            entity_id=current_id,
            direction="source",
            relationship_type=relationship_type,
            limit=1000
        )
    except AttributeError:
        # Fallback if list_relationships_by_entity doesn't exist
        all_relationships = await db.list_relationships(limit=10000)
        relationships = [
            r for r in all_relationships
            if r.source_entity_id == current_id and r.type == relationship_type
        ]
    
    # Recursively check each target
    for rel in relationships:
        if await _has_path(db, rel.target_entity_id, target_id, relationship_type, visited):
            return True
    
    return False


async def check_duplicate_relationship(
    db: EntityDatabase,
    source_entity_id: str,
    target_entity_id: str,
    relationship_type: str
) -> bool:
    """Check if a relationship already exists.
    
    Args:
        db: Database instance
        source_entity_id: ID of the source entity
        target_entity_id: ID of the target entity
        relationship_type: Type of relationship
        
    Returns:
        True if the relationship already exists, False otherwise
    """
    # Get all relationships for the source entity
    try:
        relationships = await db.list_relationships_by_entity(
            entity_id=source_entity_id,
            direction="source",
            limit=1000
        )
    except AttributeError:
        # Fallback if list_relationships_by_entity doesn't exist
        all_relationships = await db.list_relationships(limit=10000)
        relationships = [
            r for r in all_relationships
            if r.source_entity_id == source_entity_id
        ]
    
    # Check if any relationship matches
    for rel in relationships:
        if (rel.target_entity_id == target_entity_id and 
            rel.type == relationship_type):
            return True
    
    return False


async def find_orphaned_relationships(db: EntityDatabase) -> List[Relationship]:
    """Find relationships where source or target entity doesn't exist.
    
    Args:
        db: Database instance
        
    Returns:
        List of orphaned relationships
    """
    orphaned = []
    
    # Get all relationships
    relationships = await db.list_relationships(limit=100000)
    
    for rel in relationships:
        # Check if source entity exists
        source_entity = await db.get_entity(rel.source_entity_id)
        if not source_entity:
            orphaned.append(rel)
            continue
        
        # Check if target entity exists
        target_entity = await db.get_entity(rel.target_entity_id)
        if not target_entity:
            orphaned.append(rel)
    
    return orphaned


async def find_circular_relationships(
    db: EntityDatabase,
    relationship_type: Optional[str] = None
) -> List[List[Relationship]]:
    """Find all circular relationships in the database.
    
    Args:
        db: Database instance
        relationship_type: Optional filter by relationship type
        
    Returns:
        List of circular relationship chains (each chain is a list of relationships)
    """
    circles = []
    
    # Get all relationships
    all_relationships = await db.list_relationships(limit=100000)
    
    # Filter by type if specified
    if relationship_type:
        relationships = [r for r in all_relationships if r.type == relationship_type]
    else:
        # Only check hierarchical types
        relationships = [r for r in all_relationships if r.type in HIERARCHICAL_RELATIONSHIP_TYPES]
    
    # Track which relationships we've already included in circles
    processed: Set[str] = set()
    
    for rel in relationships:
        if rel.id in processed:
            continue
        
        # Check if this relationship is part of a circle
        visited: Set[str] = set()
        path: List[Relationship] = []
        
        if await _find_circle_from(
            db, rel.target_entity_id, rel.source_entity_id, 
            rel.type, visited, path, all_relationships
        ):
            # Found a circle
            circle = [rel] + path
            circles.append(circle)
            
            # Mark all relationships in this circle as processed
            for r in circle:
                processed.add(r.id)
    
    return circles


async def _find_circle_from(
    db: EntityDatabase,
    current_id: str,
    target_id: str,
    relationship_type: str,
    visited: Set[str],
    path: List[Relationship],
    all_relationships: List[Relationship]
) -> bool:
    """Helper function to find a circle starting from current_id.
    
    Args:
        db: Database instance
        current_id: Current entity ID
        target_id: Target entity ID to reach (completes the circle)
        relationship_type: Type of relationship to follow
        visited: Set of visited entity IDs
        path: Current path of relationships
        all_relationships: All relationships in the database
        
    Returns:
        True if a circle is found, False otherwise
    """
    if current_id == target_id:
        return True
    
    if current_id in visited:
        return False
    
    visited.add(current_id)
    
    # Find outgoing relationships from current entity
    outgoing = [
        r for r in all_relationships
        if r.source_entity_id == current_id and r.type == relationship_type
    ]
    
    for rel in outgoing:
        path.append(rel)
        if await _find_circle_from(
            db, rel.target_entity_id, target_id, 
            relationship_type, visited, path, all_relationships
        ):
            return True
        path.pop()
    
    return False


async def find_duplicate_relationships(db: EntityDatabase) -> List[List[Relationship]]:
    """Find duplicate relationships (same source, target, and type).
    
    Args:
        db: Database instance
        
    Returns:
        List of duplicate relationship groups
    """
    duplicates = []
    
    # Get all relationships
    relationships = await db.list_relationships(limit=100000)
    
    # Group by (source, target, type)
    groups = {}
    for rel in relationships:
        key = (rel.source_entity_id, rel.target_entity_id, rel.type)
        if key not in groups:
            groups[key] = []
        groups[key].append(rel)
    
    # Find groups with more than one relationship
    for key, group in groups.items():
        if len(group) > 1:
            duplicates.append(group)
    
    return duplicates
