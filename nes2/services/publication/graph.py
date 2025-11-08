"""Relationship graph traversal functions for nes2.

This module provides functions for traversing and visualizing relationship graphs:
- Bidirectional traversal: Navigate relationships in any direction
- Depth-limited exploration: Control how far to traverse from a starting entity
- Path finding between entities: Find shortest paths using BFS
- Graph visualization: Generate visualizations in DOT, Mermaid, or JSON formats

The graph traversal functions use breadth-first search (BFS) to ensure shortest
paths are found and to efficiently explore the relationship graph. All functions
are async and work with the EntityDatabase interface.

Example usage:
    from nes2.services.publication.graph import (
        traverse_relationships,
        find_path,
        generate_graph_visualization
    )
    
    # Traverse relationships up to depth 2
    relationships = await traverse_relationships(
        db=database,
        entity_id="entity:person/ram-chandra-poudel",
        direction="both",
        depth=2
    )
    
    # Find path between two entities
    path = await find_path(
        db=database,
        source_entity_id="entity:person/a",
        target_entity_id="entity:person/b"
    )
    
    # Generate Mermaid diagram
    diagram = await generate_graph_visualization(
        db=database,
        entity_id="entity:person/ram-chandra-poudel",
        format="mermaid",
        depth=2
    )
"""

from typing import List, Dict, Any, Optional, Set
from collections import deque
import json

from nes2.database.entity_database import EntityDatabase
from nes2.core.models.relationship import Relationship


async def traverse_relationships(
    db: EntityDatabase,
    entity_id: str,
    direction: str = "both",
    depth: Optional[int] = 1
) -> List[Dict[str, Any]]:
    """Traverse relationships from an entity up to a specified depth.
    
    Args:
        db: Database instance
        entity_id: Starting entity ID
        direction: Direction to traverse - "outgoing", "incoming", or "both"
        depth: Maximum depth to traverse (None for unlimited)
        
    Returns:
        List of relationship dictionaries with traversal information
    """
    visited_entities: Set[str] = set()
    visited_relationships: Set[str] = set()
    results: List[Dict[str, Any]] = []
    
    # BFS queue: (current_entity_id, current_depth)
    queue = deque([(entity_id, 0)])
    visited_entities.add(entity_id)
    
    while queue:
        current_id, current_depth = queue.popleft()
        
        # Check depth limit
        if depth is not None and current_depth >= depth:
            continue
        
        # Get relationships for current entity
        relationships = []
        
        if direction in ["outgoing", "both"]:
            try:
                outgoing = await db.list_relationships_by_entity(
                    entity_id=current_id,
                    direction="source",
                    limit=1000
                )
                relationships.extend(outgoing)
            except AttributeError:
                # Fallback if method doesn't exist
                all_rels = await db.list_relationships(limit=10000)
                outgoing = [r for r in all_rels if r.source_entity_id == current_id]
                relationships.extend(outgoing)
        
        if direction in ["incoming", "both"]:
            try:
                incoming = await db.list_relationships_by_entity(
                    entity_id=current_id,
                    direction="target",
                    limit=1000
                )
                relationships.extend(incoming)
            except AttributeError:
                # Fallback if method doesn't exist
                all_rels = await db.list_relationships(limit=10000)
                incoming = [r for r in all_rels if r.target_entity_id == current_id]
                relationships.extend(incoming)
        
        # Process relationships
        for rel in relationships:
            if rel.id in visited_relationships:
                continue
            
            visited_relationships.add(rel.id)
            
            # Add to results
            results.append({
                "id": rel.id,
                "source_entity_id": rel.source_entity_id,
                "target_entity_id": rel.target_entity_id,
                "type": rel.type,
                "depth": current_depth + 1,
                "start_date": rel.start_date.isoformat() if rel.start_date else None,
                "end_date": rel.end_date.isoformat() if rel.end_date else None,
                "attributes": rel.attributes
            })
            
            # Add next entity to queue
            next_entity_id = None
            if rel.source_entity_id == current_id:
                next_entity_id = rel.target_entity_id
            elif rel.target_entity_id == current_id:
                next_entity_id = rel.source_entity_id
            
            if next_entity_id and next_entity_id not in visited_entities:
                visited_entities.add(next_entity_id)
                queue.append((next_entity_id, current_depth + 1))
    
    return results


async def find_path(
    db: EntityDatabase,
    source_entity_id: str,
    target_entity_id: str,
    max_depth: Optional[int] = None
) -> Optional[List[Dict[str, Any]]]:
    """Find a path between two entities using BFS (shortest path).
    
    Args:
        db: Database instance
        source_entity_id: Starting entity ID
        target_entity_id: Target entity ID
        max_depth: Maximum depth to search (None for unlimited)
        
    Returns:
        List of relationships forming the path, or None if no path exists
    """
    if source_entity_id == target_entity_id:
        return []
    
    # BFS queue: (current_entity_id, path_so_far, current_depth)
    queue = deque([(source_entity_id, [], 0)])
    visited: Set[str] = {source_entity_id}
    
    while queue:
        current_id, path, current_depth = queue.popleft()
        
        # Check depth limit
        if max_depth is not None and current_depth >= max_depth:
            continue
        
        # Get all relationships from current entity
        try:
            relationships = await db.list_relationships_by_entity(
                entity_id=current_id,
                direction="source",
                limit=1000
            )
        except AttributeError:
            # Fallback
            all_rels = await db.list_relationships(limit=10000)
            relationships = [r for r in all_rels if r.source_entity_id == current_id]
        
        for rel in relationships:
            next_id = rel.target_entity_id
            
            # Found target!
            if next_id == target_entity_id:
                return path + [{
                    "id": rel.id,
                    "source_entity_id": rel.source_entity_id,
                    "target_entity_id": rel.target_entity_id,
                    "type": rel.type,
                    "start_date": rel.start_date.isoformat() if rel.start_date else None,
                    "end_date": rel.end_date.isoformat() if rel.end_date else None
                }]
            
            # Continue searching
            if next_id not in visited:
                visited.add(next_id)
                new_path = path + [{
                    "id": rel.id,
                    "source_entity_id": rel.source_entity_id,
                    "target_entity_id": rel.target_entity_id,
                    "type": rel.type,
                    "start_date": rel.start_date.isoformat() if rel.start_date else None,
                    "end_date": rel.end_date.isoformat() if rel.end_date else None
                }]
                queue.append((next_id, new_path, current_depth + 1))
    
    return None


async def generate_graph_visualization(
    db: EntityDatabase,
    entity_id: str,
    format: str = "dot",
    depth: int = 2
) -> str:
    """Generate a graph visualization in the specified format.
    
    Args:
        db: Database instance
        entity_id: Starting entity ID
        format: Output format - "dot", "mermaid", or "json"
        depth: Maximum depth to include in visualization
        
    Returns:
        String representation of the graph in the specified format
    """
    # Get relationships
    relationships = await traverse_relationships(
        db=db,
        entity_id=entity_id,
        direction="both",
        depth=depth
    )
    
    # Collect all entities
    entity_ids: Set[str] = {entity_id}
    for rel in relationships:
        entity_ids.add(rel["source_entity_id"])
        entity_ids.add(rel["target_entity_id"])
    
    # Get entity details
    entities = {}
    for eid in entity_ids:
        entity = await db.get_entity(eid)
        if entity:
            # Get primary name
            primary_name = next(
                (n for n in entity.names if n.kind.value == 'PRIMARY'),
                entity.names[0] if entity.names else None
            )
            
            if primary_name:
                name = primary_name.en.full if primary_name.en else primary_name.ne.full if primary_name.ne else eid
            else:
                name = eid
            
            entities[eid] = {
                "id": eid,
                "name": name,
                "type": entity.type,
                "sub_type": entity.sub_type
            }
    
    # Generate output based on format
    if format == "dot":
        return _generate_dot(entities, relationships)
    elif format == "mermaid":
        return _generate_mermaid(entities, relationships)
    elif format == "json":
        return _generate_json(entities, relationships)
    else:
        raise ValueError(f"Unsupported format: {format}")


def _generate_dot(entities: Dict[str, Dict], relationships: List[Dict]) -> str:
    """Generate Graphviz DOT format."""
    lines = ["digraph G {"]
    lines.append("  rankdir=LR;")
    lines.append("  node [shape=box];")
    lines.append("")
    
    # Add nodes
    for eid, entity in entities.items():
        label = entity["name"].replace('"', '\\"')
        lines.append(f'  "{eid}" [label="{label}"];')
    
    lines.append("")
    
    # Add edges
    for rel in relationships:
        source = rel["source_entity_id"]
        target = rel["target_entity_id"]
        rel_type = rel["type"]
        lines.append(f'  "{source}" -> "{target}" [label="{rel_type}"];')
    
    lines.append("}")
    return "\n".join(lines)


def _generate_mermaid(entities: Dict[str, Dict], relationships: List[Dict]) -> str:
    """Generate Mermaid diagram format."""
    lines = ["graph LR"]
    
    # Create node ID mapping (Mermaid doesn't like colons and slashes)
    node_map = {}
    for i, eid in enumerate(entities.keys()):
        node_map[eid] = f"N{i}"
    
    # Add nodes with labels
    for eid, entity in entities.items():
        node_id = node_map[eid]
        label = entity["name"]
        lines.append(f'  {node_id}["{label}"]')
    
    # Add edges
    for rel in relationships:
        source_id = node_map[rel["source_entity_id"]]
        target_id = node_map[rel["target_entity_id"]]
        rel_type = rel["type"]
        lines.append(f'  {source_id} -->|{rel_type}| {target_id}')
    
    return "\n".join(lines)


def _generate_json(entities: Dict[str, Dict], relationships: List[Dict]) -> str:
    """Generate JSON format."""
    data = {
        "nodes": list(entities.values()),
        "edges": [
            {
                "source": rel["source_entity_id"],
                "target": rel["target_entity_id"],
                "type": rel["type"],
                "start_date": rel.get("start_date"),
                "end_date": rel.get("end_date")
            }
            for rel in relationships
        ]
    }
    return json.dumps(data, indent=2)
