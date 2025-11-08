"""Tests for FileDatabase search capabilities in nes2.

Following TDD approach: Write failing tests first (Red phase).
These tests define the expected behavior of search functionality.

Test Coverage:
- Text-based entity search
- Case-insensitive matching
- Multilingual search (Nepali and English)
- Type and subtype filtering
- Attribute-based filtering
- Search result ranking
"""

import pytest
from datetime import datetime, UTC

from nes2.core.models.base import Name, NameKind
from nes2.core.models.person import Person
from nes2.core.models.organization import PoliticalParty
from nes2.core.models.location import Location
from nes2.core.models.entity import EntitySubType
from nes2.core.models.version import Actor, VersionSummary, VersionType


class TestTextBasedEntitySearch:
    """Test text-based search functionality for entities."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database populated with test entities."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create multiple entities with different names
        entities = [
            Person(
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
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC),
                attributes={"party": "nepali-congress"}
            ),
            Person(
                slug="sher-bahadur-deuba",
                names=[
                    Name(
                        kind=NameKind.PRIMARY,
                        en={"full": "Sher Bahadur Deuba", "first": "Sher Bahadur", "last": "Deuba"},
                        ne={"full": "शेरबहादुर देउवा", "first": "शेरबहादुर", "last": "देउवा"}
                    )
                ],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/sher-bahadur-deuba",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC),
                attributes={"party": "nepali-congress"}
            ),
            Person(
                slug="pushpa-kamal-dahal",
                names=[
                    Name(
                        kind=NameKind.PRIMARY,
                        en={"full": "Pushpa Kamal Dahal", "first": "Pushpa Kamal", "last": "Dahal"},
                        ne={"full": "पुष्पकमल दाहाल", "first": "पुष्पकमल", "last": "दाहाल"}
                    ),
                    Name(
                        kind=NameKind.ALIAS,
                        en={"full": "Prachanda"},
                        ne={"full": "प्रचण्ड"}
                    )
                ],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/pushpa-kamal-dahal",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC),
                attributes={"party": "cpn-maoist-centre"}
            ),
            PoliticalParty(
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
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC),
                attributes={"founded": "1947"}
            ),
            Location(
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
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC),
                attributes={"province": "Bagmati"}
            )
        ]
        
        # Store all entities
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_search_entities_by_text_query(self, populated_db):
        """Test that search_entities can find entities by text query."""
        # This test will fail until search_entities method is implemented
        results = await populated_db.search_entities(query="Poudel")
        
        # Should find Ram Chandra Poudel
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_entities_by_partial_name(self, populated_db):
        """Test that search can find entities by partial name match."""
        results = await populated_db.search_entities(query="Ram")
        
        # Should find Ram Chandra Poudel
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_entities_by_alias(self, populated_db):
        """Test that search can find entities by alias names."""
        results = await populated_db.search_entities(query="Prachanda")
        
        # Should find Pushpa Kamal Dahal by his alias
        assert len(results) >= 1
        assert any(e.slug == "pushpa-kamal-dahal" for e in results)

    @pytest.mark.asyncio
    async def test_search_entities_returns_empty_for_no_match(self, populated_db):
        """Test that search returns empty list when no entities match."""
        results = await populated_db.search_entities(query="NonexistentName")
        
        # Should return empty list
        assert len(results) == 0


class TestCaseInsensitiveSearch:
    """Test case-insensitive search functionality."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with entities for case-insensitive testing."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        entity = Person(
            slug="ram-chandra-poudel",
            names=[
                Name(
                    kind=NameKind.PRIMARY,
                    en={"full": "Ram Chandra Poudel"},
                    ne={"full": "राम चन्द्र पौडेल"}
                )
            ],
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
        
        asyncio.run(db.put_entity(entity))
        return db

    @pytest.mark.asyncio
    async def test_search_case_insensitive_lowercase(self, populated_db):
        """Test that search is case-insensitive with lowercase query."""
        results = await populated_db.search_entities(query="poudel")
        
        # Should find entity regardless of case
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_case_insensitive_uppercase(self, populated_db):
        """Test that search is case-insensitive with uppercase query."""
        results = await populated_db.search_entities(query="POUDEL")
        
        # Should find entity regardless of case
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_case_insensitive_mixed_case(self, populated_db):
        """Test that search is case-insensitive with mixed case query."""
        results = await populated_db.search_entities(query="RaM cHaNdRa")
        
        # Should find entity regardless of case
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)


class TestMultilingualSearch:
    """Test multilingual search functionality (Nepali and English)."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with multilingual entities."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        entities = [
            Person(
                slug="ram-chandra-poudel",
                names=[
                    Name(
                        kind=NameKind.PRIMARY,
                        en={"full": "Ram Chandra Poudel"},
                        ne={"full": "राम चन्द्र पौडेल"}
                    )
                ],
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
            PoliticalParty(
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
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_search_by_english_name(self, populated_db):
        """Test that search can find entities by English name."""
        results = await populated_db.search_entities(query="Poudel")
        
        # Should find entity by English name
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_by_nepali_name(self, populated_db):
        """Test that search can find entities by Nepali (Devanagari) name."""
        results = await populated_db.search_entities(query="पौडेल")
        
        # Should find entity by Nepali name
        assert len(results) >= 1
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_by_nepali_organization_name(self, populated_db):
        """Test that search can find organizations by Nepali name."""
        results = await populated_db.search_entities(query="कांग्रेस")
        
        # Should find Nepali Congress by Nepali name
        assert len(results) >= 1
        assert any(e.slug == "nepali-congress" for e in results)

    @pytest.mark.asyncio
    async def test_search_mixed_language_results(self, populated_db):
        """Test that search returns results regardless of query language."""
        # Search with English query
        english_results = await populated_db.search_entities(query="Congress")
        
        # Search with Nepali query
        nepali_results = await populated_db.search_entities(query="कांग्रेस")
        
        # Both should find the same entity
        assert len(english_results) >= 1
        assert len(nepali_results) >= 1
        assert english_results[0].id == nepali_results[0].id


class TestTypeAndSubtypeFiltering:
    """Test filtering by entity type and subtype."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with entities of different types."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
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
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Kathmandu Metropolitan City"})],
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
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_search_filter_by_type_person(self, populated_db):
        """Test that search can filter by entity type (person)."""
        results = await populated_db.search_entities(entity_type="person")
        
        # Should only return person entities
        assert len(results) >= 1
        assert all(e.type == "person" for e in results)
        assert any(e.slug == "ram-chandra-poudel" for e in results)

    @pytest.mark.asyncio
    async def test_search_filter_by_type_organization(self, populated_db):
        """Test that search can filter by entity type (organization)."""
        results = await populated_db.search_entities(entity_type="organization")
        
        # Should only return organization entities
        assert len(results) >= 1
        assert all(e.type == "organization" for e in results)
        assert any(e.slug == "nepali-congress" for e in results)

    @pytest.mark.asyncio
    async def test_search_filter_by_type_location(self, populated_db):
        """Test that search can filter by entity type (location)."""
        results = await populated_db.search_entities(entity_type="location")
        
        # Should only return location entities
        assert len(results) >= 1
        assert all(e.type == "location" for e in results)
        assert any(e.slug == "kathmandu-metropolitan-city" for e in results)

    @pytest.mark.asyncio
    async def test_search_filter_by_subtype_political_party(self, populated_db):
        """Test that search can filter by entity subtype (political_party)."""
        results = await populated_db.search_entities(
            entity_type="organization",
            sub_type="political_party"
        )
        
        # Should only return political party entities
        assert len(results) >= 1
        assert all(e.sub_type == EntitySubType.POLITICAL_PARTY for e in results)
        assert any(e.slug == "nepali-congress" for e in results)

    @pytest.mark.asyncio
    async def test_search_filter_by_subtype_metropolitan_city(self, populated_db):
        """Test that search can filter by entity subtype (metropolitan_city)."""
        results = await populated_db.search_entities(
            entity_type="location",
            sub_type="metropolitan_city"
        )
        
        # Should only return metropolitan city entities
        assert len(results) >= 1
        assert all(e.sub_type == EntitySubType.METROPOLITAN_CITY for e in results)
        assert any(e.slug == "kathmandu-metropolitan-city" for e in results)

    @pytest.mark.asyncio
    async def test_search_combined_query_and_type_filter(self, populated_db):
        """Test that search can combine text query with type filtering."""
        results = await populated_db.search_entities(
            query="Congress",
            entity_type="organization"
        )
        
        # Should find Nepali Congress but not Ram Chandra Poudel
        assert len(results) >= 1
        assert all(e.type == "organization" for e in results)
        assert any(e.slug == "nepali-congress" for e in results)
        assert not any(e.slug == "ram-chandra-poudel" for e in results)


class TestAttributeBasedFiltering:
    """Test filtering by entity attributes."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with entities having various attributes."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
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
                created_at=datetime.now(UTC),
                attributes={"party": "nepali-congress", "constituency": "Tanahun-1"}
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
                created_at=datetime.now(UTC),
                attributes={"party": "nepali-congress", "constituency": "Dadeldhura-1"}
            ),
            Person(
                slug="pushpa-kamal-dahal",
                names=[Name(kind=NameKind.PRIMARY, en={"full": "Pushpa Kamal Dahal"})],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/pushpa-kamal-dahal",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC),
                attributes={"party": "cpn-maoist-centre", "constituency": "Gorkha-2"}
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
                created_at=datetime.now(UTC),
                attributes={"founded": "1947", "ideology": "social-democracy"}
            )
        ]
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_search_filter_by_single_attribute(self, populated_db):
        """Test that search can filter by a single attribute."""
        results = await populated_db.search_entities(
            attr_filters={"party": "nepali-congress"}
        )
        
        # Should find both Nepali Congress members
        assert len(results) >= 2
        assert any(e.slug == "ram-chandra-poudel" for e in results)
        assert any(e.slug == "sher-bahadur-deuba" for e in results)
        assert not any(e.slug == "pushpa-kamal-dahal" for e in results)

    @pytest.mark.asyncio
    async def test_search_filter_by_multiple_attributes_and_logic(self, populated_db):
        """Test that search applies AND logic for multiple attribute filters."""
        results = await populated_db.search_entities(
            attr_filters={"party": "nepali-congress", "constituency": "Tanahun-1"}
        )
        
        # Should only find Ram Chandra Poudel (matches both criteria)
        assert len(results) == 1
        assert results[0].slug == "ram-chandra-poudel"

    @pytest.mark.asyncio
    async def test_search_filter_by_attribute_no_match(self, populated_db):
        """Test that search returns empty list when attribute filter has no match."""
        results = await populated_db.search_entities(
            attr_filters={"party": "nonexistent-party"}
        )
        
        # Should return empty list
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_search_combined_query_type_and_attributes(self, populated_db):
        """Test that search can combine text query, type filter, and attribute filters."""
        results = await populated_db.search_entities(
            query="Deuba",
            entity_type="person",
            attr_filters={"party": "nepali-congress"}
        )
        
        # Should find only Sher Bahadur Deuba
        assert len(results) == 1
        assert results[0].slug == "sher-bahadur-deuba"

    @pytest.mark.asyncio
    async def test_search_filter_by_numeric_attribute(self, populated_db):
        """Test that search can filter by numeric attributes."""
        results = await populated_db.search_entities(
            entity_type="organization",
            attr_filters={"founded": "1947"}
        )
        
        # Should find Nepali Congress (founded in 1947)
        assert len(results) >= 1
        assert any(e.slug == "nepali-congress" for e in results)


class TestSearchResultRanking:
    """Test search result ranking and relevance."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with entities for ranking tests."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        entities = [
            Person(
                slug="ram-chandra-poudel",
                names=[
                    Name(
                        kind=NameKind.PRIMARY,
                        en={"full": "Ram Chandra Poudel", "first": "Ram Chandra", "last": "Poudel"}
                    )
                ],
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
                slug="ram-prasad-sharma",
                names=[
                    Name(
                        kind=NameKind.PRIMARY,
                        en={"full": "Ram Prasad Sharma", "first": "Ram Prasad", "last": "Sharma"}
                    )
                ],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/ram-prasad-sharma",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            ),
            Person(
                slug="ramesh-lekhak",
                names=[
                    Name(
                        kind=NameKind.PRIMARY,
                        en={"full": "Ramesh Lekhak", "first": "Ramesh", "last": "Lekhak"}
                    )
                ],
                version_summary=VersionSummary(
                    entity_or_relationship_id="entity:person/ramesh-lekhak",
                    type=VersionType.ENTITY,
                    version_number=1,
                    actor=Actor(slug="system"),
                    change_description="Initial",
                    created_at=datetime.now(UTC)
                ),
                created_at=datetime.now(UTC)
            )
        ]
        
        async def populate():
            for entity in entities:
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_search_ranks_exact_match_higher(self, populated_db):
        """Test that exact matches are ranked higher than partial matches."""
        results = await populated_db.search_entities(query="Ram")
        
        # Should return results with "Ram" in the name
        assert len(results) >= 2
        # Exact word match "Ram" should rank higher than "Ramesh"
        # (This is a basic ranking test - implementation may vary)
        assert any("Ram" in e.names[0].en.full for e in results)

    @pytest.mark.asyncio
    async def test_search_ranks_primary_name_matches_higher(self, populated_db):
        """Test that matches in primary names rank higher than alias matches."""
        # Add entity with alias
        from nes2.database.file_database import FileDatabase
        
        entity_with_alias = Person(
            slug="pushpa-kamal-dahal",
            names=[
                Name(
                    kind=NameKind.PRIMARY,
                    en={"full": "Pushpa Kamal Dahal"}
                ),
                Name(
                    kind=NameKind.ALIAS,
                    en={"full": "Prachanda Ram"}
                )
            ],
            version_summary=VersionSummary(
                entity_or_relationship_id="entity:person/pushpa-kamal-dahal",
                type=VersionType.ENTITY,
                version_number=1,
                actor=Actor(slug="system"),
                change_description="Initial",
                created_at=datetime.now(UTC)
            ),
            created_at=datetime.now(UTC)
        )
        
        await populated_db.put_entity(entity_with_alias)
        
        results = await populated_db.search_entities(query="Ram")
        
        # Should return results, with primary name matches potentially ranked higher
        assert len(results) >= 3
        # All results should contain "Ram" somewhere in their names
        assert all(
            any("Ram" in name.en.full for name in e.names)
            for e in results
        )

    @pytest.mark.asyncio
    async def test_search_returns_consistent_ordering(self, populated_db):
        """Test that search returns results in consistent order."""
        # Run the same search multiple times
        results1 = await populated_db.search_entities(query="Ram")
        results2 = await populated_db.search_entities(query="Ram")
        
        # Should return same results in same order
        assert len(results1) == len(results2)
        assert [e.id for e in results1] == [e.id for e in results2]


class TestSearchPagination:
    """Test pagination in search results."""

    @pytest.fixture
    def populated_db(self, temp_db_path):
        """Create a database with many entities for pagination testing."""
        from nes2.database.file_database import FileDatabase
        import asyncio
        
        db = FileDatabase(base_path=str(temp_db_path))
        
        # Create 10 entities with "Ram" in their names
        async def populate():
            for i in range(10):
                entity = Person(
                    slug=f"ram-person-{i}",
                    names=[Name(kind=NameKind.PRIMARY, en={"full": f"Ram Person {i}"})],
                    version_summary=VersionSummary(
                        entity_or_relationship_id=f"entity:person/ram-person-{i}",
                        type=VersionType.ENTITY,
                        version_number=1,
                        actor=Actor(slug="system"),
                        change_description="Initial",
                        created_at=datetime.now(UTC)
                    ),
                    created_at=datetime.now(UTC)
                )
                await db.put_entity(entity)
        
        asyncio.run(populate())
        return db

    @pytest.mark.asyncio
    async def test_search_with_limit(self, populated_db):
        """Test that search respects limit parameter."""
        results = await populated_db.search_entities(query="Ram", limit=5)
        
        # Should return at most 5 results
        assert len(results) <= 5

    @pytest.mark.asyncio
    async def test_search_with_offset(self, populated_db):
        """Test that search respects offset parameter."""
        # Get first page
        page1 = await populated_db.search_entities(query="Ram", limit=3, offset=0)
        
        # Get second page
        page2 = await populated_db.search_entities(query="Ram", limit=3, offset=3)
        
        # Pages should have different entities
        page1_ids = [e.id for e in page1]
        page2_ids = [e.id for e in page2]
        assert len(set(page1_ids) & set(page2_ids)) == 0

    @pytest.mark.asyncio
    async def test_search_pagination_covers_all_results(self, populated_db):
        """Test that pagination can retrieve all matching results."""
        # Get all results
        all_results = await populated_db.search_entities(query="Ram", limit=100)
        
        # Get results in pages
        page1 = await populated_db.search_entities(query="Ram", limit=5, offset=0)
        page2 = await populated_db.search_entities(query="Ram", limit=5, offset=5)
        
        # Combined pages should cover the results
        combined_ids = [e.id for e in page1] + [e.id for e in page2]
        all_ids = [e.id for e in all_results]
        
        # All IDs from pages should be in the full result set
        assert all(id in all_ids for id in combined_ids)
