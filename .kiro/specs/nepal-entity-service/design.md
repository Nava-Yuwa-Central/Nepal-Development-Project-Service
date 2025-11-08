# Design Document

## Overview

The Nepal Entity Service is a comprehensive system designed to manage Nepali public entities with a focus on transparency, accountability, and data integrity. The system provides a robust foundation for civic technology applications by offering structured entity management, versioning, relationship tracking, and automated data collection capabilities.

The architecture follows a modular design with optional components, allowing users to install only the functionality they need. The core system provides entity models and utilities, while optional modules add API services and scraping capabilities.

## Architecture

### High-Level Architecture

```mermaid
graph TB
    subgraph "Client Applications"
        WEB[Web Applications]
        CLI[CLI Tools]
        NOTEBOOK[Notebooks]
    end
    
    subgraph "API Layer"
        API[FastAPI Service]
    end
    
    subgraph "Data Management layer"
        PUB[Publication Service]
        SRCH[Search Service]

        subgraph "Modules"
            ENT[Entity Module]
            REL[Relationship Module]
            VER[Version Module]
            ACT[Author Module]
        end

        PUB --> ENT
        PUB --> REL
        PUB --> VER
        PUB --> ACT
    
        SRCH --> ENT
        SRCH --> REL
    end

    SCRP[Scraping Service]

    subgraph "GenAI and LLM"
    WEB_SCRP[Web Scraper]
    TRANS[Translation Service]
    NORM[Data Normalization Service]
    end
    
    subgraph "Data Layer"
        EDB[Entity Database]
        FDB[File Database]
        JSON[JSON Files]
    end
    
    subgraph "External Sources"
        WIKI[Wikipedia]
        GOV[Government Sites]
        NEWS[News]
        OTHER[Other Sources]
    end

    SCRP --> WEB_SCRP
    SCRP --> TRANS
    SCRP --> NORM


    NOTEBOOK --> SCRP
    CLI --> SCRP
    NOTEBOOK --> PUB
    NOTEBOOK --> SRCH
    
    WEB --> API
    CLI --> SRCH
    
    API --> PUB
    API --> SRCH
    
    PUB --> ENT
    PUB --> REL
    PUB --> VER
    
    SRCH --> EDB
    
    ENT --> EDB
    REL --> EDB
    VER --> EDB
    
    EDB --> FDB
    FDB --> JSON
    
    WEB_SCRP --> WIKI
    WEB_SCRP --> GOV
    WEB_SCRP --> NEWS
```

### Component Architecture

The system is organized into several key components:

1. **Core Models**: Pydantic-based data models for entities, relationships, and versions
2. **Data Management Layer**: Publication and Search services with shared modules
   - **Publication Service**: Entity lifecycle management with Entity, Relationship, Version, and Author modules
   - **Search Service**: Entity and relationship search with filtering and pagination
3. **Scraping Service**: External data extraction with GenAI/LLM components
   - **Web Scraper**: Multi-source data extraction
   - **Translation Service**: Nepali/English translation
   - **Data Normalization Service**: LLM-powered data structuring
4. **Database Layer**: Abstract database interface with file-based implementation  
5. **API Layer**: FastAPI-based REST service for data retrieval with hosted documentation
6. **Client Applications**: Web apps, CLI tools, and Jupyter notebooks with direct Publication Service access
7. **Data Maintainer Interface**: Pythonic interface for local data maintenance

## Components and Interfaces

### Publication Service

The **Publication Service** is the central orchestration layer that manages the complete lifecycle of entities, relationships, and their versions. Rather than having separate services for each concern, the Publication Service provides a unified interface with specialized internal modules.

#### Architecture Philosophy

The Publication Service follows a modular monolith pattern:
- **Single Entry Point**: All entity operations flow through the Publication Service
- **Internal Modules**: Entity, Relationship, and Version modules handle specialized logic
- **Coordinated Operations**: Cross-cutting concerns (validation, versioning, attribution) are handled consistently
- **Transaction Boundaries**: The service manages atomic operations across multiple modules

#### Core Responsibilities

1. **Entity Lifecycle Management**
   - Entity creation, updates, and retrieval through the Entity Module
   - Automatic version creation on all modifications
   - Validation and constraint enforcement
   - Attribution tracking for all changes

2. **Relationship Management**
   - Relationship creation and modification through the Relationship Module
   - Bidirectional relationship consistency
   - Temporal relationship tracking
   - Relationship versioning

3. **Version Control**
   - Automatic snapshot creation through the Version Module
   - Historical state retrieval
   - Change tracking and audit trails
   - Author attribution

4. **Cross-Cutting Concerns**
   - Unified validation across all operations
   - Consistent error handling
   - Transaction management
   - Event publishing for external integrations

#### Module Structure

The Publication Service uses shared modules within the Data Management Layer:

**Entity Module**
- Entity CRUD operations
- Name and identifier management
- Entity-specific validation
- Used by both Publication and Search services

**Relationship Module**
- Relationship CRUD operations
- Relationship type validation
- Temporal relationship handling
- Bidirectional consistency checks
- Used by both Publication and Search services

**Version Module**
- Snapshot creation and storage
- Version retrieval and comparison
- Change description management
- Attribution tracking
- Used exclusively by Publication Service

**Author Module**
- Author management and tracking
- Attribution metadata
- Author validation
- Used by Publication Service for change tracking

#### Service Interface Example

```python
from nes.services import PublicationService
from nes.core.models import Entity, Relationship

# Initialize the publication service
pub_service = PublicationService(database=db)

# Entity operations (uses Entity Module internally)
entity = pub_service.create_entity(
    entity_data=entity_dict,
    author_id="author:system:csv-importer",
    change_description="Initial import"
)

# Relationship operations (uses Relationship Module internally)
relationship = pub_service.create_relationship(
    source_id="entity:person:politician:ram-chandra-poudel",
    target_id="entity:organization:political_party:nepali-congress",
    relationship_type="MEMBER_OF",
    author_id="author:system:csv-importer"
)

# Version operations (uses Version Module internally)
versions = pub_service.get_entity_versions(
    entity_id="entity:person:politician:ram-chandra-poudel"
)

# Coordinated operations across modules
pub_service.update_entity_with_relationships(
    entity=updated_entity,
    new_relationships=[rel1, rel2],
    author_id="author:system:csv-importer"
)
```

### Search Service

The **Search Service** provides basic search capabilities for entities and relationships with support for filtering, attribute-based queries, and pagination.

#### Architecture Philosophy

The Search Service is designed as a simple, read-optimized service:
- **Separate from Publication Service**: Search is a distinct concern focused on data retrieval
- **Database-Backed**: Queries the database directly without complex indexing
- **Simple Filtering**: Basic type, subtype, and attribute filtering
- **Pagination Support**: Efficient result pagination for large datasets

#### Core Responsibilities

1. **Entity Search**
   - Basic text search across entity names (Nepali and English)
   - Type and subtype filtering
   - Attribute-based filtering
   - Pagination support

2. **Relationship Search**
   - Find relationships by type
   - Filter by source or target entity
   - Basic attribute filtering
   - Pagination support

#### Service Interface Example

```python
from nes.services import SearchService
from nes.database import FileDatabase

# Initialize search service
db = FileDatabase(base_path="./nes-db/v2")
search_service = SearchService(database=db)

# Basic entity search with text query
results = search_service.search_entities(
    query="poudel",
    limit=10,
    offset=0
)

# Entity search with filters
results = search_service.search_entities(
    query="ram",
    entity_type="person",
    sub_type="politician",
    limit=20,
    offset=0
)

# Attribute-based filtering
results = search_service.search_entities(
    attributes={"party": "nepali-congress"},
    entity_type="person",
    limit=10,
    offset=0
)

# Relationship search
relationships = search_service.search_relationships(
    relationship_type="MEMBER_OF",
    target_entity_id="entity:organization:political_party:nepali-congress",
    limit=10,
    offset=0
)

# Paginated results
page_1 = search_service.search_entities(query="politician", limit=20, offset=0)
page_2 = search_service.search_entities(query="politician", limit=20, offset=20)
```

#### Search Capabilities

**Text Search**
- Case-insensitive search across entity name fields
- Supports both Nepali (Devanagari) and English text
- Simple substring matching

**Filter Options**
- Entity type filtering (person, organization, location)
- Subtype filtering (politician, political_party, etc.)
- Attribute key-value filtering
- Identifier scheme filtering

**Pagination**
- Limit: Maximum number of results to return
- Offset: Number of results to skip
- Total count: Total matching results for pagination UI

### Core Models

#### Entity Model
The `Entity` model serves as the foundation for all entity types:

- **Identification**: Unique slug-based IDs with computed full identifiers
- **Typing**: Hierarchical type system (type + subtype)
- **Naming**: Multilingual name support with primary/alias classifications
- **Metadata**: Versioning, timestamps, attributions, and external identifiers
- **Extensibility**: Flexible attributes system for domain-specific data

#### Relationship Model
The `Relationship` model manages connections between entities:

- **Bidirectional**: Source and target entity references
- **Typed**: Predefined relationship types (AFFILIATED_WITH, MEMBER_OF, etc.)
- **Temporal**: Optional start and end dates for time-bound relationships
- **Attributed**: Custom attributes for relationship-specific metadata

#### Version Model
The `Version` model provides comprehensive audit trails:

- **Snapshots**: Complete entity/relationship state preservation
- **Attribution**: Author tracking for all modifications
- **Timestamps**: Creation and modification tracking
- **Metadata**: Change descriptions and source information

### Database Interface

#### EntityDatabase Abstract Class
Provides standardized CRUD operations for:

- **Entities**: Create, read, update, delete, and list operations
- **Relationships**: Full relationship lifecycle management
- **Versions**: Version creation, retrieval, and listing
- **Authors**: Author management for attribution tracking

#### File Database Implementation
File-based storage system with:

- **JSON Storage**: Human-readable entity files
- **Directory Structure**: Organized by entity type and subtype at `nes-db/v2`
- **Atomic Operations**: Safe concurrent access patterns
- **Backup Support**: Version history preservation
- **Database Location**: All entity data stored in `nes-db/v2` directory

#### Data Maintainer Interface
Pythonic interface for local data maintenance operations:

- **Local Python Access**: Direct Python API for maintainers to use in local scripts and notebooks
- **No Authentication Required**: Designed for trusted local environment use without authentication overhead
- **Entity Updates**: Simplified entity modification with automatic versioning
- **Relationship Management**: Easy relationship creation, modification, and deletion
- **Schema Validation**: Real-time data validation feedback during write operations
- **Batch Operations**: Bulk update capabilities for large-scale data maintenance
- **Change Tracking**: Automatic attribution and change description capture

### API Service

#### REST Endpoints

All API endpoints are served under the `/api` prefix, with documentation served at the root:

**API Endpoints (`/api/*`)**
- **Entity Retrieval**: `/api/entities` - Read operations with filtering and pagination
- **Entity Details**: `/api/entities/{entity_id}` - Get specific entity by ID
- **Version Access**: `/api/entities/{entity_id}/versions` - Historical entity state retrieval
- **Relationship Queries**: `/api/relationships` - Entity connection exploration
- **Search Endpoints**: `/api/search` - Basic text and filtered search via Search Service
- **Health Check**: `/api/health` - System health and readiness status

**Documentation Endpoints**
- **Root**: `/` - Main documentation landing page
- **Documentation Pages**: `/{page}` - Individual documentation pages from Markdown
- **API Schema**: `/docs` - OpenAPI/Swagger schema documentation

Note: Write operations are handled through the Publication Service in notebooks and scripts, not through the API.

#### Documentation Hosting

The API service hosts comprehensive documentation from the root endpoint, providing a unified documentation experience:

**Root Documentation Portal (`/`)**
- **Endpoint**: `/` serves the main documentation landing page
- **Format**: Static HTML generated from Markdown files
- **Content**: Architecture overview, usage guides, API reference, examples
- **Navigation**: Simple navigation between documentation sections
- **Public Access**: No authentication required, fully public documentation

**API Schema Documentation (`/docs`)**
- **Endpoint**: `/docs` provides OpenAPI/Swagger schema documentation
- **Auto-generated**: FastAPI automatically generates OpenAPI 3.0 specification
- **Schema Exploration**: Complete data model documentation with request/response examples
- **Read-Only**: Documentation for read-only public API endpoints

**Documentation Structure**
```
docs/
├── index.md              # Landing page (served at /)
├── getting-started.md    # Quick start guide
├── architecture.md       # System architecture
├── api-reference.md      # API endpoint documentation
├── data-models.md        # Entity, Relationship, Version schemas
├── examples.md           # Usage examples
└── contributing.md       # Contribution guidelines
```

**Documentation Build and Serving**
```python
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import markdown

app = FastAPI()

# Serve API endpoints under /api prefix
app.include_router(api_router, prefix="/api")

# Serve OpenAPI docs at /docs
# (FastAPI default, no configuration needed)

# Serve markdown documentation at root
@app.get("/")
async def root():
    """Serve the main documentation landing page."""
    with open("docs/index.md", "r") as f:
        content = markdown.markdown(f.read())
    return HTMLResponse(content=render_template(content))

# Serve other documentation pages
@app.get("/{page}")
async def documentation_page(page: str):
    """Serve documentation pages from markdown files."""
    try:
        with open(f"docs/{page}.md", "r") as f:
            content = markdown.markdown(f.read())
        return HTMLResponse(content=render_template(content))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Page not found")
```

**Documentation Features**
- **Markdown-Based**: All documentation written in simple Markdown files
- **Version Control**: Documentation versioned alongside code in Git
- **Easy Updates**: Non-technical contributors can update documentation via Markdown
- **No Build Step**: Markdown rendered on-the-fly by the API service
- **Consistent Styling**: Simple HTML template for consistent look and feel

#### Middleware Stack
- **CORS Support**: Cross-origin request handling for web applications
- **Error Handling**: Standardized error responses with detailed messages
- **Request Validation**: Automatic validation of input params using Pydantic
- **Response Formatting**: Consistent JSON response structure

### Scraping Service

The **Scraping Service** is a standalone service responsible for extracting and normalizing data from external sources using GenAI and LLM capabilities. It does not directly access the database but returns normalized data for client applications to process.

#### Architecture Philosophy

The Scraping Service is designed as a data extraction and transformation layer:
- **Source Agnostic**: Pluggable extractors for different data sources
- **No Database Access**: Returns normalized data without persisting it
- **GenAI/LLM Integration**: Uses language models for intelligent data extraction and normalization
- **Reusable**: Can be used by CLI, notebooks, and import scripts

**Note**: While the Scraping Service does not directly access entities in the database, it may accept structured or unstructured inputs from users or existing entities (e.g., entity IDs, names, or attributes) to guide data extraction and normalization. The service produces normalized data that can be reviewed and imported by client applications.

#### Core Components

**Web Scraper**
- Multi-source data extraction (Wikipedia, government sites, news)
- Rate limiting and respectful scraping
- Error handling and retry logic
- HTML parsing and content extraction

**Translation Service**
- Nepali to English translation
- English to Nepali translation
- Transliteration handling
- Language detection

**Data Normalization Service**
- LLM-powered data structuring
- Extract structured data from unstructured text
- Relationship discovery from narrative text
- Name disambiguation and standardization
- Data quality assessment

#### Supported Data Sources

- **Wikipedia**: Politician profiles, organization pages, infoboxes
- **Government Sites**: Election Commission data, ministry registrations
- **News**: Public statements, event coverage, biographical information
- **Other Sources**: Social media, official announcements

#### Service Interface Example

```python
from nes.services import ScrapingService

# Initialize scraping service
scraping_service = ScrapingService()

# Extract entity data from Wikipedia
raw_data = scraping_service.extract_from_wikipedia(
    page_title="Ram_Chandra_Poudel",
    language="en"
)

# Normalize to entity model (doesn't save to database)
normalized_entity = scraping_service.normalize_person_data(
    raw_data=raw_data,
    source="wikipedia"
)

# Extract relationships from text using LLM
relationships = scraping_service.extract_relationships(
    text="Ram Chandra Poudel is a member of Nepali Congress",
    entity_id="entity:person:politician:ram-chandra-poudel"
)

# Translate Nepali text to English
translated = scraping_service.translate(
    text="राम चन्द्र पौडेल",
    source_lang="ne",
    target_lang="en"
)
```

### CLI Tools

The CLI provides command-line access to the system, using Search Service for database queries and Scraping Service for external data discovery. The CLI is built using Python Click, a composable command-line interface toolkit that provides elegant argument parsing, help generation, and command grouping.

#### CLI Capabilities

- **Entity Search**: Search and filter entities from the database
- **Entity Display**: View detailed entity information
- **Relationship Exploration**: Browse entity relationships
- **External Search**: Search external sources using Scraping Service
- **Data Export**: Export search results to various formats

#### CLI Integration Example

```python
from nes.services import SearchService, ScrapingService
from nes.database import FileDatabase

# CLI command: nes search "ram poudel"
def cli_search(query: str, entity_type: str = None):
    db = FileDatabase(base_path="./nes-db/v2")
    search_service = SearchService(database=db)
    
    results = search_service.search_entities(
        query=query,
        entity_type=entity_type,
        limit=10,
        offset=0
    )
    
    for entity in results.results:
        print(f"{entity.id}: {entity.names[0].en.full}")
    
    print(f"\nTotal results: {results.total}")

# CLI command: nes scrape-info "ram poudel"
def cli_scrape_info(query: str):
    scraping_service = ScrapingService()
    
    # Search external sources
    results = scraping_service.search_external_sources(
        query=query,
        sources=["wikipedia"]
    )
    
    for result in results:
        print(f"Source: {result.source}")
        print(f"Title: {result.title}")
        print(f"URL: {result.url}")
        print(f"Summary: {result.summary}\n")
```

### Notebook Applications

Jupyter notebooks serve as interactive data import and maintenance tools, orchestrating Publication, Search, and Scraping services.

#### Architecture Philosophy

- **Interactive Exploration**: Exploratory data import and analysis
- **Service Orchestration**: Combine Scraping, Search, and Publication services
- **Human-in-the-Loop**: Manual review and approval of scraped data
- **Iterative Development**: Test and refine data import workflows

#### Typical Notebook Workflow

```python
from nes.services import PublicationService, SearchService, ScrapingService
from nes.database import FileDatabase

# Initialize all services
db = FileDatabase(base_path="./nes-db/v2")
pub_service = PublicationService(database=db)
search_service = SearchService(database=db)
scraping_service = ScrapingService()

# Notebook workflow: Import politician from Wikipedia
def import_politician_from_wikipedia(wikipedia_page: str):
    # 1. Scrape data from Wikipedia using Scraping Service
    raw_data = scraping_service.extract_from_wikipedia(
        page_title=wikipedia_page,
        language="en"
    )
    
    # 2. Normalize to entity model
    normalized = scraping_service.normalize_person_data(
        raw_data=raw_data,
        source="wikipedia"
    )
    
    # 3. Check for duplicates using Search Service
    existing = search_service.search_entities(
        query=normalized["names"][0]["en"]["full"],
        entity_type="person",
        sub_type="politician",
        limit=5
    )
    
    # 4. Review and decide (human-in-the-loop)
    if existing.total > 0:
        print(f"Found {existing.total} potential duplicates:")
        for e in existing.results:
            print(f"  - {e.id}: {e.names[0].en.full}")
        
        # Manual decision: update or create new
        should_update = input("Update existing? (y/n): ")
        
        if should_update.lower() == 'y':
            entity_id = input("Enter entity ID to update: ")
            entity = pub_service.get_entity(entity_id)
            # Merge and update
            pub_service.update_entity(
                entity=entity,
                author_id="author:human:data-maintainer",
                change_description=f"Updated from Wikipedia: {wikipedia_page}"
            )
        else:
            # Create new entity
            entity = pub_service.create_entity(
                entity_data=normalized,
                author_id="author:human:data-maintainer",
                change_description=f"Imported from Wikipedia: {wikipedia_page}"
            )
    else:
        # No duplicates, create new
        entity = pub_service.create_entity(
            entity_data=normalized,
            author_id="author:human:data-maintainer",
            change_description=f"Imported from Wikipedia: {wikipedia_page}"
        )
    
    return entity

# Use in notebook
politician = import_politician_from_wikipedia("Ram_Chandra_Poudel")
print(f"Imported: {politician.id}")
```

#### Notebook Use Cases

- **Interactive Data Import**: Import entities with manual review
- **Data Quality Analysis**: Analyze and fix data quality issues
- **Experimental Workflows**: Test new scraping and normalization approaches
- **Bulk Operations**: Process multiple entities with human oversight
- **Data Exploration**: Explore relationships and entity connections

## Data Models

### Entity Schema

```json
{
  "slug": "string (required, 3-50 chars, kebab-case)",
  "type": "person|organization|location",
  "sub_type": "political_party|government_body|province|district|...",
  "names": [
    {
      "kind": "PRIMARY|ALIAS|ALTERNATE|BIRTH|OFFICIAL",
      "en": {
        "full": "string",
        "first": "string?",
        "middle": "string?",
        "last": "string?",
        "prefix": "string?",
        "suffix": "string?"
      },
      "ne": { /* same structure */ }
    }
  ],
  "version_summary": {
    "version": "integer",
    "created_at": "datetime",
    "created_by": "string"
  },
  "identifiers": [
    {
      "scheme": "wikipedia|wikidata|twitter|...",
      "value": "string",
      "url": "string?"
    }
  ],
  "attributes": { /* flexible key-value pairs */ },
  "contacts": [
    {
      "type": "EMAIL|PHONE|URL|...",
      "value": "string"
    }
  ],
  "descriptions": {
    "en": { "value": "string", "provenance": "human|llm|..." },
    "ne": { "value": "string", "provenance": "human|llm|..." }
  }
}
```

### Relationship Schema

```json
{
  "source_entity_id": "entity:type:subtype:slug",
  "target_entity_id": "entity:type:subtype:slug",
  "type": "AFFILIATED_WITH|EMPLOYED_BY|MEMBER_OF|...",
  "start_date": "date?",
  "end_date": "date?",
  "attributes": { /* relationship-specific data */ },
  "version_summary": { /* version metadata */ },
  "attributions": ["source1", "source2"]
}
```

### Version Schema

```json
{
  "entity_id": "string",
  "version": "integer",
  "snapshot": { /* complete entity/relationship state */ },
  "created_at": "datetime",
  "created_by": "string",
  "change_description": "string?",
  "attribution": { /* source information */ }
}
```

### Data Maintainer Interface

The Data Maintainer Interface is a Pythonic API designed for local use by trusted maintainers. It provides a clean, intuitive interface for data maintenance operations without requiring authentication.

#### Python API Example

```python
from nes.services import PublicationService
from nes.database import FileDatabase
from nes.core.models import Entity, Relationship

# Initialize database and publication service
db = FileDatabase(base_path="./nes-db/v2")
pub_service = PublicationService(database=db)

# Update an entity (automatically creates version)
entity = pub_service.get_entity("entity:person:politician:ram-chandra-poudel")
entity.names[0].en.full = "Ram Chandra Poudel"
pub_service.update_entity(
    entity=entity,
    author_id="author:system:csv-importer",
    change_description="Updated name spelling"
)

# Create a relationship (automatically creates version)
relationship = pub_service.create_relationship(
    source_entity_id="entity:person:politician:ram-chandra-poudel",
    target_entity_id="entity:organization:political_party:nepali-congress",
    relationship_type="MEMBER_OF",
    start_date="2000-01-01",
    author_id="author:system:csv-importer"
)

# Batch operations through publication service
entities = pub_service.list_entities(entity_type="person", sub_type="politician")
for entity in entities:
    # Process and update entities with automatic versioning
    pub_service.update_entity(
        entity, 
        author_id="author:system:batch-processor",
        change_description="Batch update"
    )

# Get version history
versions = pub_service.get_entity_versions(
    entity_id="entity:person:politician:ram-chandra-poudel"
)
```

#### Interface Characteristics

- **No Authentication**: Operates directly on local file system without authentication checks
- **Publication Service Layer**: Uses PublicationService for coordinated operations across modules
- **Automatic Versioning**: All updates automatically create version snapshots through Version Module
- **Author Attribution**: Requires author_id for change tracking but no user authentication
- **Validation**: Full Pydantic validation on all operations coordinated by Publication Service
- **Transaction Safety**: Atomic operations managed by Publication Service
- **Module Coordination**: Entity, Relationship, and Version modules work together seamlessly

## Error Handling

### Validation Errors
- **Schema Validation**: Pydantic model validation with field-level error details
- **Business Rules**: Custom validation for entity-specific constraints
- **Reference Integrity**: Entity ID validation and relationship consistency checks
- **Data Quality**: Name requirements, identifier format validation

### API Error Responses
- **400 Bad Request**: Invalid input data with detailed field errors
- **404 Not Found**: Entity, relationship, or version not found
- **409 Conflict**: Duplicate entity creation attempts
- **422 Unprocessable Entity**: Valid JSON but invalid business logic
- **500 Internal Server Error**: Database or system errors

### Error Response Format

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Entity validation failed",
    "details": [
      {
        "field": "names",
        "message": "At least one name with kind='PRIMARY' is required"
      }
    ]
  }
}
```

## Testing Strategy

### Test-Driven Development (TDD)
The project follows the Red-Green-Refactor cycle:

- **Red Phase**: Write failing tests first that define the expected behavior
- **Green Phase**: Write minimal code to make tests pass, focusing on functionality over elegance
- **Refactor Phase**: Improve code quality, performance, and maintainability while keeping tests green

### Unit Testing
- **Model Validation**: Comprehensive Pydantic model testing with TDD approach
- **Business Logic**: Core service method testing with test-first development
- **Identifier Generation**: ID building and validation testing using Red-Green-Refactor
- **Data Transformation**: Scraping and normalization testing with failing tests written first

### Integration Testing
- **Database Operations**: Full CRUD operation testing following TDD principles
- **API Endpoints**: Request/response cycle testing with test-first approach
- **Version Management**: End-to-end versioning workflow testing using Red-Green-Refactor
- **Relationship Management**: Complex relationship scenario testing with comprehensive test coverage

### End-to-End Testing
- **Complete Workflows**: Entity creation through API consumption with behavior-driven tests
- **Data Import**: Scraping to database to API testing following TDD methodology
- **Multi-Entity Scenarios**: Complex entity relationship testing with test-first design
- **Performance Testing**: Large dataset handling validation with performance benchmarks

### Test Data Strategy
- **Authentic Nepali Data**: Real Nepali names, organizations, and locations for realistic testing
- **Cultural Context**: Proper Nepali political and administrative structures in test scenarios
- **Multilingual Testing**: Nepali and English name variations with comprehensive coverage
- **Edge Cases**: Boundary conditions and error scenarios designed through failing tests first

### TDD Implementation Guidelines
- **Test First**: Always write tests before implementing functionality
- **Minimal Implementation**: Write just enough code to pass the current test
- **Continuous Refactoring**: Regularly improve code structure while maintaining test coverage
- **Fast Feedback**: Ensure tests run quickly to support rapid Red-Green-Refactor cycles

## Performance Considerations

### Read-Time Optimization Priority
The system prioritizes read-time latency reduction over write-time performance, as the read-only API serves public consumers while writes are performed by data maintainers in controlled environments.

### Database Optimization
- **Read-Optimized File Organization**: Directory structure designed for fast entity lookups and retrieval
- **Aggressive Caching Strategy**: In-memory caching for frequently accessed entities with cache warming
- **Pre-computed Indexes**: Build search indexes during write operations to accelerate read queries
- **Denormalized Storage**: Store redundant data to minimize read-time joins and computations
- **Write-Time Processing**: Perform expensive operations (validation, normalization, indexing) during writes

### Search Service Optimization
- **Database Query Optimization**: Efficient filtering at the database layer
- **Result Caching**: Cache common search queries
- **Pagination Efficiency**: Limit result set sizes for fast response times

### API Performance
- **Fast Response Times**: Optimize for sub-100ms response times for entity retrieval
- **Efficient Pagination**: Pre-sorted data structures for instant offset-based pagination
- **HTTP Caching**: Aggressive caching headers with ETags for unchanged data
- **Query Pre-processing**: Pre-compute common filter combinations during write operations
- **Connection Pooling**: Optimize database connections for concurrent read requests

### Write-Time Trade-offs
- **Comprehensive Validation**: Accept longer write times for thorough data validation
- **Index Rebuilding**: Rebuild search indexes during writes to maintain read performance
- **Batch Processing**: Group write operations to amortize expensive operations
- **Background Processing**: Defer non-critical write operations to background tasks

### Scalability Design
- **Read Replica Strategy**: Support for multiple read-only database replicas
- **CDN Integration**: Static asset delivery through content delivery networks
- **Horizontal Read Scaling**: Independent scaling of read-only API instances
- **Async Write Processing**: Non-blocking write operations with eventual consistency

## Security Considerations

### Data Protection
- **Input Validation**: Comprehensive sanitization of all inputs
- **SQL Injection Prevention**: Parameterized queries and safe operations
- **XSS Protection**: Proper output encoding and sanitization
- **File System Security**: Safe file operations and path validation

### API Security
- **CORS Configuration**: Controlled cross-origin access
- **Rate Limiting**: Protection against DoS attacks
- **Input Size Limits**: Prevention of resource exhaustion
- **Error Information**: Careful error message disclosure

### Data Privacy
- **PII Handling**: Careful management of personally identifiable information
- **Attribution Privacy**: Optional anonymous contributions
- **Data Retention**: Clear policies for data lifecycle management
- **Access Control**: Future authentication and authorization framework