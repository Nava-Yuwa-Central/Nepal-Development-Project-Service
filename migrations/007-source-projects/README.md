# 007-source-projects: Development Project Data Migration

This migration imports development project data from multiple sources to create a comprehensive database of development projects in Nepal. The system is designed to accommodate various data sources including government systems and international development partners.

## Purpose

This migration imports official development project data from various sources to create a comprehensive database of development projects in Nepal. The system standardizes data from different sources into a unified schema while preserving source-specific details.

## Data Sources

### Current Sources

#### MoF DFMIS (Ministry of Finance - Development Finance Information Management System)
- **Primary Source**: MoF DFMIS API (`https://dfims.mof.gov.np/api/v2/core/projects/`)
- **Organization**: Ministry of Finance, Government of Nepal
- **Coverage**: Development projects funded by various development partners (World Bank, ADB, JICA, EU, etc.)
- **Update Frequency**: Data represents the official GoN view of development projects

### Planned Sources

#### World Bank Projects
- **Primary Source**: World Bank API (`https://projects.worldbank.org/api/`)
- **Organization**: World Bank Group
- **Coverage**: World Bank-funded projects in Nepal
- **Update Frequency**: Updated regularly with new and ongoing projects

#### Asian Development Bank (ADB) Projects
- **Primary Source**: ADB iVisions API
- **Organization**: Asian Development Bank
- **Coverage**: ADB-funded projects in Nepal
- **Update Frequency**: Updated quarterly with new and ongoing projects

#### Japan International Cooperation Agency (JICA) Projects
- **Primary Source**: JICA API/Database
- **Organization**: Japan International Cooperation Agency
- **Coverage**: JICA-funded projects in Nepal
- **Update Frequency**: Updated biannually with new and ongoing projects

#### European Union (EU) Projects
- **Primary Source**: EU Open Data Portal
- **Organization**: European Union
- **Coverage**: EU-funded projects in Nepal
- **Update Frequency**: Updated annually with new and ongoing projects

## Data Structure

The migration processes the following project data elements from various sources:

### Project Information
- Project ID, name, and description
- Project status and implementation timeline
- Financing details (amounts, types, currencies)
- Sector classifications
- Geographic coverage (province, district, municipality level)

### Development Partners
- Funding organizations and amounts
- Implementing/executing agencies
- Donor information and categorization
- Commitment and disbursement data

## Migration Process

### Step 1: Data Scraping
The migration first scrapes data from various APIs using source-specific scrapers in subdirectories:

```bash
# For DFMIS projects
python -m migrations.007-source-projects.mof_dfmis.scrape_mof_dfmis
```

This creates source-specific JSONL files with normalized project data.

### Step 2: Data Migration
The migration then processes the scraped data to create entities in the system:

- Creates Project entities with complete metadata
- Establishes relationships with organizations and locations
- Links funding sources and implementing agencies
- Preserves all original data in donor extensions

## Key Features

- **HTML Sanitization**: Descriptions containing HTML are converted to plain text
- **Entity Standardization**: Data from various sources is mapped to standardized entity schema
- **Relationship Mapping**: Funding and implementation relationships are preserved
- **Data Integrity**: Duplicate handling and slug generation with conflict resolution
- **Multi-Source Support**: System designed to accommodate various data sources

## Dependencies

This migration does not depend on other migrations but serves as a foundational data source for subsequent analysis and reporting.

## Data Quality

- **Completeness**: Represents the most comprehensive view of development projects from official sources
- **Accuracy**: Data comes from official government and development partner systems
- **Coverage**: Includes projects from all major development partners
- **Updates**: Represents the latest official project information from each source

## Output

The migration creates:
- Development project entities with complete metadata
- Organization entities for funding/implementing agencies
- Location relationships for project coverage
- Financial data with commitment and disbursement information
- Source-specific extensions preserving original data formats

## Validation

After running the migration, verify:
- All projects from configured sources have been imported
- Relationships are correctly established
- Financial data is properly mapped
- No duplicate entities are created
- Source attribution is preserved correctly