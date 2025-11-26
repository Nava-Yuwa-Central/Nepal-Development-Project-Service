# World Bank Data Scraper for Nepal Development Projects

This module provides functionality to crawl and extract project data from the World Bank's APIs for projects related to Nepal. It follows the existing architecture patterns in the nes project and implements proper rate limiting, error handling, and data normalization.

## Overview

The scraper targets the following World Bank APIs:
1. World Bank Projects & Operations API: https://search.worldbank.org/api/v2/projects
2. FinancesOne API: https://datacatalogapi.worldbank.org/dexapps/fone/summary/ibrd/lending/table
3. World Bank Open API: https://api.worldbank.org/v2/projects

## Architecture

The implementation consists of:
- `scrape_world_bank.py`: Main scraper implementation with API client, rate limiting, and data normalization
- `migrate.py`: Migration script to import scraped data into the system
- `test_worldbank_scraper.py`: Comprehensive test suite

## Usage

### 1. Run the scraper to collect data:
```bash
python -m migrations.007-source-projects.scrape_world_bank
```

This will:
- Scrape project data from World Bank APIs for Nepal
- Normalize the data to the Public Transparency Portal (PTP) format
- Save the data to JSON files in the `world_bank/` subdirectory
- Save a copy to `world_bank_projects.json` for migration

### 2. Run the migration to import data:
```bash
python -m nes.services.migration.manager run 007
```

This will:
- Load the scraped data from `world_bank_projects.json`
- Create Project entities in the database
- Create relationships (funded by, located in, implemented by)
- Log progress and errors

## Data Model

The scraper normalizes data to match the PTP requirements:
- Project ID, title, description
- Implementing agency
- Timeline and milestones
- Location information (province, district, municipality)
- Funding information (source, budget, spending)
- Progress metrics (physical and financial)
- Reports and documents
- Contractor information

## Features

- **Rate Limiting**: Respects World Bank API limits with configurable rate limiting
- **Error Handling**: Comprehensive error handling with logging and retry logic
- **Data Normalization**: Converts raw API data to standard PTP format
- **Relationship Mapping**: Creates appropriate relationships between projects, funders, and locations
- **Migration Integration**: Seamlessly integrates with existing migration system
- **Testing**: Comprehensive test suite covering all functionality

## API Endpoints Used

The scraper uses these World Bank API endpoints:
- Projects & Operations: Retrieves project details
- FinancesOne: Retrieves lending information
- Combines data from multiple sources to provide comprehensive project view

## Rate Limiting

The scraper implements conservative rate limits (default 0.5 requests per second) to be respectful to the World Bank APIs.