"""
MoF DFMIS (Ministry of Finance - Development Finance Information Management System) Data Scraper for Nepal Development Projects.

This module provides functionality to extract project data from the Nepal Government's 
MoF DFMIS API for projects related to Nepal. It follows the existing architecture patterns
in the nes project and transforms DFMIS data to match the standardized project schema used by other sources.
"""

import os
import asyncio
import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from html import unescape
try:
    from html.parser import HTMLParser
except ImportError:
    from HTMLParser import HTMLParser

import aiohttp

from nes.core.models.project import (
    Project, ProjectStage, FinancingComponent, FinancingInstrument,
    FinancingInstrumentType, ProjectDateEvent, ProjectLocation,
    SectorMapping, CrossCuttingTag, DonorExtension
)
from nes.core.models.base import Name, NameParts, NameKind, LangText, LangTextValue
from nes.core.models.entity import EntitySubType, EntityType
from nes.core.models.version import VersionSummary, VersionType, Author
from nes.services.scraping.web_scraper import RateLimiter, RetryHandler

# Configure logging
logger = logging.getLogger(__name__)


class HTMLStripper(HTMLParser):
    """Simple HTML tag stripper for converting HTML content to plain text."""

    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_html_tags(html_content: str) -> str:
    """Strip HTML tags from content and return plain text."""
    if not html_content:
        return ""

    # First unescape HTML entities
    unescaped = unescape(html_content)

    # Then strip HTML tags
    stripper = HTMLStripper()
    stripper.feed(unescaped)
    return stripper.get_data().strip()


class MOFDFMISAPIClient:
    """HTTP client for MoF DFMIS API with rate limiting and retry logic."""

    def __init__(
        self,
        requests_per_second: float = 0.5,  # Conservative rate limit
        requests_per_minute: int = 30,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        """Initialize the MoF DFMIS API client.

        Args:
            requests_per_second: Maximum requests per second per domain
            requests_per_minute: Maximum requests per minute per domain
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            requests_per_minute=requests_per_minute,
        )
        self.retry_handler = RetryHandler(max_retries=max_retries)
        self.timeout = timeout
        self.session = None

    async def __aenter__(self):
        """Async context manager entry."""
        # Create a session that can store cookies for authentication
        # Disable SSL verification for dfims.mof.gov.np due to certificate issues
        import ssl
        connector = aiohttp.TCPConnector(ssl=False)  # Disable SSL verification

        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            connector=connector,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/json",
                "Connection": "keep-alive",
                "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-site": "same-origin",
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def _get_session_cookies(self) -> bool:
        """Get session cookies by accessing the main page first."""
        try:
            # Access the main page to get initial session cookies
            main_url = "https://dfims.mof.gov.np/projects"

            # Apply rate limiting
            await self.rate_limiter.acquire("dfims.mof.gov.np")

            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }

            # Use the same session which already has SSL disabled
            async with self.session.get(main_url, headers=headers) as response:
                if response.status in [200, 201, 302, 304]:
                    logger.info("Successfully accessed main page to establish session")
                    # Cookies are automatically handled by the session
                    return True
                else:
                    logger.warning(f"Failed to access main page: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Error accessing main page for session: {e}")
            return False

    async def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make a request to the MoF DFMIS API with rate limiting, session cookies, and error handling.

        Args:
            url: The API endpoint URL
            params: Query parameters for the request

        Returns:
            JSON response data or None if request fails
        """
        if not self.session:
            raise RuntimeError("Client not initialized. Use within async context manager.")

        # First, try to get session cookies by accessing the main page
        await self._get_session_cookies()

        # Apply rate limiting
        await self.rate_limiter.acquire("dfims.mof.gov.np")

        # Prepare URL with parameters
        if params:
            query_string = urlencode(params)
            full_url = f"{url}?{query_string}"
        else:
            full_url = url

        try:
            # Use browser-like headers to mimic web requests, including potential authentication
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Ch-Ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"macOS"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",  # Important for same-origin requests
                "Referer": "https://dfims.mof.gov.np/projects",  # Referer header may be required
                "X-Requested-With": "XMLHttpRequest",  # Many APIs expect this for AJAX requests
            }

            # Add CSRF token if available in cookies
            # Attempt to get CSRF token from session cookies
            csrf_token = None
            for cookie in self.session.cookie_jar:
                if cookie.key.lower() == 'csrftoken':
                    csrf_token = cookie.value
                    break

            if csrf_token:
                headers["X-CSRFToken"] = csrf_token

            # Add authentication if needed (we might need to handle the 'Bearer null' issue)
            if os.getenv("MOF_DFMIS_AUTH_TOKEN"):
                headers["Authorization"] = f"Bearer {os.getenv('MOF_DFMIS_AUTH_TOKEN')}"
            else:
                # Default to 'Bearer null' as shown in the original request
                headers["Authorization"] = "Bearer null"

            async with self.session.get(full_url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 401:
                    logger.warning(f"Unauthorized access to {full_url}. Need proper authentication.")
                    # Try with additional headers that might be needed
                    headers.update({
                        "Authorization": f"Bearer {os.getenv('MOF_DFMIS_AUTH_TOKEN')}" if os.getenv('MOF_DFMIS_AUTH_TOKEN') else "Bearer null",
                    })
                    # Retry with updated headers
                    async with self.session.get(full_url, headers=headers) as retry_response:
                        if retry_response.status == 200:
                            return await retry_response.json()
                elif response.status == 403:
                    logger.warning(f"Forbidden access to {full_url}. May require login or special permissions.")
                elif response.status == 404:
                    logger.warning(f"Endpoint not found: {full_url}. Trying alternative endpoint.")

                logger.warning(f"API request failed with status {response.status}: {full_url}")
                logger.warning(f"Response text: {await response.text()}")
                return None
        except asyncio.TimeoutError:
            logger.error(f"Request timeout for URL: {full_url}")
            return None
        except Exception as e:
            logger.error(f"Error making request to {full_url}: {e}")
            return None


class MOFDFMISProjectScraper:
    """Scraper for MoF DFMIS (Ministry of Finance - Development Finance Information Management System) projects in Nepal."""

    # Main API endpoint
    DFMIS_API_URL = "https://dfims.mof.gov.np/api/v2/core/projects/"

    def __init__(self, client: Optional[MOFDFMISAPIClient] = None):
        """Initialize the MoF DFMIS project scraper.

        Args:
            client: MOFDFMISAPIClient instance. If None, a default client will be created
        """
        self.client = client or MOFDFMISAPIClient()

    async def search_dfmis_projects(self) -> List[Dict[str, Any]]:
        """Search for MoF DFMIS projects related to Nepal.

        Returns:
            List of project data dictionaries
        """
        async with self.client:
            projects = await self._fetch_projects_from_dfmis_api()
            logger.info(f"Successfully scraped {len(projects)} projects from MoF DFMIS")
            return projects

    async def _fetch_projects_from_dfmis_api(self) -> List[Dict[str, Any]]:
        """Fetch projects from MoF DFMIS API with pagination.

        Returns:
            List of project data dictionaries
        """

        # Check if all_projects.json exists, and use it if it does
        raw_output_path = os.path.join(os.path.dirname(__file__), "all_projects.json")
        if os.path.exists(raw_output_path):
            logger.info(f"Found existing {raw_output_path}, loading projects from file...")
            with open(raw_output_path, 'r', encoding='utf-8') as f:
                all_raw_projects = json.load(f)
            logger.info(f"Loaded {len(all_raw_projects)} raw DFMIS projects from {raw_output_path}")
        else:
            logger.info("No cached all_projects.json found, fetching from API...")
            all_raw_projects = []  # Store raw projects for reference
            all_projects = []
            page = 1
            items_per_page = 100  # Use larger page size to reduce requests

            try:
                while True:
                    logger.info(f"Fetching page {page} from MoF DFMIS API...")

                    params = {
                        "page": page,
                        "items_per_page": items_per_page,
                        "search_term": "",
                        "ordering": "id",
                        "sort_order": "asc",
                        "sortBy": "id"
                    }

                    data = await self.client._make_request(self.DFMIS_API_URL, params)

                    if data is None:
                        logger.warning(f"Failed to fetch page {page}, stopping pagination.")
                        break

                    results = data.get("results", [])
                    count = data.get("count", 0)

                    if not results:
                        logger.info(f"No more results found, stopping pagination at page {page}")
                        break

                    # Store raw project data before normalization
                    for project_data in results:
                        all_raw_projects.append(project_data)
                        normalized = self._normalize_dfmis_project(project_data)
                        if normalized:
                            all_projects.append(normalized)

                    logger.info(f"Processed {len(results)} projects from page {page}. Total so far: {len(all_projects)}/{count}")

                    # If we got fewer results than the page size, we're probably on the last page
                    if len(results) < items_per_page:
                        break

                    # Check if we've reached the total count
                    if len(all_projects) >= count:
                        break

                    page += 1

                    # Add a small delay between pages to be respectful to the API
                    await asyncio.sleep(0.5)

                # Save raw projects to file for future use
                os.makedirs(os.path.dirname(raw_output_path), exist_ok=True)
                with open(raw_output_path, 'w', encoding='utf-8') as f:
                    json.dump(all_raw_projects, f, ensure_ascii=False, indent=2)
                logger.info(f"Saved {len(all_raw_projects)} raw DFMIS projects to {raw_output_path}")

                logger.info(f"Completed fetching from MoF DFMIS. Total projects: {len(all_projects)}")

            except Exception as e:
                logger.error(f"Error fetching projects from MoF DFMIS API: {e}")
                # If there was an error but we have some projects, return what we have
                if all_projects:
                    logger.info(f"Returning {len(all_projects)} projects collected before error")

            return all_projects

        # If we loaded from cache file, process all_raw_projects into normalized format
        all_projects = []
        for project_data in all_raw_projects:
            normalized = self._normalize_dfmis_project(project_data)
            if normalized:
                all_projects.append(normalized)

        return all_projects

    def _extract_agencies(self, agency_list: List[Dict[str, Any]], field_name: str) -> str:
        """Extract agency names from a list of agency objects.
        
        Args:
            agency_list: List of agency objects from the API
            field_name: The field name containing the agency name (e.g., 'organization__name')
        
        Returns:
            Comma-separated string of agency names
        """
        if not agency_list or not isinstance(agency_list, list):
            return ""
        
        names = []
        for agency in agency_list:
            if isinstance(agency, dict) and field_name in agency:
                name = agency[field_name]
                if name:
                    names.append(str(name))
        
        return ", ".join(names)

    def _extract_agency_details(self, agency_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract full agency details including organization metadata.
        
        Args:
            agency_list: List of agency objects from the API
        
        Returns:
            List of agency detail dictionaries with name, architecture, and group
        """
        if not agency_list or not isinstance(agency_list, list):
            return []
        
        details = []
        for agency in agency_list:
            if isinstance(agency, dict):
                org_name = agency.get("organization__name", "")
                if org_name:
                    details.append({
                        "name": org_name,
                        "architecture": agency.get("organization__development_cooperation_group__architecture__name", ""),
                        "group": agency.get("organization__development_cooperation_group__name", ""),
                        "percentage": agency.get("percentage"),
                    })
        return details

    def _extract_location_details(self, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract location details from raw location data.
        
        Args:
            locations: List of location objects from the API
        
        Returns:
            List of location detail dictionaries
        """
        if not locations or not isinstance(locations, list):
            return []
        
        details = []
        for loc in locations:
            if isinstance(loc, dict):
                details.append({
                    "location_type": loc.get("location_type", ""),
                    "province": loc.get("province__name"),
                    "district": loc.get("district__name"),
                    "municipality": loc.get("municipality__name"),
                    "percentage": loc.get("percentage"),
                })
        return details

    def _normalize_dfmis_project(self, project_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a single DFMIS project to match the new Pydantic project model.

        Args:
            project_data: Raw project data from DFMIS API

        Returns:
            Normalized project data in standard format, or None if invalid
        """
        try:
            # Extract project_id
            project_id = project_data.get("project_id", project_data.get("id", ""))

            # Get details section (most project info is in the details object)
            details = project_data.get("details", {})

            # Extract title
            title = details.get("name", project_data.get("name", "")).strip()
            if not title:
                logger.debug(f"Skipping project with no title: {project_data.get('id', 'unknown')}")
                return None

            # Convert DFMIS status to ProjectStage enum
            status_str = project_data.get("status", details.get("project_status", "")).upper()

            # DFMIS status mapping to ProjectStage enum values
            status_mapping = {
                'PLANNED': 'PLANNING',
                'ONGOING': 'ONGOING',
                'COMPLETED': 'COMPLETED',
                'CANCELLED': 'CANCELLED',
                'PIPELINE': 'PIPELINE',
                'APPROVED': 'APPROVED',
                'SUSPENDED': 'SUSPENDED',
                'TERMINATED': 'TERMINATED'
            }

            # Check if we have a mapping, otherwise use status_str directly
            mapped_status = status_mapping.get(status_str, status_str)

            try:
                # Handle mapping from DFMIS status to ProjectStage
                stage = ProjectStage[mapped_status]
            except KeyError:
                stage = ProjectStage.UNKNOWN

            # Create slug from project ID - must match slug pattern [a-z0-9-]+
            slug = f"dfmis-{project_id}"

            # Create names list (required by Entity base class)
            names = [
                Name(
                    kind=NameKind.PRIMARY,
                    en=NameParts(full=title)
                )
            ]
            # Add Nepali name if available
            if details.get("name_ne"):
                names.append(
                    Name(
                        kind=NameKind.ALTERNATE,
                        ne=NameParts(full=details["name_ne"])
                    )
                )

            # Create version summary (required by Entity base class)
            version_summary = VersionSummary(
                entity_or_relationship_id=f"project:{EntitySubType.DEVELOPMENT_PROJECT.value}/{slug}",
                type=VersionType.ENTITY,
                version_number=1,
                author=Author(slug="dfmis-import", name="MoF DFMIS Import"),
                change_description="Import from MoF DFMIS",
                created_at=datetime.now()
            )

            # Extract description from DFMIS details (convert string to LangText)
            description_text = (
                details.get("input", "") or
                details.get("output", "") or
                details.get("outcome", "") or
                details.get("impact", "")
            )

            # Strip HTML tags from description for plain text storage
            plain_description = strip_html_tags(description_text) if description_text else ""

            description = LangText(
                en=LangTextValue(value=plain_description) if plain_description else None
            ) if plain_description else None

            # Extract location info for ProjectLocation model
            location_data = []
            locations = project_data.get("locations", [])
            for loc in locations:
                if loc:  # Only process if location exists
                    # Since DFMIS doesn't provide lat/lng, we might need to skip or use defaults
                    # For now, we'll create the location model with available data if we have coordinates
                    if loc.get("province__name") or loc.get("district__name") or loc.get("municipality__name"):
                        # We'll need to skip creating ProjectLocation without coordinates for now
                        # ProjectLocation model requires latitude and longitude
                        pass

            # Extract sectors
            sector_mappings = []
            sectors = project_data.get("sector", [])
            for sector in sectors:
                if isinstance(sector, dict):
                    sector_name = sector.get("sector__name", "")
                    if sector_name:
                        sector_mappings.append(
                            SectorMapping(
                                normalized_sector=sector_name,
                                donor_sector=sector_name
                            )
                        )

            # Extract financing information from commitment data
            financing_components = []
            commitment_info = project_data.get("commitment", [])
            for commitment in commitment_info:
                if isinstance(commitment, dict):
                    commitment_amount = commitment.get("commitment")
                    assistance_type = commitment.get("assistance_type", "").lower()

                    if commitment_amount is not None:
                        # Determine instrument type based on assistance type
                        if "grant" in assistance_type:
                            instrument_type = FinancingInstrumentType.GRANT
                        elif "loan" in assistance_type:
                            instrument_type = FinancingInstrumentType.LOAN
                        else:
                            instrument_type = FinancingInstrumentType.OTHER

                        financing_instrument = FinancingInstrument(
                            instrument_type=instrument_type,
                            currency=commitment.get("signing_currency"),
                            amount=commitment_amount,
                            tying_status=commitment.get("tied_status")
                        )

                        financing_components.append(
                            FinancingComponent(
                                name=commitment.get("financing_instrument", "Project Support"),
                                financing=financing_instrument
                            )
                        )

            # Extract dates
            date_events = []
            if details.get("agreement_date"):
                try:
                    date_obj = date.fromisoformat(details["agreement_date"].split("T")[0]) if "T" in details["agreement_date"] else date.fromisoformat(details["agreement_date"])
                    date_events.append(
                        ProjectDateEvent(
                            date=date_obj,
                            type="APPROVAL",
                            source="MoF DFMIS"
                        )
                    )
                except ValueError:
                    pass  # Invalid date format, skip

            if details.get("effectiveness_date"):
                try:
                    date_obj = date.fromisoformat(details["effectiveness_date"].split("T")[0]) if "T" in details["effectiveness_date"] else date.fromisoformat(details["effectiveness_date"])
                    date_events.append(
                        ProjectDateEvent(
                            date=date_obj,
                            type="START",
                            source="MoF DFMIS"
                        )
                    )
                except ValueError:
                    pass  # Invalid date format, skip

            if details.get("completion_date"):
                try:
                    date_obj = date.fromisoformat(details["completion_date"].split("T")[0]) if "T" in details["completion_date"] else date.fromisoformat(details["completion_date"])
                    date_events.append(
                        ProjectDateEvent(
                            date=date_obj,
                            type="COMPLETION",
                            source="MoF DFMIS"
                        )
                    )
                except ValueError:
                    pass  # Invalid date format, skip

            # Extract agencies with full metadata
            implementing_agency = self._extract_agencies(project_data.get("implementing_agency", []), "organization__name")
            executing_agency = self._extract_agencies(project_data.get("executing_agency", []), "organization__name")
            
            # Extract detailed agency info for migration (includes architecture/group)
            implementing_agency_details = self._extract_agency_details(project_data.get("implementing_agency", []))
            executing_agency_details = self._extract_agency_details(project_data.get("executing_agency", []))
            government_agency_details = self._extract_agency_details(project_data.get("government_agency", []))
            
            # Extract location details
            location_details = self._extract_location_details(project_data.get("locations", []))

            # Extract donor information with full metadata
            development_agencies = project_data.get("development_agency", [])
            donor_names = []
            donor_extensions = []
            development_agency_details = self._extract_agency_details(development_agencies)
            for agency in development_agencies:
                if isinstance(agency, dict):
                    name = agency.get("organization__name", "")
                    if name:
                        donor_names.append(name)
                        # Create donor extension with raw data
                        donor_extension = DonorExtension(
                            donor=name,
                            donor_project_id=str(project_id),
                            raw_payload=agency
                        )
                        donor_extensions.append(donor_extension)

            # Create project instance using the new model (with Entity base class)
            # Note: The id field will be computed from type, subtype, and slug
            project = Project(
                slug=slug,  # Required by Entity base class
                names=names,  # Required by Entity base class
                version_summary=version_summary,  # Required by Entity base class
                created_at=datetime.now(),  # Required by Entity base class
                type=EntityType.PROJECT,
                sub_type=EntitySubType.DEVELOPMENT_PROJECT,
                stage=stage,
                description=description,  # Convert to LangText now
                implementing_agency=implementing_agency or None,
                executing_agency=executing_agency or None,
                financing=financing_components if financing_components else None,
                dates=date_events if date_events else None,
                # NOTE: Not adding locations since DFMIS doesn't provide coordinates required by ProjectLocation
                sectors=sector_mappings if sector_mappings else None,
                # NOTE: Not adding tags for now
                donors=donor_names if donor_names else None,
                donor_extensions=donor_extensions if donor_extensions else None,
                project_url=f"https://dfims.mof.gov.np/projects/{project_id}" if project_id else None
            )

            # Convert to dict for compatibility with existing code structure
            # Enable serialization of datetime objects
            result = project.model_dump(by_alias=True, exclude_unset=True, mode='json')
            
            # Add detailed agency/location info for migration (not part of Project model)
            # These are used by migrate.py to create relationships with proper org subtypes
            result["_migration_metadata"] = {
                "implementing_agencies": implementing_agency_details,
                "executing_agencies": executing_agency_details,
                "development_agencies": development_agency_details,
                "government_agencies": government_agency_details,
                "locations": location_details,
            }
            
            return result

        except Exception as e:
            logger.error(f"Error normalizing DFMIS project: {e}")
            logger.debug(f"Problematic project data: {project_data}")
            return None


async def scrape_and_save_dfmis_projects(output_file: str = "dfmis_projects.jsonl") -> int:
    """Scrape and transform DFMIS projects and save to a JSONL file in source directory.

    Args:
        output_file: Name of the output file where projects will be saved

    Returns:
        Number of projects scraped and saved
    """
    logger.info("Starting DFMIS project scraping/transforming...")

    # Define the source directory - this is relative to the project root
    # We want to save to migrations/007-source-projects/source/
    project_root = os.path.join(os.path.dirname(__file__), "..", "..", "..")
    source_dir = os.path.join(project_root, "migrations", "007-source-projects", "source")
    os.makedirs(source_dir, exist_ok=True)

    # Create the full output path
    output_path = os.path.join(source_dir, output_file)

    scraper = MOFDFMISProjectScraper()
    projects = await scraper.search_dfmis_projects()

    # Save projects to file in JSONL format
    with open(output_path, 'w', encoding='utf-8') as f:
        for project in projects:
            f.write(json.dumps(project, ensure_ascii=False) + "\n")

    logger.info(f"Saved {len(projects)} DFMIS projects to {output_path} in JSONL format")
    return len(projects)


if __name__ == "__main__":
    # For development and testing
    async def main():
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logger.info("Running MoF DFMIS project scraper/transformer...")

        # Scrape and save projects
        count = await scrape_and_save_dfmis_projects()
        logger.info(f"Completed scraping/transformation. Total projects: {count}")

    # Run the scraper
    asyncio.run(main())