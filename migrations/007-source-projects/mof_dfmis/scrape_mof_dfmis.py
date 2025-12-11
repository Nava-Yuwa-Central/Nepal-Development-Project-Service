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
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from nes.services.scraping.web_scraper import RateLimiter, RetryHandler

# Configure logging
logger = logging.getLogger(__name__)


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
                
                # Process and normalize each project in the results
                for project_data in results:
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
            
            logger.info(f"Completed fetching from MoF DFMIS. Total projects: {len(all_projects)}")
            
        except Exception as e:
            logger.error(f"Error fetching projects from MoF DFMIS API: {e}")
            # If there was an error but we have some projects, return what we have
            if all_projects:
                logger.info(f"Returning {len(all_projects)} projects collected before error")
        
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

    def _normalize_dfmis_project(self, project_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a single DFMIS project to match the standardized project format.

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

            # Extract description from multiple possible fields
            description = details.get("input", "") or details.get("output", "") or details.get("outcome", "") or details.get("impact", "")

            # Extract implementing agency from the implementing_agency array
            implementing_agency = self._extract_agencies(project_data.get("implementing_agency", []), "organization__name")
            
            # If no implementing agency, try executing agency
            if not implementing_agency:
                implementing_agency = self._extract_agencies(project_data.get("executing_agency", []), "organization__name")

            # Extract location info
            locations = project_data.get("locations", [])
            primary_location = locations[0] if locations else {}
            location = {
                "country": "Nepal",
                "country_code": "NP",
                "region": primary_location.get("location_type", ""),
                "province": primary_location.get("province__name", ""),
                "district": primary_location.get("district__name", ""),
                "municipality": primary_location.get("municipality__name", "")
            }

            # Extract start and end dates
            start_date = details.get("actual_start_date") or details.get("proposed_start_date") or details.get("effectiveness_date")
            end_date = details.get("completion_date") or details.get("planned_completion_date")

            # Extract funding information from commitment data
            commitment_info = project_data.get("commitment", [])
            total_commitment = project_data.get("total_commitment", "")
            funding_source = ""
            loan_amount = ""
            grant_amount = ""
            
            if commitment_info and isinstance(commitment_info, list) and len(commitment_info) > 0:
                first_commitment = commitment_info[0]
                funding_source = first_commitment.get("donor__name", "")
                
                # Determine if it's a loan or grant based on assistance type
                assistance_type = first_commitment.get("assistance_type", "").lower()
                commitment_amount = first_commitment.get("commitment", "")
                
                if "grant" in assistance_type:
                    grant_amount = f"{commitment_amount} {first_commitment.get('signing_currency', '')}"
                elif "loan" in assistance_type:
                    loan_amount = f"{commitment_amount} {first_commitment.get('signing_currency', '')}"
            
            # Extract sector information
            sectors = project_data.get("sector", [])
            sector_names = []
            for sector in sectors:
                if isinstance(sector, dict):
                    sector_name = sector.get("sector__name", "")
                    if sector_name:
                        sector_names.append(sector_name)
            sector = ", ".join(sector_names)

            # Extract status information
            status = project_data.get("status", details.get("project_status", ""))

            # Extract development agencies (donors)
            development_agencies = project_data.get("development_agency", [])
            donor_names = []
            for agency in development_agencies:
                if isinstance(agency, dict):
                    name = agency.get("organization__name", "")
                    if name:
                        donor_names.append(name)
            funding_source = ", ".join(donor_names) if donor_names else funding_source

            # Extract executing agencies
            executing_agencies = project_data.get("executing_agency", [])
            executing_agency_names = []
            for agency in executing_agencies:
                if isinstance(agency, dict):
                    name = agency.get("organization__name", "")
                    if name:
                        executing_agency_names.append(name)
            executing_agency = ", ".join(executing_agency_names)

            # Extract government agencies
            government_agencies = project_data.get("government_agency", [])
            government_agency_names = []
            for agency in government_agencies:
                if isinstance(agency, dict):
                    name = agency.get("organization__name", "")
                    if name:
                        government_agency_names.append(name)
            borrower = ", ".join(government_agency_names)

            # Extract disbursement information
            disbursements = project_data.get("disbursement", [])
            total_disbursement = project_data.get("total_disbursement", "")
            real_time_spending = str(total_disbursement) if total_disbursement else ""

            # Create normalized project
            normalized_project = {
                "project_id": str(project_id),
                "title": title,
                "description": description,
                "implementing_agency": implementing_agency,
                "start_date": start_date or "",
                "end_date": end_date or "",
                "location": location,
                "funding_source": funding_source or "Government of Nepal",
                "total_allocated_budget": f"{total_commitment} USD" if total_commitment else "",  # Assuming USD as default, but this could be improved
                "real_time_spending": real_time_spending,
                "loan_amount": loan_amount,
                "grant_amount": grant_amount,
                "physical_progress": "",  # DFMIS API doesn't seem to provide this directly
                "financial_progress": "",  # DFMIS API doesn't seem to provide this directly
                "borrower": borrower or executing_agency,
                "sector": sector,
                "major_theme": "",  # DFMIS API doesn't seem to provide a major theme specifically
                "environmental_category": details.get("climate", ""),
                "implementation_status": status,
                "url": f"https://dfims.mof.gov.np/projects/{project_id}" if project_id else "",  # DFMIS project URL
                "project_document_url": "",  # DFMIS API doesn't seem to provide document URLs directly
                "milestones": [
                    {
                        "name": "Agreement Date",
                        "date": details.get("agreement_date", ""),
                        "status": "Completed" if details.get("agreement_date") else "Planned",
                        "description": "Project agreement signed"
                    },
                    {
                        "name": "Effectiveness Date",
                        "date": details.get("effectiveness_date", ""),
                        "status": "Completed" if details.get("effectiveness_date") else "Planned", 
                        "description": "Project became effective"
                    },
                    {
                        "name": "Completion Date",
                        "date": details.get("completion_date", ""),
                        "status": "Completed" if details.get("completion_date") else "Planned",
                        "description": "Project completed"
                    }
                ],
                "yearly_budget_breakdown": [],  # DFMIS API doesn't seem to provide yearly breakdowns in this format
                "cost_overruns": {},
                "reports": [],  # DFMIS API doesn't seem to provide report links directly
                "verification_documents": [],  # DFMIS API doesn't seem to provide verification documents directly
                "photos": [],  # DFMIS API doesn't seem to provide photos directly
                "contractor_change_log": [],  # DFMIS API doesn't seem to provide contractor information directly
                "last_updated": details.get("date_modified", datetime.now().isoformat()),
                "source": "MoF DFMIS (Ministry of Finance - Development Finance Information Management System)",
                "source_api": "MoF DFMIS API (https://dfims.mof.gov.np/api/v2/core/projects/)"
            }

            # Only return if there's a valid title
            if normalized_project["title"]:
                return normalized_project
            else:
                logger.debug(f"Skipping project with no title: {project_data}")
                return None
        except Exception as e:
            logger.error(f"Error normalizing DFMIS project: {e}")
            logger.debug(f"Problematic project data: {project_data}")
            return None


async def scrape_and_save_dfmis_projects(output_file: str = "dfmis_projects.json") -> int:
    """Scrape or transform DFMIS projects and save to a JSON file.

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

    # Save projects to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(projects, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(projects)} DFMIS projects to {output_path}")
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