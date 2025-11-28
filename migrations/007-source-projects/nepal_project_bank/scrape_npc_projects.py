"""
NPC (National Planning Commission) Data Scraper for Nepal Development Projects.

This module provides functionality to crawl and extract project data from
the Nepal Government's NPBMIS (Nepal Planning Database Management Information System)
API for projects related to Nepal. It follows the existing architecture patterns
in the nes project and implements proper rate limiting, error handling,
and data normalization.
It can either scrape directly from the API or transform data from a local file.
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


class NPCAPIClient:
    """HTTP client for NPC APIs with rate limiting and retry logic."""

    def __init__(
        self,
        requests_per_second: float = 0.5,  # Conservative rate limit
        requests_per_minute: int = 30,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        """Initialize the NPC API client.

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
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
                "Accept": "application/json, text/html, */*",
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
            main_url = "https://npbmis.npc.gov.np/"

            # Apply rate limiting
            await self.rate_limiter.acquire("npbmis.npc.gov.np")

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
        """Make a request to the NPC API with rate limiting, session cookies, and error handling.

        Args:
            url: The API endpoint URL
            params: Query parameters for the request

        Returns:
            JSON response data or None if request fails
        """
        if not self.session:
            raise RuntimeError("Client not initialized. Use within async context manager.")

        # First, get session cookies by accessing the main page
        await self._get_session_cookies()

        # Apply rate limiting
        await self.rate_limiter.acquire("npbmis.npc.gov.np")

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
                "Referer": "https://npbmis.npc.gov.np/",  # Referer header may be required
                "X-Requested-With": "XMLHttpRequest",  # Many APIs expect this for AJAX requests
            }

            # Add authentication if needed (we might need to handle cookies or tokens)
            if os.getenv("NPC_AUTH_COOKIE"):
                headers["Cookie"] = os.getenv("NPC_AUTH_COOKIE")

            async with self.session.get(full_url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 401:
                    logger.warning(f"Unauthorized access to {full_url}. Need proper authentication.")
                    # Try with additional headers that might be needed
                    headers.update({
                        "Authorization": f"Bearer {os.getenv('NPC_API_TOKEN')}" if os.getenv('NPC_API_TOKEN') else "",
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


class NPCProjectScraper:
    """Scraper for NPC (National Planning Commission) projects in Nepal."""

    # Main API endpoints
    NPC_API_URL = "https://npbmis.npc.gov.np/api/data"
    HOMEPAGE_API_URL = "https://npbmis.npc.gov.np/api/homepage"  # The API endpoint mentioned by the user

    def __init__(self, client: Optional[NPCAPIClient] = None):
        """Initialize the NPC project scraper.

        Args:
            client: NPCAPIClient instance. If None, a default client will be created
        """
        self.client = client or NPCAPIClient()

    async def search_npc_projects(self) -> List[Dict[str, Any]]:
        """Search for NPC projects related to Nepal.

        Returns:
            List of project data dictionaries
        """
        async with self.client:
            projects = await self._fetch_projects_from_npc_api()
            logger.info(f"Successfully scraped {len(projects)} projects from NPC")
            return projects

    async def _fetch_projects_from_npc_api(self) -> List[Dict[str, Any]]:
        """Fetch projects from NPC API, with fallback to local file if API fails.

        Returns:
            List of project data dictionaries
        """
        # First, try to access the homepage API directly as mentioned by user
        homepage_url = self.HOMEPAGE_API_URL
        try:
            logger.info(f"Attempting to fetch data from homepage API: {homepage_url}")
            data = await self.client._make_request(homepage_url)

            if data is not None:
                logger.info("Successfully fetched data from homepage API")
                return self._process_fetched_data(data)
            else:
                logger.warning("Homepage API request failed, trying main data API...")
                # If homepage API fails, try the main data API
                data = await self.client._make_request(self.NPC_API_URL)

                if data is not None:
                    logger.info("Successfully fetched data from main API")
                    return self._process_fetched_data(data)
                else:
                    logger.warning("All API attempts failed, trying to load from local file...")
        except Exception as e:
            logger.error(f"Error fetching from API: {e}")

        # If API access fails, load from the local file as fallback
        return await self._load_from_local_file()

    async def _load_from_local_file(self) -> List[Dict[str, Any]]:
        """Load projects from the local all_projects.json file as a fallback.

        Returns:
            List of project data dictionaries
        """
        try:
            # Try to locate the all_projects.json file
            local_paths = [
                "nepal_project_bank/all_projects.json",  # Relative to current script
                "../nepal_project_bank/all_projects.json",  # One level up
                "migrations/007-source-projects/nepal_project_bank/all_projects.json",  # Full path relative to project
                "/Users/interstellarninja/Documents/projects/nyc/Nepal-Development-Project-Service/migrations/007-source-projects/nepal_project_bank/all_projects.json",
            ]

            data = None
            file_path = None

            for path in local_paths:
                try:
                    abs_path = os.path.join(os.path.dirname(__file__), path)
                    if os.path.exists(abs_path):
                        file_path = abs_path
                        with open(abs_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        logger.info(f"Loaded data from local file: {file_path}")
                        break
                except Exception as e:
                    logger.debug(f"Could not load from {path}: {e}")
                    continue

            if data is None:
                logger.error("Could not find all_projects.json in any of the expected locations")
                return []

            # Extract projects from the nested structure if needed
            if isinstance(data, dict) and "projects" in data and isinstance(data["projects"], dict) and "projects" in data["projects"]:
                raw_projects = data["projects"]["projects"]
            elif isinstance(data, dict) and "projects" in data and isinstance(data["projects"], list):
                raw_projects = data["projects"]
            elif isinstance(data, list):
                raw_projects = data
            else:
                # If it's a single project object, wrap in a list
                raw_projects = [data]

            # Process and normalize projects
            processed_projects = []
            for project in raw_projects:
                processed_project = self._normalize_npc_project(project)
                if processed_project:
                    processed_projects.append(processed_project)

            logger.info(f"Loaded {len(processed_projects)} projects from local file")
            return processed_projects

        except Exception as e:
            logger.error(f"Error loading from local file: {e}")
            return []

    def _process_fetched_data(self, data: Any) -> List[Dict[str, Any]]:
        """Process fetched API data and normalize to standard format.

        Args:
            data: Raw data from API

        Returns:
            List of normalized project dictionaries
        """
        if isinstance(data, dict):
            # Handle the actual response format from NPC API
            # Check if there's a success field and data inside
            if not data.get('success', False):
                error_info = data.get('error', {})
                logger.warning(f"NPC API returned error: {error_info}")

                # Even if there's an error flag, there might still be data
                # For example, error might indicate rate limiting but data still available
                if 'data' in data:
                    raw_projects = data['data']
                elif 'projects' in data:
                    raw_projects = data['projects']
                else:
                    logger.error("No valid data found in API response despite error flag")
                    return []
            else:
                # Success response - extract projects
                if 'data' in data:
                    raw_projects = data['data']
                elif 'projects' in data and isinstance(data['projects'], dict):
                    # If projects is a dict with nested projects array
                    if 'projects' in data['projects']:
                        raw_projects = data['projects']['projects']
                    else:
                        raw_projects = data['projects']
                elif 'projects' in data and isinstance(data['projects'], list):
                    raw_projects = data['projects']
                elif 'result' in data:
                    raw_projects = data['result']
                elif 'items' in data:
                    raw_projects = data['items']
                else:
                    # If the entire response is the project list
                    raw_projects = data

            # Normalize to list if not already
            if not isinstance(raw_projects, list):
                if isinstance(raw_projects, dict):
                    # If it's a single project object, wrap in a list
                    raw_projects = [raw_projects]
                else:
                    logger.warning(f"Unexpected data format from NPC API: {type(raw_projects)}")
                    return []

            # Process and normalize projects
            processed_projects = []
            for project in raw_projects:
                processed_project = self._normalize_npc_project(project)
                if processed_project:
                    processed_projects.append(processed_project)

            return processed_projects
        elif isinstance(data, list):
            # If the response is directly a list of projects
            processed_projects = []
            for project in data:
                processed_project = self._normalize_npc_project(project)
                if processed_project:
                    processed_projects.append(processed_project)
            return processed_projects
        else:
            logger.warning(f"Received unexpected response type from NPC API: {type(data)}")
            return []

    def _normalize_npc_project(self, project_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a single NPC project to match the world bank project format.

        Args:
            project_data: Raw project data from NPC API or file

        Returns:
            Normalized project data in standard format, or None if invalid
        """
        try:
            # Extract project_id
            project_id = (
                project_data.get("id") or
                project_data.get("project_id") or
                project_data.get("code") or
                project_data.get("projectId", "")
            )

            # Extract title with fallbacks
            title = (
                project_data.get("project_name_in_english") or  # From all_projects.json format
                project_data.get("project_name") or  # Alternative name field
                project_data.get("title") or
                project_data.get("name") or
                project_data.get("projectName", "") or
                ""
            ).strip()

            # Extract description
            description = (
                project_data.get("project_name_in_english") or  # Use project name as description if available
                project_data.get("description", "") or
                project_data.get("details", "") or
                project_data.get("detail", "") or
                project_data.get("summary", "") or
                ""
            )

            # Extract implementing agency
            implementing_agency = (
                project_data.get("ministry", {}).get("name", "") or  # From all_projects.json format
                project_data.get("implementing_agency") or
                project_data.get("implementing_body") or
                project_data.get("executing_agency") or
                project_data.get("implementingAgency", "") or
                project_data.get("executingAgency", "") or
                ""
            )

            # Extract location info
            location_info = project_data.get("locationInfo", {})
            location = {
                "country": "Nepal",
                "country_code": "NP",
                "region": project_data.get("region", location_info.get("region", "")),
                "province": (
                    location_info.get("province") or
                    project_data.get("province") or
                    project_data.get("province_name") or
                    project_data.get("state") or
                    project_data.get("provinceName", "") or
                    project_data.get("stateName", "") or
                    project_data.get("state_name", "") or
                    ""
                ),
                "district": (
                    location_info.get("district") or
                    project_data.get("district") or
                    project_data.get("district_name") or
                    project_data.get("districtName", "") or
                    ""
                ),
                "municipality": (
                    location_info.get("municipality") or
                    project_data.get("municipality") or
                    project_data.get("local_level") or
                    project_data.get("vdc") or
                    project_data.get("municipalityName", "") or
                    project_data.get("localLevel", "") or
                    project_data.get("vdcName", "") or
                    ""
                )
            }

            # Create normalized project
            normalized_project = {
                "project_id": project_id,
                "title": title,
                "description": description,
                "implementing_agency": implementing_agency,
                "start_date": (
                    project_data.get("start_date") or
                    project_data.get("commencement_date") or
                    project_data.get("begin_date") or
                    project_data.get("startDate", "") or
                    project_data.get("commencementDate", "") or
                    ""
                ),
                "end_date": (
                    project_data.get("end_date") or
                    project_data.get("completion_date") or
                    project_data.get("finish_date") or
                    project_data.get("endDate", "") or
                    project_data.get("completionDate", "") or
                    ""
                ),
                "location": location,
                "funding_source": (
                    project_data.get("funding_source") or
                    project_data.get("fundingSource", "") or
                    "Government of Nepal"
                ),
                "total_allocated_budget": str(
                    project_data.get("budget") or
                    project_data.get("allocated_budget") or
                    project_data.get("totalEstimateBudget") or # from all_projects.json stats
                    project_data.get("amount") or
                    project_data.get("totalBudget", "") or
                    project_data.get("allocatedBudget", "") or
                    project_data.get("amount", "") or
                    ""
                ),
                "real_time_spending": str(
                    project_data.get("spending") or
                    project_data.get("expenditure") or
                    project_data.get("realTimeSpending", "") or
                    project_data.get("expenditureAmount", "") or
                    ""
                ),
                "loan_amount": str(
                    project_data.get("loan_amount", "") or
                    project_data.get("loanAmount", "") or
                    ""
                ),
                "grant_amount": str(
                    project_data.get("grant_amount", "") or
                    project_data.get("grantAmount", "") or
                    ""
                ),
                "physical_progress": str(
                    project_data.get("physical_progress", "") or
                    project_data.get("physicalProgress", "") or
                    ""
                ),
                "financial_progress": str(
                    project_data.get("financial_progress", "") or
                    project_data.get("financialProgress", "") or
                    ""
                ),
                "borrower": (
                    project_data.get("borrower") or
                    project_data.get("executing_agency") or
                    project_data.get("borrowerName", "") or
                    ""
                ),
                "sector": (
                    project_data.get("sector") or
                    project_data.get("sector_name") or
                    project_data.get("category") or
                    project_data.get("sectorName", "") or
                    project_data.get("categoryName", "") or
                    project_data.get("sectorType", "") or
                    ""
                ),
                "major_theme": (
                    project_data.get("major_theme") or
                    project_data.get("theme") or
                    project_data.get("majorTheme", "") or
                    project_data.get("themeName", "") or
                    ""
                ),
                "environmental_category": (
                    project_data.get("environmental_category") or
                    project_data.get("eco_category") or
                    project_data.get("environmentalCategory", "") or
                    project_data.get("environmentalRiskCategory", "") or
                    ""
                ),
                "implementation_status": (
                    project_data.get("status") or
                    project_data.get("implementation_status") or
                    project_data.get("current_status") or
                    project_data.get("implementationStatus", "") or
                    project_data.get("currentStatus", "") or
                    project_data.get("projectStatus", "") or
                    ""
                ),
                "url": (
                    project_data.get("url") or
                    project_data.get("project_url") or
                    project_data.get("projectUrl", "") or
                    f"https://npbmis.npc.gov.np/projects/{project_id}" if project_id else ""
                ),
                "project_document_url": (
                    project_data.get("document_url") or
                    project_data.get("docs_url") or
                    project_data.get("documentUrl", "") or
                    project_data.get("documentsUrl", "") or
                    ""
                ),
                "milestones": project_data.get("milestones", project_data.get("projectMilestones", project_data.get("milestone", []))),
                "yearly_budget_breakdown": project_data.get("yearly_budget_breakdown", project_data.get("yearlyBudgetBreakdown", project_data.get("annualBudget", []))),
                "cost_overruns": project_data.get("cost_overruns", project_data.get("costOverruns", project_data.get("budgetOverrun", {}))),
                "reports": project_data.get("reports", project_data.get("projectReports", project_data.get("report", []))),
                "verification_documents": project_data.get("verification_documents", project_data.get("verificationDocuments", project_data.get("auditReports", []))),
                "photos": project_data.get("photos", project_data.get("images", project_data.get("photosList", []))),
                "contractor_change_log": project_data.get("contractor_change_log", project_data.get("contractorChangeLog", project_data.get("contractorHistory", []))),
                "last_updated": datetime.now().isoformat(),
                "source": "NPC (National Planning Commission)",
                "source_api": "NPC API (NPBMIS)"
            }

            # Only return if there's a valid title
            if normalized_project["title"]:
                return normalized_project
            else:
                logger.debug(f"Skipping project with no title: {project_data}")
                return None
        except Exception as e:
            logger.error(f"Error normalizing NPC project: {e}")
            logger.debug(f"Problematic project data: {project_data}")
            return None


async def scrape_and_save_npc_projects(output_file: str = "npc_projects.json") -> int:
    """Scrape or transform NPC projects and save to a JSON file.

    Args:
        output_file: Name of the output file where projects will be saved

    Returns:
        Number of projects scraped and saved
    """
    logger.info("Starting NPC project scraping/transforming...")

    # Define the source directory - this is relative to the project root
    # We want to save to migrations/007-source-projects/source/
    project_root = os.path.join(os.path.dirname(__file__), "..", "..", "..")
    source_dir = os.path.join(project_root, "migrations", "007-source-projects", "source")
    os.makedirs(source_dir, exist_ok=True)

    # Create the full output path
    output_path = os.path.join(source_dir, output_file)

    scraper = NPCProjectScraper()
    projects = await scraper.search_npc_projects()

    # Save projects to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(projects, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(projects)} NPC projects to {output_path}")
    return len(projects)


if __name__ == "__main__":
    # For development and testing
    async def main():
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logger.info("Running NPC project scraper/transformer...")

        # Scrape and save projects
        count = await scrape_and_save_npc_projects()
        logger.info(f"Completed scraping/transformation. Total projects: {count}")

    # Run the scraper
    asyncio.run(main())