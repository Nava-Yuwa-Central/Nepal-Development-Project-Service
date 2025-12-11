"""
JICA (Japan International Cooperation Agency) Data Scraper for Nepal Development Projects.

This module provides functionality to transform project data from JICA's loan database
for projects related to Nepal. It follows the existing architecture patterns in the nes 
project and transforms JICA data to match the standardized project schema used by other sources.
"""

import os
import asyncio
import json
import logging
import csv
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from nes.services.scraping.web_scraper import RateLimiter, RetryHandler

# Configure logging
logger = logging.getLogger(__name__)


class JICAAPIClient:
    """HTTP client for JICA data access with rate limiting and retry logic."""

    def __init__(
        self,
        requests_per_second: float = 0.5,  # Conservative rate limit
        requests_per_minute: int = 30,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        """Initialize the JICA API client.

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

    async def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make a request to the JICA API endpoint with rate limiting and error handling.

        Args:
            url: The API endpoint URL
            params: Query parameters for the request

        Returns:
            Response data or None if request fails
        """
        if not self.session:
            raise RuntimeError("Client not initialized. Use within async context manager.")

        # Apply rate limiting
        await self.rate_limiter.acquire("jica.go.jp")

        # Prepare URL with parameters
        if params:
            query_string = urlencode(params)
            full_url = f"{url}?{query_string}"
        else:
            full_url = url

        try:
            # Use browser-like headers to mimic web requests
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
                "Accept": "application/json, text/html, */*",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }

            async with self.session.get(full_url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 401:
                    logger.warning(f"Unauthorized access to {full_url}. Need proper authentication.")
                elif response.status == 403:
                    logger.warning(f"Forbidden access to {full_url}. May require login or special permissions.")
                elif response.status == 404:
                    logger.warning(f"Endpoint not found: {full_url}")

                logger.warning(f"API request failed with status {response.status}: {full_url}")
                logger.warning(f"Response text: {await response.text()}")
                return None
        except asyncio.TimeoutError:
            logger.error(f"Request timeout for URL: {full_url}")
            return None
        except Exception as e:
            logger.error(f"Error making request to {full_url}: {e}")
            return None


class JICAProjectScraper:
    """Scraper for JICA (Japan International Cooperation Agency) projects in Nepal."""

    def __init__(self, client: Optional[JICAAPIClient] = None):
        """Initialize the JICA project scraper.

        Args:
            client: JICAAPIClient instance. If None, a default client will be created
        """
        self.client = client or JICAAPIClient()

    async def search_jica_projects(self) -> List[Dict[str, Any]]:
        """Search for JICA projects related to Nepal.

        Returns:
            List of project data dictionaries
        """
        async with self.client:
            projects = await self._load_jica_projects_from_csv()
            logger.info(f"Successfully loaded and transformed {len(projects)} projects from JICA")
            return projects

    async def _load_jica_projects_from_csv(self) -> List[Dict[str, Any]]:
        """Load JICA projects from the existing CSV file.

        Returns:
            List of project data dictionaries
        """
        try:
            # Try to locate the yen_loan.csv file
            local_paths = [
                "yen_loan.csv",  # Relative to current script
                "../jica/yen_loan.csv",  # One level up
                "migrations/007-source-projects/jica/yen_loan.csv",  # Full path relative to project
                "/Users/interstellarninja/Documents/projects/nyc/Nepal-Development-Project-Service/migrations/007-source-projects/jica/yen_loan.csv",
            ]

            csv_data = None
            file_path = None

            for path in local_paths:
                try:
                    abs_path = os.path.join(os.path.dirname(__file__), path)
                    if os.path.exists(abs_path):
                        file_path = abs_path
                        with open(abs_path, 'r', encoding='utf-8') as f:
                            csv_content = f.read()
                        logger.info(f"Loaded data from local CSV file: {file_path}")
                        
                        # Parse the CSV content
                        lines = csv_content.strip().split('\n')
                        reader = csv.DictReader(lines)
                        csv_data = [dict(row) for row in reader]
                        break
                except Exception as e:
                    logger.debug(f"Could not load from {path}: {e}")
                    continue

            if csv_data is None:
                logger.error("Could not find yen_loan.csv in any of the expected locations")
                return []

            # Transform CSV data to normalized projects
            transformed_projects = []
            for row in csv_data:
                # Skip summary row
                if row.get("No", "").strip() == "":
                    continue
                
                normalized = self._normalize_jica_project(row)
                if normalized:
                    transformed_projects.append(normalized)

            # Filter out any None values
            transformed_projects = [p for p in transformed_projects if p is not None]
            logger.info(f"Successfully transformed {len(transformed_projects)} JICA projects from CSV data")
            return transformed_projects

        except Exception as e:
            logger.error(f"Error loading from CSV file: {e}")
            return []

    def _normalize_jica_project(self, project_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a single JICA project from CSV to match the standardized project format.

        Args:
            project_data: Raw project data from JICA CSV

        Returns:
            Normalized project data in standard format, or None if invalid
        """
        try:
            # Extract project_id - using a combination of fields as JICA doesn't have a single ID field
            project_name = project_data.get("project name", "").strip()
            approval_date = project_data.get("Date of approval(year/month/day)", "").strip()
            
            # Create a unique project ID based on project name and approval date
            if project_name and approval_date:
                import re
                clean_name = re.sub(r'[^\w\s-]', '', project_name).replace(' ', '_')[:50]  # Limit length
                project_id = f"JICA-{clean_name}-{approval_date.replace('-', '')}"
            else:
                project_id = f"JICA-{len(str(project_data))}"  # fallback

            # Extract title with fallbacks
            title = project_name

            # Extract description
            description = f"JICA funded project: {project_data.get('project name', '')}. Sector: {project_data.get('sector', '')}. Subsector: {project_data.get('subsector', '')}"

            # Extract implementing agency
            implementing_agency = (
                project_data.get("Executing agency", "") or
                project_data.get("executing_agency", "") or
                ""
            )

            # Extract location info
            location = {
                "country": "Nepal",
                "country_code": "NP",
                "region": project_data.get("region", ""),
                "province": "",
                "district": "",
                "municipality": ""
            }

            # Extract dates
            start_date = (
                project_data.get("Date of approval(year/month/day)", "") or
                ""
            )
            
            # No explicit end date in the data, but we can calculate it based on repayment period
            end_date = ""
            approval_date_str = project_data.get("Date of approval(year/month/day)", "")
            repayment_period = project_data.get("Main portion Repayment period(years)", "")
            
            if approval_date_str and repayment_period:
                try:
                    approval_date_obj = datetime.strptime(approval_date_str, "%Y-%m-%d")
                    years = int(float(repayment_period))  # Convert string to float first, then to int
                    end_date_obj = approval_date_obj.replace(year=approval_date_obj.year + years)
                    end_date = end_date_obj.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    end_date = ""

            # Extract funding source information
            funding_source = "JICA (Japan International Cooperation Agency)"

            # Extract sector information
            sector = (
                project_data.get("sector", "") or
                project_data.get("subsector", "") or
                ""
            )

            # Extract loan amount and convert to proper format
            loan_amount = project_data.get("Amount of approval(millions; jpy)", "")
            total_allocated_budget = ""
            if loan_amount:
                try:
                    # Convert millions of JPY to JPY
                    amount_val = float(loan_amount) * 1_000_000  # Convert millions to actual amount
                    total_allocated_budget = f"{amount_val:.0f} JPY"
                except ValueError:
                    total_allocated_budget = f"{loan_amount} million JPY"

            # Create normalized project
            normalized_project = {
                "project_id": project_id,
                "title": title,
                "description": description,
                "implementing_agency": implementing_agency,
                "start_date": start_date,
                "end_date": end_date,
                "location": location,
                "funding_source": funding_source,
                "total_allocated_budget": total_allocated_budget,
                "real_time_spending": "",  # Not provided in JICA data
                "loan_amount": f"{project_data.get('Amount of approval(millions; jpy)', '')} million JPY",
                "grant_amount": "",  # JICA data primarily has loans, not grants in this dataset
                "physical_progress": "",  # Not provided in JICA data
                "financial_progress": "",  # Not provided in JICA data
                "borrower": project_data.get("Executing agency", ""),  # Same as implementing agency in this case
                "sector": sector,
                "major_theme": "",  # Not explicitly provided in JICA data
                "environmental_category": "",  # Not provided in JICA data
                "implementation_status": "Implemented",  # All projects in this dataset are implemented
                "url": project_data.get("project url", ""),  # Project URL if available
                "project_document_url": project_data.get("other url", ""),  # Additional documents URL
                "milestones": [
                    {
                        "name": "Project Approval",
                        "date": project_data.get("Date of approval(year/month/day)", ""),
                        "status": "Completed",
                        "description": "Project approved by JICA"
                    }
                ] if project_data.get("Date of approval(year/month/day)", "") else [],
                "yearly_budget_breakdown": [],  # Not provided in a yearly format
                "cost_overruns": {},
                "reports": [
                    {
                        "type": "Ex-ante Evaluation",
                        "url": project_data.get("ex-ante evaluation", ""),
                        "title": "Ex-ante Evaluation Report",
                        "date": ""
                    },
                    {
                        "type": "Ex-post Evaluation", 
                        "url": project_data.get("ex-post evaluation", ""),
                        "title": "Ex-post Evaluation Report",
                        "date": ""
                    }
                ] if project_data.get("ex-ante evaluation", "") or project_data.get("ex-post evaluation", "") else [],
                "verification_documents": [],  # Not explicitly provided in JICA data
                "photos": [],  # Not provided in JICA data
                "contractor_change_log": [],  # Not provided in JICA data
                "last_updated": datetime.now().isoformat(),
                "source": "JICA (Japan International Cooperation Agency)",
                "source_api": "JICA Yen Loan Database (CSV)"
            }

            # Only return if there's a valid title
            if normalized_project["title"]:
                return normalized_project
            else:
                logger.debug(f"Skipping project with no title: {project_data}")
                return None
        except Exception as e:
            logger.error(f"Error normalizing JICA project: {e}")
            logger.debug(f"Problematic project data: {project_data}")
            return None


async def scrape_and_save_jica_projects(output_file: str = "jica_projects.json") -> int:
    """Scrape or transform JICA projects and save to a JSON file.

    Args:
        output_file: Name of the output file where projects will be saved

    Returns:
        Number of projects scraped and saved
    """
    logger.info("Starting JICA project scraping/transforming...")

    # Define the source directory - this is relative to the project root
    # We want to save to migrations/007-source-projects/source/
    project_root = os.path.join(os.path.dirname(__file__), "..", "..", "..")
    source_dir = os.path.join(project_root, "migrations", "007-source-projects", "source")
    os.makedirs(source_dir, exist_ok=True)

    # Create the full output path
    output_path = os.path.join(source_dir, output_file)

    scraper = JICAProjectScraper()
    projects = await scraper.search_jica_projects()

    # Save projects to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(projects, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(projects)} JICA projects to {output_path}")
    return len(projects)


if __name__ == "__main__":
    # For development and testing
    async def main():
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logger.info("Running JICA project scraper/transformer...")

        # Scrape and save projects
        count = await scrape_and_save_jica_projects()
        logger.info(f"Completed scraping/transformation. Total projects: {count}")

    # Run the scraper
    asyncio.run(main())