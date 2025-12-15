"""
Migration: 007-source-projects-mof-dfims
Description: Import MoF DFMIS (Ministry of Finance - Development Finance Information Management System) projects for Nepal from scraped JSON data
Author: Nepal Development Project Team
Date: 2025-01-26
"""

import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set

from nes.core.models import (
    Address,
    Attribution,
    ExternalIdentifier,
    LangText,
    LangTextValue,
    Name,
    NameParts,
)
from nes.core.models.base import NameKind
from nes.core.models.entity import EntitySubType, EntityType
from nes.core.models.version import Author
from nes.core.utils.slug_helper import text_to_slug
from nes.services.migration.context import MigrationContext
from nes.services.scraping.normalization import NameExtractor

# Migration metadata
AUTHOR = "Nava Yuwa Central"
DATE = "2025-01-26"
DESCRIPTION = "Import MoF DFMIS projects for Nepal from scraped JSON data"
CHANGE_DESCRIPTION = "Initial sourcing from MoF DFMIS API"

name_extractor = NameExtractor()


def _parse_date(date_input):
    """Parse date from various formats and return a date object."""
    if not date_input:
        return None

    try:
        if isinstance(date_input, str):
            # Replace 'Z' with '+00:00' for proper ISO format parsing
            iso_date = date_input.replace('Z', '+00:00')
            return datetime.fromisoformat(iso_date).date()
        elif hasattr(date_input, 'date'):  # datetime object
            return date_input.date()
        else:
            return date_input
    except Exception:
        return None


def _strip_html_tags(text: str) -> str:
    """Strip HTML tags from text content and return plain text."""
    if not text:
        return text

    import html
    unescaped = html.unescape(text)

    try:
        from html.parser import HTMLParser
    except ImportError:
        from HTMLParser import HTMLParser

    class HTMLStripper(HTMLParser):
        def __init__(self):
            super().__init__()
            self.reset()
            self.fed = []

        def handle_data(self, d):
            self.fed.append(d)

        def get_data(self):
            return ''.join(self.fed)

    stripper = HTMLStripper()
    stripper.feed(unescaped)
    return stripper.get_data().strip()


def _normalize_location_name(name: str) -> str:
    """Normalize location name for matching."""
    s = (name or "").strip().lower()
    if not s:
        return s
    s = s.replace(",", " ")
    s = " ".join(s.split())
    suffixes = [
        "province", "pradesh", "प्रदेश",
        "district", "जिल्ला",
        "metropolitan city", "महानगरपालिका",
        "sub metropolitan city", "sub-metropolitan city", "उपमहानगरपालिका",
        "municipality", "नगरपालिका",
        "rural municipality", "गाउँपालिका",
    ]
    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
            s = " ".join(s.split())
    return s


# Location name aliases for common misspellings
LOCATION_NAME_ALIASES = {
    "sankhuwasava": "sankhuwasabha",
    "panchthar": "pachthar",
    "madesh": "madhesh",
    "sidhupalchowk": "sindhupalchok",
    "kavrepalanchowk": "kavrepalanchok",
    "makawanpur": "makwanpur",
    "chitawan": "chitwan",
    "parbat": "parwat",
    "rukumkot": "eastern rukum",
    "arghakhachi": "arghakhanchi",
    "nawalparasi": "nawalpur",
    "rukum": "western rukum",
    "sudurpashchim": "sudur paschimanchal",
    "achham": "acham",
}


def _map_organization_subtype(
    architecture_name: str = "",
    group_name: str = "",
    org_name: str = "",
    is_donor: bool = False
) -> EntitySubType:
    """
    Map DFMIS organization to appropriate entity subtypes.

    Uses three sources of information:
    1. architecture_name - from DFMIS raw_payload (most reliable for donors)
    2. group_name - from DFMIS raw_payload
    3. org_name - organization name (used for implementing/executing agencies)
    4. is_donor - whether this is a funding organization (affects default)

    Default behavior:
    - Donors without clear classification → INTERNATIONAL_ORG (foreign aid context)
    - Implementing/executing agencies without clear classification → NGO (local implementers)
    """
    arch_lower = (architecture_name or "").strip().lower()
    group_lower = (group_name or "").strip().lower()
    name_lower = (org_name or "").strip().lower()

    # 1. Check architecture name first (most reliable for donors)
    if "government of nepal" in arch_lower:
        return EntitySubType.GOVERNMENT_BODY

    if "non-government" in arch_lower or "non government" in arch_lower:
        if group_lower == "ngo":
            return EntitySubType.NGO
        elif group_lower == "ingo":
            return EntitySubType.INTERNATIONAL_ORG
        return EntitySubType.NGO

    if "multilateral" in arch_lower or "bilateral" in arch_lower:
        return EntitySubType.INTERNATIONAL_ORG

    # 2. Check group name
    if group_lower == "ngo":
        return EntitySubType.NGO
    if group_lower == "ingo":
        return EntitySubType.INTERNATIONAL_ORG
    if "line ministries" in group_lower:
        return EntitySubType.GOVERNMENT_BODY

    # 3. Name-based detection for implementing/executing agencies
    if name_lower:
        # Nepal government bodies
        gov_keywords = [
            "ministry of", "department of", "office of",
            "government of nepal", "nepal government",
            "commission", "secretariat", "authority",
            "council", "board", "bureau",
            # Nepali local government units
            "gaunpalika", "nagarpalika", "mahanagarpalika",
            "rural municipality", "municipality", "metropolitan",
            "district", "province", "pradesh",
        ]
        for kw in gov_keywords:
            if kw in name_lower:
                return EntitySubType.GOVERNMENT_BODY

        # International organizations / bilateral agencies
        intl_keywords = [
            "international", "world bank", "asian development",
            "united nations", "usaid", "jica", "dfid", "giz",
            "european union", "embassy", "cooperation agency",
            "development bank", "monetary fund",
        ]
        for kw in intl_keywords:
            if kw in name_lower:
                return EntitySubType.INTERNATIONAL_ORG

    # 4. Default based on role
    # 4. Default based on role
    # Donors are typically international orgs (foreign aid)
    # Implementing/executing agencies are typically local NGOs
    if is_donor:
        return EntitySubType.INTERNATIONAL_ORG
    else:
        return EntitySubType.NGO


class ProjectMigration:
    """Migration class for DFMIS projects."""

    def __init__(self, context: MigrationContext):
        self.context = context
        self.author_id: str = ""
        self.created_entity_ids: List[str] = []
        self.created_relationship_ids: List[str] = []
        self.organization_cache: Dict[str, str] = {}  # org_name -> entity_id
        self.location_lookup: Dict[str, Any] = {}
        self.province_lookup: Dict[str, Any] = {}
        self.district_lookup: Dict[str, Any] = {}
        self.municipality_lookup: Dict[str, Any] = {}

    async def run(self) -> None:
        """Run the migration."""
        self.context.log("Migration started: Importing MoF DFMIS projects for Nepal")

        try:
            await self._setup_author()
            await self._build_location_lookups()
            await self._build_organization_cache()
            self._load_raw_projects()  # Load raw data for location info
            await self._migrate_projects()
            await self._verify()
            self.context.log("Migration completed successfully")
        except Exception as e:
            self.context.log(f"Migration failed: {e}")
            await self._rollback()
            raise

    def _load_raw_projects(self) -> None:
        """No longer needed - migration metadata is now in the transformed JSONL file."""
        # The scraper now includes _migration_metadata in dfmis_projects.jsonl
        # which contains all agency/location details needed for relationships
        self.context.log("Using migration metadata from transformed JSONL file")

    async def _setup_author(self) -> None:
        """Create the migration author."""
        author = Author(slug=text_to_slug(AUTHOR), name=AUTHOR)
        await self.context.db.put_author(author)
        self.author_id = author.id
        self.context.log(f"Created author: {author.name} ({self.author_id})")

    async def _build_location_lookups(self) -> None:
        """Build lookup dictionaries for locations."""
        locations = await self.context.search.search_entities(
            entity_type="location", limit=10_000
        )

        for loc in locations:
            st = loc.sub_type.value if loc.sub_type else None
            for nm in loc.names:
                if nm.en and nm.en.full:
                    key_full = nm.en.full.strip().lower()
                    key_norm = _normalize_location_name(nm.en.full)
                    self.location_lookup[key_full] = loc
                    self.location_lookup[key_norm] = loc
                    if st == "province":
                        self.province_lookup[key_full] = loc
                        self.province_lookup[key_norm] = loc
                    elif st == "district":
                        self.district_lookup[key_full] = loc
                        self.district_lookup[key_norm] = loc
                    elif st in ["metropolitan_city", "sub_metropolitan_city", "municipality", "rural_municipality"]:
                        self.municipality_lookup[key_full] = loc
                        self.municipality_lookup[key_norm] = loc
                if nm.ne and nm.ne.full:
                    key_ne = nm.ne.full.strip().lower()
                    key_ne_norm = _normalize_location_name(nm.ne.full)
                    self.location_lookup[key_ne] = loc
                    self.location_lookup[key_ne_norm] = loc
                    if st == "province":
                        self.province_lookup[key_ne] = loc
                        self.province_lookup[key_ne_norm] = loc
                    elif st == "district":
                        self.district_lookup[key_ne] = loc
                        self.district_lookup[key_ne_norm] = loc
                    elif st in ["metropolitan_city", "sub_metropolitan_city", "municipality", "rural_municipality"]:
                        self.municipality_lookup[key_ne] = loc
                        self.municipality_lookup[key_ne_norm] = loc

        self.context.log(f"Built location lookups: {len(self.location_lookup)} entries")

    async def _build_organization_cache(self) -> None:
        """Build cache of existing organizations."""
        orgs = await self.context.db.list_entities(
            entity_type="organization", limit=10_000
        )
        for org in orgs:
            for name in org.names:
                if name.en and name.en.full:
                    key = name.en.full.strip().lower()
                    self.organization_cache[key] = org.id
                if name.ne and name.ne.full:
                    key = name.ne.full.strip().lower()
                    self.organization_cache[key] = org.id

        self.context.log(f"Built organization cache: {len(self.organization_cache)} entries")

    async def _migrate_projects(self) -> None:
        """Import projects from the pre-transformed JSONL file."""
        # Load projects from JSONL file
        projects = []
        source_file = self.context.migration_dir / "source" / "dfmis_projects.jsonl"
        try:
            with open(source_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        projects.append(json.loads(line))
            self.context.log(f"Loaded {len(projects)} projects from source/dfmis_projects.jsonl")
        except FileNotFoundError:
            self.context.log("ERROR: source/dfmis_projects.jsonl not found.")
            self.context.log("Please run the scraper first: cd migrations/007-source-projects/mof_dfmis && python scrape_mof_dfmis.py")
            raise

        if not projects:
            self.context.log("WARNING: No projects found in source data.")
            return

        count = 0
        relationship_count = 0

        for project_data in projects:
            try:
                # Create project entity
                project_entity = await self._create_project_entity(project_data)
                if not project_entity:
                    continue

                self.created_entity_ids.append(project_entity.id)
                count += 1

                # Create relationships
                # Debug: verify project_entity.id is a string
                project_id_str = project_entity.id
                if not isinstance(project_id_str, str):
                    self.context.log(f"  ERROR: project_entity.id is not a string: {type(project_id_str)} = {project_id_str}")
                    continue
                rel_count = await self._create_project_relationships(project_id_str, project_data)
                relationship_count += rel_count

                if count % 100 == 0:
                    self.context.log(f"Processed {count} projects...")

            except Exception as e:
                self.context.log(f"Error processing project {project_data.get('slug', 'unknown')}: {e}")
                continue

        self.context.log(f"Created {count} project entities")
        self.context.log(f"Created {relationship_count} relationships")

    async def _create_project_entity(self, project_data: Dict[str, Any]) -> Optional[Any]:
        """Create a project entity from the pre-transformed data."""
        slug = project_data.get("slug", "")
        if not slug:
            return None

        # Ensure slug is valid
        if len(slug) > 100:
            slug = slug[:100]
        if len(slug) < 3:
            slug = f"project-{int(datetime.now().timestamp())}"

        # Build names
        names_data = project_data.get("names", [])
        if not names_data:
            return None

        names = []
        for name_entry in names_data:
            name_obj = Name(
                kind=NameKind(name_entry.get("kind", "PRIMARY")),
                en=NameParts(full=name_entry.get("en", {}).get("full")) if name_entry.get("en") else None,
                ne=NameParts(full=name_entry.get("ne", {}).get("full")) if name_entry.get("ne") else None,
            )
            names.append(name_obj.model_dump())

        # Build description
        description = None
        desc_data = project_data.get("description")
        if desc_data and isinstance(desc_data, dict):
            en_desc = desc_data.get("en", {})
            if en_desc and en_desc.get("value"):
                # Strip HTML and truncate if needed
                clean_desc = _strip_html_tags(en_desc.get("value", ""))
                if clean_desc:
                    description = LangText(
                        en=LangTextValue(value=clean_desc[:5000], provenance="imported")
                    ).model_dump()

        # Build attributions
        attributions = [
            Attribution(
                title=LangText(
                    en=LangTextValue(value="MoF DFMIS", provenance="human")
                ),
                details=LangText(
                    en=LangTextValue(
                        value=f"Imported from Nepal Ministry of Finance DFMIS on {DATE}",
                        provenance="human"
                    )
                ),
            ).model_dump()
        ]

        # Build identifiers
        identifiers = []
        project_url = project_data.get("project_url")
        if project_url:
            # Extract project ID from URL
            project_id = slug.replace("dfmis-", "") if slug.startswith("dfmis-") else slug
            identifiers.append(
                ExternalIdentifier(
                    scheme="other",
                    value=project_id,
                    url=project_url,
                    name=LangText(
                        en=LangTextValue(value="MoF DFMIS Project ID", provenance="human")
                    ),
                ).model_dump()
            )

        # Build entity data
        entity_data = {
            "slug": slug,
            "names": names,
            "attributions": attributions,
            "description": description,
            "identifiers": identifiers if identifiers else None,
            "stage": project_data.get("stage", "unknown"),
            "implementing_agency": project_data.get("implementing_agency"),
            "executing_agency": project_data.get("executing_agency"),
            "financing": project_data.get("financing"),
            "dates": project_data.get("dates"),
            "sectors": project_data.get("sectors"),
            "donors": project_data.get("donors"),
            "donor_extensions": project_data.get("donor_extensions"),
            "project_url": project_url,
        }

        # Remove None values
        entity_data = {k: v for k, v in entity_data.items() if v is not None}

        try:
            project = await self.context.publication.create_entity(
                entity_type=EntityType.PROJECT,
                entity_subtype=EntitySubType.DEVELOPMENT_PROJECT,
                entity_data=entity_data,
                author_id=self.author_id,
                change_description=CHANGE_DESCRIPTION,
            )
            self.context.log(f"Created project {project.id}")
            return project
        except ValueError as e:
            if "already exists" in str(e):
                # Try with a suffix
                i = 2
                while i < 10:
                    entity_data["slug"] = f"{slug}-{i}"
                    try:
                        project = await self.context.publication.create_entity(
                            entity_type=EntityType.PROJECT,
                            entity_subtype=EntitySubType.DEVELOPMENT_PROJECT,
                            entity_data=entity_data,
                            author_id=self.author_id,
                            change_description=CHANGE_DESCRIPTION,
                        )
                        self.context.log(f"Created project {project.id}")
                        return project
                    except ValueError as e2:
                        if "already exists" in str(e2):
                            i += 1
                            continue
                        raise
            raise

    async def _create_project_relationships(self, project_id: str, project_data: Dict[str, Any]) -> int:
        """Create relationships for a project entity using migration metadata."""
        rel_count = 0

        # Get migration metadata from transformed data (includes agency/location details)
        migration_meta = project_data.get("_migration_metadata", {})

        # Helper to create org from agency details
        async def create_org_relationship(
            agencies: List[Dict], 
            rel_type: str, 
            is_donor: bool
        ) -> int:
            count = 0
            for agency in agencies:
                if not isinstance(agency, dict):
                    continue
                org_name = agency.get("name", "")
                if not org_name:
                    continue
                # Build raw_payload from agency metadata
                raw_payload = {
                    "organization__development_cooperation_group__name": agency.get("group", ""),
                    "organization__development_cooperation_group__architecture__name": agency.get("architecture", ""),
                }
                org_id = await self._get_or_create_organization(
                    org_name=org_name,
                    raw_payload=raw_payload,
                    is_donor=is_donor
                )
                if org_id:
                    try:
                        rel = await self.context.publication.create_relationship(
                            source_entity_id=project_id,
                            target_entity_id=org_id,
                            relationship_type=rel_type,
                            author_id=self.author_id,
                            change_description=f"Project {rel_type.lower().replace('_', ' ')} {org_name}",
                        )
                        self.created_relationship_ids.append(rel.id)
                        count += 1
                    except Exception as e:
                        self.context.log(f"  Warning: Could not create {rel_type} relationship: {type(e).__name__}: {e}")
            return count

        # Use migration metadata for relationships (has full org metadata)
        if migration_meta:
            # FUNDED_BY - from development_agencies (donors)
            dev_agencies = migration_meta.get("development_agencies", [])
            rel_count += await create_org_relationship(dev_agencies, "FUNDED_BY", is_donor=True)

            # IMPLEMENTED_BY - from implementing_agencies
            impl_agencies = migration_meta.get("implementing_agencies", [])
            rel_count += await create_org_relationship(impl_agencies, "IMPLEMENTED_BY", is_donor=False)

            # EXECUTED_BY - from executing_agencies
            exec_agencies = migration_meta.get("executing_agencies", [])
            rel_count += await create_org_relationship(exec_agencies, "EXECUTED_BY", is_donor=False)
        else:
            # Fallback to pre-transformed data (no org metadata, uses name heuristics)
            # FUNDED_BY from donor_extensions
            for donor_ext in project_data.get("donor_extensions", []):
                if not isinstance(donor_ext, dict):
                    continue
                donor_name = donor_ext.get("donor", "")
                if not donor_name:
                    continue
                org_id = await self._get_or_create_organization(
                    org_name=donor_name,
                    raw_payload=donor_ext.get("raw_payload", {}),
                    is_donor=True
                )
                if org_id:
                    try:
                        rel = await self.context.publication.create_relationship(
                            source_entity_id=project_id,
                            target_entity_id=org_id,
                            relationship_type="FUNDED_BY",
                            author_id=self.author_id,
                            change_description=f"Project funded by {donor_name}",
                        )
                        self.created_relationship_ids.append(rel.id)
                        rel_count += 1
                    except Exception as e:
                        self.context.log(f"  Warning: Could not create FUNDED_BY relationship: {e}")

            # IMPLEMENTED_BY from implementing_agency string
            for agency_name in (project_data.get("implementing_agency", "") or "").split(","):
                agency_name = agency_name.strip()
                if not agency_name:
                    continue
                org_id = await self._get_or_create_organization(org_name=agency_name, is_donor=False)
                if org_id:
                    try:
                        rel = await self.context.publication.create_relationship(
                            source_entity_id=project_id,
                            target_entity_id=org_id,
                            relationship_type="IMPLEMENTED_BY",
                            author_id=self.author_id,
                            change_description=f"Project implemented by {agency_name}",
                        )
                        self.created_relationship_ids.append(rel.id)
                        rel_count += 1
                    except Exception as e:
                        self.context.log(f"  Warning: Could not create IMPLEMENTED_BY relationship: {e}")

            # EXECUTED_BY from executing_agency string
            for agency_name in (project_data.get("executing_agency", "") or "").split(","):
                agency_name = agency_name.strip()
                if not agency_name:
                    continue
                org_id = await self._get_or_create_organization(org_name=agency_name, is_donor=False)
                if org_id:
                    try:
                        rel = await self.context.publication.create_relationship(
                            source_entity_id=project_id,
                            target_entity_id=org_id,
                            relationship_type="EXECUTED_BY",
                            author_id=self.author_id,
                            change_description=f"Project executed by {agency_name}",
                        )
                        self.created_relationship_ids.append(rel.id)
                        rel_count += 1
                    except Exception as e:
                        self.context.log(f"  Warning: Could not create EXECUTED_BY relationship: {e}")

        # Create LOCATED_IN relationships from migration metadata
        if migration_meta:
            locations = migration_meta.get("locations", [])
            
            # Track which locations we've already linked to avoid duplicates
            linked_location_ids: Set[str] = set()
            
            for loc in locations:
                if not isinstance(loc, dict):
                    continue
                
                # Skip national level projects (no specific location)
                location_type = loc.get("location_type", "")
                if location_type == "National Level":
                    continue
                
                # Try to link to municipality first, then district, then province
                location_id = None
                location_name = None
                
                # Try municipality
                municipality_name = loc.get("municipality")
                if municipality_name:
                    location_id = self._find_location_id(municipality_name, self.municipality_lookup)
                    location_name = municipality_name
                
                # Try district if no municipality match
                if not location_id:
                    district_name = loc.get("district")
                    if district_name:
                        location_id = self._find_location_id(district_name, self.district_lookup)
                        location_name = district_name
                
                # Try province if no district match
                if not location_id:
                    province_name = loc.get("province")
                    if province_name:
                        location_id = self._find_location_id(province_name, self.province_lookup)
                        location_name = province_name
                
                # Create relationship if we found a matching location
                if location_id and location_id not in linked_location_ids:
                    try:
                        rel = await self.context.publication.create_relationship(
                            source_entity_id=project_id,
                            target_entity_id=location_id,
                            relationship_type="LOCATED_IN",
                            author_id=self.author_id,
                            change_description=f"Project located in {location_name}",
                        )
                        self.created_relationship_ids.append(rel.id)
                        linked_location_ids.add(location_id)
                        rel_count += 1
                    except Exception as e:
                        self.context.log(f"  Warning: Could not create LOCATED_IN relationship for {location_name}: {e}")

        return rel_count

    def _find_location_id(self, location_name: str, lookup: Dict[str, Any]) -> Optional[str]:
        """Find location ID from lookup dictionary."""
        if not location_name:
            return None
        
        # Try exact match first
        key_lower = location_name.strip().lower()
        if key_lower in lookup:
            return lookup[key_lower].id
        
        # Try normalized match
        key_norm = _normalize_location_name(location_name)
        if key_norm in lookup:
            return lookup[key_norm].id
        
        # Try with alias
        key_alias = LOCATION_NAME_ALIASES.get(key_norm, key_norm)
        if key_alias in lookup:
            return lookup[key_alias].id
        
        return None

    async def _get_or_create_organization(
        self,
        org_name: str,
        raw_payload: Optional[Dict[str, Any]] = None,
        is_donor: bool = False
    ) -> Optional[str]:
        """Get existing organization or create a new one."""
        if not org_name:
            return None

        # Check cache first
        cache_key = org_name.strip().lower()
        if cache_key in self.organization_cache:
            return self.organization_cache[cache_key]

        # Search for existing organization
        search_results = await self.context.search.search_entities(
            query=org_name,
            entity_type="organization",
            limit=10
        )

        # Look for exact match
        for result in search_results:
            # Check type - handle both string and enum comparison
            result_type = result.type.value if hasattr(result.type, 'value') else str(result.type)
            if result_type != "organization":
                continue
            for name_entry in result.names:
                if name_entry.en and name_entry.en.full:
                    if name_entry.en.full.strip().lower() == cache_key:
                        self.organization_cache[cache_key] = result.id
                        return result.id

        # Create new organization
        slug = text_to_slug(org_name)
        if not slug or len(slug) < 3:
            slug = f"org-{int(datetime.now().timestamp())}"
        if len(slug) > 100:
            slug = slug[:100]

        # Determine subtype from raw payload and/or org name
        arch_name = ""
        group_name = ""
        if raw_payload:
            arch_name = raw_payload.get("organization__development_cooperation_group__architecture__name", "")
            group_name = raw_payload.get("organization__development_cooperation_group__name", "")

        subtype = _map_organization_subtype(arch_name, group_name, org_name, is_donor)

        entity_data = {
            "slug": slug,
            "names": [
                Name(
                    kind=NameKind.PRIMARY,
                    en=NameParts(full=org_name)
                ).model_dump()
            ],
            "attributions": [
                Attribution(
                    title=LangText(
                        en=LangTextValue(value="MoF DFMIS Organization", provenance="human")
                    ),
                    details=LangText(
                        en=LangTextValue(
                            value=f"Organization from MoF DFMIS - {arch_name or 'Development Partner'}",
                            provenance="human"
                        )
                    ),
                ).model_dump()
            ]
        }

        # Add attributes if available
        if arch_name or group_name:
            entity_data["attributes"] = {}
            if arch_name:
                entity_data["attributes"]["architecture"] = arch_name
            if group_name:
                entity_data["attributes"]["group"] = group_name

        try:
            org_entity = await self.context.publication.create_entity(
                entity_type=EntityType.ORGANIZATION,
                entity_subtype=subtype,
                entity_data=entity_data,
                author_id=self.author_id,
                change_description=f"Import organization from MoF DFMIS: {org_name}"
            )
            self.context.log(f"  Created organization: {org_name} ({org_entity.id})")
            self.organization_cache[cache_key] = org_entity.id
            self.created_entity_ids.append(org_entity.id)
            return org_entity.id
        except ValueError as e:
            if "already exists" in str(e):
                # Try with suffix
                i = 2
                while i < 20:
                    entity_data["slug"] = f"{slug}-{i}"
                    try:
                        org_entity = await self.context.publication.create_entity(
                            entity_type=EntityType.ORGANIZATION,
                            entity_subtype=subtype,
                            entity_data=entity_data,
                            author_id=self.author_id,
                            change_description=f"Import organization from MoF DFMIS: {org_name}"
                        )
                        self.context.log(f"  Created organization: {org_name} ({org_entity.id})")
                        self.organization_cache[cache_key] = org_entity.id
                        self.created_entity_ids.append(org_entity.id)
                        return org_entity.id
                    except ValueError as e2:
                        if "already exists" in str(e2):
                            i += 1
                            continue
                        self.context.log(f"  Error creating organization {org_name}: {e2}")
                        return None
            else:
                self.context.log(f"  Error creating organization {org_name}: {e}")
                return None

        return None

    async def _verify(self) -> None:
        """Verify the migration results."""
        projects = await self.context.db.list_entities(
            limit=15_000, entity_type="project", sub_type="development_project"
        )
        self.context.log(f"Verified: {len(projects)} project entities in database")

    async def _rollback(self) -> None:
        """Rollback created entities and relationships on failure."""
        self.context.log("Rolling back migration...")

        # Delete relationships first
        for rel_id in reversed(self.created_relationship_ids):
            try:
                await self.context.publication.delete_relationship(
                    relationship_id=rel_id,
                    author_id=self.author_id,
                    change_description="Rollback: migration failed",
                )
                versions = await self.context.db.list_versions_by_entity(
                    entity_or_relationship_id=rel_id, limit=1000
                )
                for v in versions:
                    await self.context.db.delete_version(v.id)
            except Exception:
                pass

        # Delete entities
        for ent_id in reversed(self.created_entity_ids):
            try:
                await self.context.publication.delete_entity(
                    entity_id=ent_id,
                    author_id=self.author_id,
                    change_description="Rollback: migration failed",
                )
                versions = await self.context.db.list_versions_by_entity(
                    entity_or_relationship_id=ent_id, limit=1000
                )
                for v in versions:
                    await self.context.db.delete_version(v.id)
            except Exception:
                pass

        # Delete author
        try:
            await self.context.db.delete_author(self.author_id)
        except Exception:
            pass

        self.context.log("Rollback completed")


async def migrate(context: MigrationContext) -> None:
    """
    Import MoF DFMIS projects for Nepal from scraped JSON data

    Data source: MoF DFMIS API (dfims.mof.gov.np/api/v2/core/projects/)
    """
    migration = ProjectMigration(context)
    await migration.run()
