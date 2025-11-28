"""
Migration: 007-source-projects-world-bank-and-npc
Description: Import World Bank projects and NPC (National Planning Commission) projects for Nepal from scraped JSON data
Author: Nepal Development Project Team
Date: 2025-01-26
"""

from datetime import datetime, timezone
from typing import Dict

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
DESCRIPTION = "Import World Bank and NPC projects for Nepal from scraped JSON data"
CHANGE_DESCRIPTION = "Initial sourcing from World Bank and NPC APIs"

name_extractor = NameExtractor()


def _normalize_location_name(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return s
    s = s.replace(",", " ")
    s = " ".join(s.split())
    suffixes = [
        "province",
        "pradesh",
        "प्रदेश",
        "district",
        "जिल्ला",
        "metropolitan city",
        "महानगरपालिका",
        "sub metropolitan city",
        "sub-metropolitan city",
        "उपमहानगरपालिका",
        "municipality",
        "नगरपालिका",
        "rural municipality",
        "गाउँपालिका",
    ]
    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
            s = " ".join(s.split())
    return s


# Common misspelling/variant aliases (keys and values are normalized)
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


async def migrate(context: MigrationContext) -> None:
    """
    Import World Bank and NPC projects for Nepal from scraped JSON data

    Data source: World Bank APIs (projects.worldbank.org, FinancesOne) and NPC API (npbmis.npc.gov.np)
    """
    context.log("Migration started: Importing World Bank and NPC projects for Nepal")

    # Create author
    author = Author(slug=text_to_slug(AUTHOR), name=AUTHOR)
    await context.db.put_author(author)
    author_id = author.id
    context.log(f"Created author: {author.name} ({author_id})")

    # Process World Bank projects first
    await _migrate_world_bank_projects(context, author_id)

    # Process NPC projects next
    await _migrate_npc_projects(context, author_id)


async def _migrate_world_bank_projects(context: MigrationContext, author_id: str) -> None:
    """
    Import World Bank projects for Nepal from scraped JSON data

    Data source: World Bank APIs (projects.worldbank.org, FinancesOne)
    """
    context.log("Starting: Importing World Bank projects for Nepal")

    # Load projects from pre-scraped data file
    context.log("Loading World Bank projects from source data...")
    try:
        projects = context.read_json("source/world_bank_projects.json")
        context.log(
            f"Loaded {len(projects)} projects from source/world_bank_projects.json"
        )
    except FileNotFoundError:
        context.log("WARNING: source/world_bank_projects.json not found.")
        context.log(
            "Please run: python -m migrations.007-source-projects.scrape_world_bank"
        )
        context.log("to scrape and save World Bank project data first.")
        context.log("Skipping World Bank project import...")
        return

    if not projects:
        context.log(
            "WARNING: No World Bank projects in source data. Skipping import."
        )
        return

    count = 0
    skipped_count = 0
    linked_count = 0
    relationships_count = 0
    created_entity_ids: list[str] = []
    created_relationship_ids: list[str] = []

    import_date = datetime.now(timezone.utc).date()
    attribution_details = f"Imported from World Bank (projects.worldbank.org) on {import_date}"
    attributions = [
        Attribution(
            title=LangText(
                en=LangTextValue(
                    value="World Bank Projects", provenance="human"
                ),
                ne=LangTextValue(value="विश्व बैंक परियोजना", provenance="human"),
            ),
            details=LangText(
                en=LangTextValue(value=attribution_details, provenance="human"),
                ne=LangTextValue(
                    value=f"विश्व बैंक (projects.worldbank.org) बाट {import_date} मा आयात गरिएको",
                    provenance="human",
                ),
            ),
        )
    ]

    locations = await context.search.search_entities(
        entity_type="location", limit=10_000
    )
    location_lookup: Dict[str, object] = {}
    province_lookup: Dict[str, object] = {}
    district_lookup: Dict[str, object] = {}
    municipality_lookup: Dict[str, object] = {}

    for loc in locations:
        st = loc.sub_type.value if loc.sub_type else None
        for nm in loc.names:
            if nm.en and nm.en.full:
                key_full = nm.en.full.strip().lower()
                key_norm = _normalize_location_name(nm.en.full)
                location_lookup[key_full] = loc
                location_lookup[key_norm] = loc
                if st == "province":
                    province_lookup[key_full] = loc
                    province_lookup[key_norm] = loc
                elif st == "district":
                    district_lookup[key_full] = loc
                    district_lookup[key_norm] = loc
                elif st in [
                    "metropolitan_city",
                    "sub_metropolitan_city",
                    "municipality",
                    "rural_municipality",
                ]:
                    municipality_lookup[key_full] = loc
                    municipality_lookup[key_norm] = loc
            if nm.ne and nm.ne.full:
                key_ne = nm.ne.full.strip().lower()
                key_ne_norm = _normalize_location_name(nm.ne.full)
                location_lookup[key_ne] = loc
                location_lookup[key_ne_norm] = loc
                if st == "province":
                    province_lookup[key_ne] = loc
                    province_lookup[key_ne_norm] = loc
                elif st == "district":
                    district_lookup[key_ne] = loc
                    district_lookup[key_ne_norm] = loc
                elif st in [
                    "metropolitan_city",
                    "sub_metropolitan_city",
                    "municipality",
                    "rural_municipality",
                ]:
                    municipality_lookup[key_ne] = loc
                    municipality_lookup[key_ne_norm] = loc

    try:
        for project_data in projects:
            # Extract basic information from World Bank data format
            title = (project_data.get("title") or "").strip()
            if not title:
                skipped_count += 1
                continue

            # Get project details
            project_id = project_data.get("project_id")
            project_description = project_data.get("description", "")

            # Get location information
            location_info = project_data.get("location", {})
            country = location_info.get("country", "")
            country_code = location_info.get("country_code", "")
            region = location_info.get("region", "")
            province_name = location_info.get("province", "")
            district_name = location_info.get("district", "")
            municipality_name = location_info.get("municipality", "")

            # Only process projects that are for Nepal
            if country_code != "NP" and country.lower() != "nepal":
                context.log(f"Skipping project not for Nepal: {title}")
                continue

            # Extract funding information
            funding_source = project_data.get("funding_source", "World Bank")
            total_budget = project_data.get("total_allocated_budget", "")
            spending = project_data.get("real_time_spending", "")
            loan_amount = project_data.get("loan_amount", "")
            grant_amount = project_data.get("grant_amount", "")

            # Extract status and timeline
            status = project_data.get("implementation_status", "")
            start_date = project_data.get("start_date", "")
            end_date = project_data.get("end_date", "")

            # Extract progress information
            physical_progress = project_data.get("physical_progress", "")
            financial_progress = project_data.get("financial_progress", "")

            # Extract implementing agency
            implementing_agency = project_data.get("implementing_agency", "")

            # Extract sector information
            sector = project_data.get("major_theme_sector", "")

            # Extract contact information
            borrower = project_data.get("borrower", "")

            # Build location components
            location_components = []
            if municipality_name:
                location_components.append(municipality_name)
            if district_name and district_name not in location_components:
                location_components.append(district_name)
            if province_name and province_name not in location_components:
                location_components.append(province_name)
            if region and region not in location_components:
                location_components.append(region)

            # Build address text
            address_parts = []
            if municipality_name:
                address_parts.append(municipality_name)
            if district_name:
                address_parts.append(district_name)
            if province_name:
                address_parts.append(province_name)
            if region:
                address_parts.append(region)
            address_text = ", ".join(address_parts)

            # Use the primary location for linking (district or province)
            location_name = district_name or province_name

            # Build names - ensure we always have at least English name
            if not title:
                context.log(f"WARNING: Project has no title, skipping")
                continue

            title_clean = name_extractor.standardize_name(title)

            names = [
                Name(
                    kind=NameKind.PRIMARY,
                    en=NameParts(full=title_clean),
                    ne=NameParts(full=title_clean) if title_clean else None,  # Would need translation
                ).model_dump()
            ]

            # Build identifiers (World Bank project ID)
            identifiers = []
            if project_id:
                identifiers.append(
                    ExternalIdentifier(
                        scheme="other",
                        value=str(project_id),
                        url=project_data.get("url", f"https://projects.worldbank.org/en/projects/{project_id}"),
                        name=LangText(
                            en=LangTextValue(
                                value="World Bank Project ID", provenance="human"
                            ),
                        ),
                    )
                )

            # Build address with location linking using caches
            location_id = None
            location_entity = None
            province_id = None
            province_entity = None

            if province_name:
                p_key_norm = _normalize_location_name(province_name)
                p_key_norm = LOCATION_NAME_ALIASES.get(p_key_norm, p_key_norm)
                p_key_full = province_name.strip().lower()
                pe = province_lookup.get(p_key_norm) or province_lookup.get(p_key_full)
                if pe:
                    province_id = pe.id
                    province_entity = pe

            primary_loc_name = location_name
            if primary_loc_name:
                l_key_norm = _normalize_location_name(primary_loc_name)
                l_key_norm = LOCATION_NAME_ALIASES.get(l_key_norm, l_key_norm)
                l_key_full = primary_loc_name.strip().lower()
                le = district_lookup.get(l_key_norm) or municipality_lookup.get(
                    l_key_norm
                )
                if not le:
                    le = district_lookup.get(l_key_full) or municipality_lookup.get(
                        l_key_full
                    )
                if le:
                    location_id = le.id
                    location_entity = le
                    linked_count += 1

            # Don't raise an error if location is not found - just log a warning and continue
            if primary_loc_name and not location_entity:
                fixed = _normalize_location_name(primary_loc_name)
                context.log(
                    f"  WARNING: Unresolvable location '{primary_loc_name}' (normalized='{fixed}'), "
                    f"skipping location linking for this project"
                )
                # Don't raise an error, just continue with location_id = None

            # Don't raise an error if province is not found - just log a warning and continue
            if province_name and not province_entity:
                fixed = _normalize_location_name(province_name)
                context.log(
                    f"  WARNING: Unresolvable province '{province_name}' (normalized='{fixed}'), "
                    f"skipping province linking for this project"
                )
                # Don't raise an error, just continue with province_id = None

            # Build address description
            address = None
            if (
                address_parts := ([address_text] if address_text else [])
            ):
                # Filter out empty strings
                address_parts_clean = [part for part in address_parts if part]
                if address_parts_clean:
                    address = Address(
                        description2=LangText(
                            en=LangTextValue(
                                value=" / ".join(address_parts_clean),
                                provenance="imported",
                            ),
                            ne=LangTextValue(
                                value=" / ".join(address_parts_clean),
                                provenance="imported",
                            ),
                        ),
                        location_id=location_id,
                    )

            # Build description
            description = None
            description_parts = []
            if sector:
                description_parts.append(f"Sector: {sector}")
            if status:
                description_parts.append(f"Status: {status}")
            if borrower:
                description_parts.append(f"Borrower: {borrower}")

            if description_parts:
                description_text = " | ".join(description_parts)
                description = LangText(
                    en=LangTextValue(value=description_text, provenance="imported"),
                )

            slug_candidate = text_to_slug(title_clean or title)
            if not slug_candidate or len(slug_candidate) < 3:
                if project_id:
                    slug_candidate = f"wb-{text_to_slug(str(project_id))}"
                else:
                    parts = [title_clean or title]
                    parts.append(district_name or province_name)
                    slug_candidate = text_to_slug("-".join([p for p in parts if p]))
                if not slug_candidate or len(slug_candidate) < 3:
                    slug_candidate = f"wb-{text_to_slug(str(project_id or 'unknown'))}"

            # Ensure slug is within 100 character limit
            final_slug = slug_candidate
            if len(final_slug) > 100:
                # Truncate to exactly 100 characters to stay within limit (100-5 for "-trunc" = 95)
                final_slug = final_slug[:95] + "-trunc"  # This creates 100 characters
                if len(final_slug) > 100:
                    final_slug = final_slug[:100]  # Make absolutely sure it's exactly 100 chars

            entity_data = dict(
                slug=final_slug,
                names=names,
                attributions=attributions,
                identifiers=identifiers if identifiers else None,
                description=description.model_dump() if description else None,
            )

            # Build project details (for fields that have specific ProjectDetail fields)
            project_details = {}

            if funding_source:
                project_details["funding_source"] = funding_source

            if total_budget:
                project_details["total_allocated_budget"] = total_budget

            if spending:
                project_details["real_time_spending"] = spending

            if start_date:
                project_details["start_date"] = start_date

            if end_date:
                project_details["end_date"] = end_date

            if physical_progress:
                project_details["physical_progress"] = physical_progress

            if financial_progress:
                project_details["financial_progress"] = financial_progress

            if implementing_agency:
                project_details["implementing_agency"] = implementing_agency

            if sector:
                project_details["sector"] = sector

            if borrower:
                project_details["borrower"] = borrower

            # Add project details if any data exists
            if project_details:
                entity_data["project_details"] = project_details

            # Add status to attributes since it's not part of project_details
            if status:
                if "attributes" not in entity_data:
                    entity_data["attributes"] = {}
                entity_data["attributes"]["status"] = status

            # Address should be handled differently for projects
            # If we need to store address information, add it to attributes
            if address:
                # Use model_dump with exclude to remove deprecated description field
                address_dict = address.model_dump(
                    exclude={"description"}, exclude_none=True
                )
                if address_dict:
                    if "attributes" not in entity_data:
                        entity_data["attributes"] = {}
                    entity_data["attributes"]["address"] = address_dict

            # Build attributes (for additional metadata)
            attributes = {}
            if loan_amount:
                attributes["loan_amount"] = loan_amount
            if grant_amount:
                attributes["grant_amount"] = grant_amount
            if borrower:
                attributes["borrower"] = borrower
            if project_data.get("environmental_category"):
                attributes["environmental_category"] = project_data.get("environmental_category")
            if project_data.get("url"):
                attributes["project_url"] = project_data.get("url")
            if project_data.get("project_document_url"):
                attributes["document_url"] = project_data.get("project_document_url")

            if project_data.get("milestones"):
                attributes["milestones"] = project_data.get("milestones")
            if project_data.get("yearly_budget_breakdown"):
                attributes["yearly_budget_breakdown"] = project_data.get("yearly_budget_breakdown")
            if project_data.get("cost_overruns"):
                attributes["cost_overruns"] = project_data.get("cost_overruns")
            if project_data.get("reports"):
                attributes["reports"] = project_data.get("reports")
            if project_data.get("verification_documents"):
                attributes["verification_documents"] = project_data.get("verification_documents")

            if attributes:
                entity_data["attributes"] = attributes

            base_slug = entity_data["slug"]
            try:
                project_entity = await context.publication.create_entity(
                    entity_type=EntityType.PROJECT,
                    entity_subtype=EntitySubType.DEVELOPMENT_PROJECT,
                    entity_data=entity_data,
                    author_id=author_id,
                    change_description=CHANGE_DESCRIPTION,
                )
            except ValueError as e:
                msg = str(e)
                if "already exists" in msg:
                    i = 2
                    while True:
                        # Ensure the new slug with suffix still fits within 100 character limit
                        temp_slug = f"{base_slug}-{i}"
                        if len(temp_slug) > 100:
                            # Truncate base slug to accommodate suffix
                            max_base_length = 100 - len(f"-{i}")
                            if max_base_length > 5:  # Ensure there's some meaningful slug part
                                truncated_base = base_slug[:max_base_length]
                                entity_data["slug"] = f"{truncated_base}-{i}"
                            else:
                                # If max_base_length is too small, use a generic slug with counter
                                entity_data["slug"] = f"project-{int(datetime.now().timestamp())}-{i}"
                        else:
                            entity_data["slug"] = temp_slug

                        try:
                            project_entity = await context.publication.create_entity(
                                entity_type=EntityType.PROJECT,
                                entity_subtype=EntitySubType.DEVELOPMENT_PROJECT,
                                entity_data=entity_data,
                                author_id=author_id,
                                change_description=CHANGE_DESCRIPTION,
                            )
                            break
                        except ValueError as e2:
                            if "already exists" in str(e2):
                                i += 1
                                continue
                            raise
                else:
                    raise
            context.log(f"Created project {project_entity.id}")
            created_entity_ids.append(project_entity.id)

            # Create LOCATED_IN relationships
            if location_id and location_entity:
                try:
                    location_name_display = (
                        location_entity.names[0].en.full
                        if location_entity.names[0].en
                        else location_name
                    )
                    rel = await context.publication.create_relationship(
                        source_entity_id=project_entity.id,
                        target_entity_id=location_id,
                        relationship_type="LOCATED_IN",
                        author_id=author_id,
                        change_description=f"Project located in {location_name_display}",
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created LOCATED_IN relationship: {project_entity.id} → {location_id}"
                    )
                except Exception as e:
                    context.log(
                        f"  ERROR: Failed to create LOCATED_IN relationship with location: {e}"
                    )
                    # Continue with other relationships even if one fails

            if province_id and province_entity:
                try:
                    province_name_display = (
                        province_entity.names[0].en.full
                        if province_entity.names[0].en
                        else province_name
                    )
                    rel = await context.publication.create_relationship(
                        source_entity_id=project_entity.id,
                        target_entity_id=province_id,
                        relationship_type="LOCATED_IN",
                        author_id=author_id,
                        change_description=f"Project located in {province_name_display}",
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created LOCATED_IN relationship: {project_entity.id} → {province_id}"
                    )
                except Exception as e:
                    context.log(
                        f"  ERROR: Failed to create LOCATED_IN relationship with province: {e}"
                    )
                    # Continue with other relationships even if one fails

            # Create FUNDED_BY relationship with World Bank
            try:
                # Create World Bank organization entity for project funding relationship
                # Use a consistent slug to avoid duplicates
                wb_slug = "world-bank-international-organization"
                wb_entity = None

                # First try to create the entity, handle if it already exists
                try:
                    wb_entity = await context.publication.create_entity(
                        entity_type=EntityType.ORGANIZATION,
                        entity_subtype=EntitySubType.INTERNATIONAL_ORG,
                        entity_data={
                            "slug": wb_slug,
                            "names": [
                                Name(
                                    kind=NameKind.PRIMARY,
                                    en=NameParts(full="World Bank"),
                                    ne=NameParts(full="विश्व बैंक"),
                                ).model_dump()
                            ],
                            "attributions": attributions,
                            "description": LangText(
                                en=LangTextValue(
                                    value="International financial institution that provides loans and grants to developing countries",
                                    provenance="imported"
                                ),
                            ).model_dump(),
                        },
                        author_id=author_id,
                        change_description="World Bank organization entity",
                    )
                    context.log(f"Created World Bank entity {wb_entity.id}")
                except ValueError as e:
                    if "already exists" in str(e):
                        # Entity already exists, we'll skip creating relationship for this project
                        # or could implement lookup logic here
                        context.log(f"World Bank entity already exists, skipping for this project")
                        wb_entity = None  # Set to None so relationship won't be created
                    else:
                        raise e  # Re-raise if it's a different error

                # Only create relationship if we have a World Bank entity
                if wb_entity:
                    # Create AFFILIATED_WITH relationship (since FUNDED_BY is not a valid type)
                    rel = await context.publication.create_relationship(
                        source_entity_id=project_entity.id,
                        target_entity_id=wb_entity.id,
                        relationship_type="AFFILIATED_WITH",
                        author_id=author_id,
                        change_description=f"Funded by World Bank: {funding_source}",
                        attributes={
                            "funding_amount": total_budget,
                            "loan_amount": loan_amount,
                            "grant_amount": grant_amount,
                            "relationship_type": "FUNDED_BY",  # Store original intent in attributes
                        } if total_budget or loan_amount or grant_amount else None,
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created AFFILIATED_WITH relationship: {project_entity.id} → {wb_entity.id}"
                    )
                else:
                    context.log("  Skipped AFFILIATED_WITH relationship: World Bank entity not available")
            except Exception as e:
                context.log(
                    f"  ERROR: Failed to create FUNDED_BY relationship with World Bank: {e}"
                )
                # Continue with other relationships even if one fails

            # Create IMPLEMENTED_BY relationship with implementing agency if available
            if implementing_agency:
                try:
                    # Create or find implementing agency entity
                    agency_slug = text_to_slug(implementing_agency)
                    agency_entity = await context.search.search_entity_by_slug(agency_slug)
                    
                    if not agency_entity:
                        # Create implementing agency entity if it doesn't exist
                        agency_entity = await context.publication.create_entity(
                            entity_type=EntityType.ORGANIZATION,
                            entity_subtype=EntitySubType.GOVERNMENT_AGENCY,
                            entity_data={
                                "slug": agency_slug,
                                "names": [
                                    Name(
                                        kind=NameKind.PRIMARY,
                                        en=NameParts(full=implementing_agency),
                                        ne=NameParts(full=implementing_agency),  # Would need translation
                                    ).model_dump()
                                ],
                                "attributions": attributions,
                            },
                            author_id=author_id,
                            change_description="World Bank project implementing agency",
                        )
                        context.log(f"Created implementing agency entity {agency_entity.id}")
                    
                    # Create AFFILIATED_WITH relationship (since IMPLEMENTS is not a valid type)
                    rel = await context.publication.create_relationship(
                        source_entity_id=agency_entity.id,
                        target_entity_id=project_entity.id,
                        relationship_type="AFFILIATED_WITH",
                        author_id=author_id,
                        change_description=f"Implements World Bank project",
                        attributes={
                            "relationship_type": "IMPLEMENTS",  # Store original intent in attributes
                        }
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created IMPLEMENTS relationship: {agency_entity.id} → {project_entity.id}"
                    )
                except Exception as e:
                    context.log(
                        f"  ERROR: Failed to create IMPLEMENTS relationship with agency: {e}"
                    )
                    # Continue with processing even if relationship creation fails

            count += 1
            if count % 100 == 0:
                context.log(f"Processed {count} projects...")

    except Exception as e:
        context.log(f"ERROR during project migration: {e}")
        raise

    context.log(
        f"Migration completed: {count} projects created, {skipped_count} skipped, "
        f"{linked_count} locations linked, {relationships_count} relationships created"
    )


async def _migrate_npc_projects(context: MigrationContext, author_id: str) -> None:
    """
    Import NPC (National Planning Commission) projects for Nepal from scraped JSON data

    Data source: Nepal Planning Database Management Information System (NPBMIS) - npbmis.npc.gov.np
    """
    context.log("Starting: Importing NPC projects for Nepal")

    # Load projects from pre-scraped data file
    context.log("Loading NPC projects from source data...")
    try:
        projects = context.read_json("source/npc_projects.json")
        context.log(
            f"Loaded {len(projects)} projects from source/npc_projects.json"
        )
    except FileNotFoundError:
        context.log("WARNING: source/npc_projects.json not found.")
        context.log(
            "Please run: python -m migrations.007-source-projects.nepal_project_bank.scrape_npc_projects"
        )
        context.log("to scrape and save NPC project data first.")
        context.log("Skipping NPC project import...")
        return

    if not projects:
        context.log(
            "WARNING: No NPC projects in source data. Skipping import."
        )
        return

    count = 0
    skipped_count = 0
    linked_count = 0
    relationships_count = 0
    created_entity_ids: list[str] = []
    created_relationship_ids: list[str] = []

    import_date = datetime.now(timezone.utc).date()
    attribution_details = f"Imported from NPC (npbmis.npc.gov.np) on {import_date}"
    attributions = [
        Attribution(
            title=LangText(
                en=LangTextValue(
                    value="NPC Projects", provenance="human"
                ),
                ne=LangTextValue(value="एनपीसी परियोजना", provenance="human"),
            ),
            details=LangText(
                en=LangTextValue(value=attribution_details, provenance="human"),
                ne=LangTextValue(
                    value=f"राष्ट्रिय योजना आयोग (npbmis.npc.gov.np) बाट {import_date} मा आयात गरिएको",
                    provenance="human",
                ),
            ),
        )
    ]

    # Get all locations for lookup (we can reuse the same lookup as World Bank)
    locations = await context.search.search_entities(
        entity_type="location", limit=10_000
    )
    location_lookup: Dict[str, object] = {}
    province_lookup: Dict[str, object] = {}
    district_lookup: Dict[str, object] = {}
    municipality_lookup: Dict[str, object] = {}

    for loc in locations:
        st = loc.sub_type.value if loc.sub_type else None
        for nm in loc.names:
            if nm.en and nm.en.full:
                key_full = nm.en.full.strip().lower()
                key_norm = _normalize_location_name(nm.en.full)
                location_lookup[key_full] = loc
                location_lookup[key_norm] = loc
                if st == "province":
                    province_lookup[key_full] = loc
                    province_lookup[key_norm] = loc
                elif st == "district":
                    district_lookup[key_full] = loc
                    district_lookup[key_norm] = loc
                elif st in [
                    "metropolitan_city",
                    "sub_metropolitan_city",
                    "municipality",
                    "rural_municipality",
                ]:
                    municipality_lookup[key_full] = loc
                    municipality_lookup[key_norm] = loc
            if nm.ne and nm.ne.full:
                key_ne = nm.ne.full.strip().lower()
                key_ne_norm = _normalize_location_name(nm.ne.full)
                location_lookup[key_ne] = loc
                location_lookup[key_ne_norm] = loc
                if st == "province":
                    province_lookup[key_ne] = loc
                    province_lookup[key_ne_norm] = loc
                elif st == "district":
                    district_lookup[key_ne] = loc
                    district_lookup[key_ne_norm] = loc
                elif st in [
                    "metropolitan_city",
                    "sub_metropolitan_city",
                    "municipality",
                    "rural_municipality",
                ]:
                    municipality_lookup[key_ne] = loc
                    municipality_lookup[key_ne_norm] = loc

    try:
        for project_data in projects:
            # Extract basic information from NPC data format
            title = (project_data.get("title") or "").strip()
            if not title:
                skipped_count += 1
                continue

            # Get project details
            project_id = project_data.get("project_id")
            project_description = project_data.get("description", "")

            # Get location information
            location_info = project_data.get("location", {})
            country = location_info.get("country", "")
            country_code = location_info.get("country_code", "")
            region = location_info.get("region", "")
            province_name = location_info.get("province", "")
            district_name = location_info.get("district", "")
            municipality_name = location_info.get("municipality", "")

            # Only process projects that are for Nepal
            if country_code != "NP" and country.lower() != "nepal":
                context.log(f"Skipping project not for Nepal: {title}")
                continue

            # Extract funding information
            funding_source = project_data.get("funding_source", "Government of Nepal")
            total_budget = project_data.get("total_allocated_budget", "")
            spending = project_data.get("real_time_spending", "")
            loan_amount = project_data.get("loan_amount", "")
            grant_amount = project_data.get("grant_amount", "")

            # Extract status and timeline
            status = project_data.get("implementation_status", "")
            start_date = project_data.get("start_date", "")
            end_date = project_data.get("end_date", "")

            # Extract progress information
            physical_progress = project_data.get("physical_progress", "")
            financial_progress = project_data.get("financial_progress", "")

            # Extract implementing agency
            implementing_agency = project_data.get("implementing_agency", "")

            # Extract sector information
            sector = project_data.get("sector", "")
            major_theme = project_data.get("major_theme", "")

            # Extract contact information
            borrower = project_data.get("borrower", "")

            # Build location components
            location_components = []
            if municipality_name:
                location_components.append(municipality_name)
            if district_name and district_name not in location_components:
                location_components.append(district_name)
            if province_name and province_name not in location_components:
                location_components.append(province_name)
            if region and region not in location_components:
                location_components.append(region)

            # Build address text
            address_parts = []
            if municipality_name:
                address_parts.append(municipality_name)
            if district_name:
                address_parts.append(district_name)
            if province_name:
                address_parts.append(province_name)
            if region:
                address_parts.append(region)
            address_text = ", ".join(address_parts)

            # Use the primary location for linking (district or province)
            location_name = district_name or province_name

            # Build names - ensure we always have at least English name
            if not title:
                context.log(f"WARNING: Project has no title, skipping")
                continue

            title_clean = name_extractor.standardize_name(title)

            names = [
                Name(
                    kind=NameKind.PRIMARY,
                    en=NameParts(full=title_clean),
                    ne=NameParts(full=title_clean) if title_clean else None,  # Would need translation
                ).model_dump()
            ]

            # Build identifiers (NPC project ID)
            identifiers = []
            if project_id:
                identifiers.append(
                    ExternalIdentifier(
                        scheme="other",
                        value=str(project_id),
                        url=project_data.get("url", f"https://npbmis.npc.gov.np/projects/{project_id}"),
                        name=LangText(
                            en=LangTextValue(
                                value="NPC Project ID", provenance="human"
                            ),
                        ),
                    )
                )

            # Build address with location linking using caches
            location_id = None
            location_entity = None
            province_id = None
            province_entity = None

            if province_name:
                p_key_norm = _normalize_location_name(province_name)
                p_key_norm = LOCATION_NAME_ALIASES.get(p_key_norm, p_key_norm)
                p_key_full = province_name.strip().lower()
                pe = province_lookup.get(p_key_norm) or province_lookup.get(p_key_full)
                if pe:
                    province_id = pe.id
                    province_entity = pe

            primary_loc_name = location_name
            if primary_loc_name:
                l_key_norm = _normalize_location_name(primary_loc_name)
                l_key_norm = LOCATION_NAME_ALIASES.get(l_key_norm, l_key_norm)
                l_key_full = primary_loc_name.strip().lower()
                le = district_lookup.get(l_key_norm) or municipality_lookup.get(
                    l_key_norm
                )
                if not le:
                    le = district_lookup.get(l_key_full) or municipality_lookup.get(
                        l_key_full
                    )
                if le:
                    location_id = le.id
                    location_entity = le
                    linked_count += 1

            # Don't raise an error if location is not found - just log a warning and continue
            if primary_loc_name and not location_entity:
                fixed = _normalize_location_name(primary_loc_name)
                context.log(
                    f"  WARNING: Unresolvable location '{primary_loc_name}' (normalized='{fixed}'), "
                    f"skipping location linking for this project"
                )
                # Don't raise an error, just continue with location_id = None

            # Don't raise an error if province is not found - just log a warning and continue
            if province_name and not province_entity:
                fixed = _normalize_location_name(province_name)
                context.log(
                    f"  WARNING: Unresolvable province '{province_name}' (normalized='{fixed}'), "
                    f"skipping province linking for this project"
                )
                # Don't raise an error, just continue with province_id = None

            # Build address description
            address = None
            if (
                address_parts := ([address_text] if address_text else [])
            ):
                # Filter out empty strings
                address_parts_clean = [part for part in address_parts if part]
                if address_parts_clean:
                    address = Address(
                        description2=LangText(
                            en=LangTextValue(
                                value=" / ".join(address_parts_clean),
                                provenance="imported",
                            ),
                            ne=LangTextValue(
                                value=" / ".join(address_parts_clean),
                                provenance="imported",
                            ),
                        ),
                        location_id=location_id,
                    )

            # Build description
            description = None
            description_parts = []
            if sector:
                description_parts.append(f"Sector: {sector}")
            if status:
                description_parts.append(f"Status: {status}")
            if borrower:
                description_parts.append(f"Borrower: {borrower}")

            if description_parts:
                description_text = " | ".join(description_parts)
                description = LangText(
                    en=LangTextValue(value=description_text, provenance="imported"),
                )

            slug_candidate = text_to_slug(title_clean or title)
            if not slug_candidate or len(slug_candidate) < 3:
                if project_id:
                    slug_candidate = f"npc-{text_to_slug(str(project_id))}"
                else:
                    parts = [title_clean or title]
                    parts.append(district_name or province_name)
                    slug_candidate = text_to_slug("-".join([p for p in parts if p]))
                if not slug_candidate or len(slug_candidate) < 3:
                    slug_candidate = f"npc-{text_to_slug(str(project_id or 'unknown'))}"

            # Ensure slug is within 100 character limit
            final_slug = slug_candidate
            if len(final_slug) > 100:
                # Truncate to exactly 100 characters to stay within limit
                final_slug = final_slug[:95] + "-trunc"
                if len(final_slug) > 100:
                    final_slug = final_slug[:100]

            entity_data = dict(
                slug=final_slug,
                names=names,
                attributions=attributions,
                identifiers=identifiers if identifiers else None,
                description=description.model_dump() if description else None,
            )

            # Build project details (for fields that have specific ProjectDetail fields)
            project_details = {}

            if funding_source:
                project_details["funding_source"] = funding_source

            if total_budget:
                project_details["total_allocated_budget"] = total_budget

            if spending:
                project_details["real_time_spending"] = spending

            if start_date:
                project_details["start_date"] = start_date

            if end_date:
                project_details["end_date"] = end_date

            if physical_progress:
                project_details["physical_progress"] = physical_progress

            if financial_progress:
                project_details["financial_progress"] = financial_progress

            if implementing_agency:
                project_details["implementing_agency"] = implementing_agency

            if sector:
                project_details["sector"] = sector

            if major_theme:
                project_details["major_theme"] = major_theme

            if borrower:
                project_details["borrower"] = borrower

            # Add project details if any data exists
            if project_details:
                entity_data["project_details"] = project_details

            # Add status to attributes since it's not part of project_details
            if status:
                if "attributes" not in entity_data:
                    entity_data["attributes"] = {}
                entity_data["attributes"]["status"] = status

            # Address should be handled differently for projects
            # If we need to store address information, add it to attributes
            if address:
                # Use model_dump with exclude to remove deprecated description field
                address_dict = address.model_dump(
                    exclude={"description"}, exclude_none=True
                )
                if address_dict:
                    if "attributes" not in entity_data:
                        entity_data["attributes"] = {}
                    entity_data["attributes"]["address"] = address_dict

            # Build attributes (for additional metadata)
            attributes = {}
            if loan_amount:
                attributes["loan_amount"] = loan_amount
            if grant_amount:
                attributes["grant_amount"] = grant_amount
            if borrower:
                attributes["borrower"] = borrower
            if project_data.get("environmental_category"):
                attributes["environmental_category"] = project_data.get("environmental_category")
            if project_data.get("url"):
                attributes["project_url"] = project_data.get("url")
            if project_data.get("project_document_url"):
                attributes["document_url"] = project_data.get("project_document_url")

            if project_data.get("milestones"):
                attributes["milestones"] = project_data.get("milestones")
            if project_data.get("yearly_budget_breakdown"):
                attributes["yearly_budget_breakdown"] = project_data.get("yearly_budget_breakdown")
            if project_data.get("cost_overruns"):
                attributes["cost_overruns"] = project_data.get("cost_overruns")
            if project_data.get("reports"):
                attributes["reports"] = project_data.get("reports")
            if project_data.get("verification_documents"):
                attributes["verification_documents"] = project_data.get("verification_documents")

            if attributes:
                entity_data["attributes"] = attributes

            base_slug = entity_data["slug"]
            try:
                project_entity = await context.publication.create_entity(
                    entity_type=EntityType.PROJECT,
                    entity_subtype=EntitySubType.DEVELOPMENT_PROJECT,
                    entity_data=entity_data,
                    author_id=author_id,
                    change_description=CHANGE_DESCRIPTION,
                )
            except ValueError as e:
                msg = str(e)
                if "already exists" in msg:
                    i = 2
                    while True:
                        # Ensure the new slug with suffix still fits within 100 character limit
                        temp_slug = f"{base_slug}-{i}"
                        if len(temp_slug) > 100:
                            # Truncate base slug to accommodate suffix
                            max_base_length = 100 - len(f"-{i}")
                            if max_base_length > 5:  # Ensure there's some meaningful slug part
                                truncated_base = base_slug[:max_base_length]
                                entity_data["slug"] = f"{truncated_base}-{i}"
                            else:
                                # If max_base_length is too small, use a generic slug with counter
                                entity_data["slug"] = f"project-{int(datetime.now().timestamp())}-{i}"
                        else:
                            entity_data["slug"] = temp_slug

                        try:
                            project_entity = await context.publication.create_entity(
                                entity_type=EntityType.PROJECT,
                                entity_subtype=EntitySubType.DEVELOPMENT_PROJECT,
                                entity_data=entity_data,
                                author_id=author_id,
                                change_description=CHANGE_DESCRIPTION,
                            )
                            break
                        except ValueError as e2:
                            if "already exists" in str(e2):
                                i += 1
                                continue
                            raise
                else:
                    raise
            context.log(f"Created NPC project {project_entity.id}")
            created_entity_ids.append(project_entity.id)

            # Create LOCATED_IN relationships
            if location_id and location_entity:
                try:
                    location_name_display = (
                        location_entity.names[0].en.full
                        if location_entity.names[0].en
                        else location_name
                    )
                    rel = await context.publication.create_relationship(
                        source_entity_id=project_entity.id,
                        target_entity_id=location_id,
                        relationship_type="LOCATED_IN",
                        author_id=author_id,
                        change_description=f"Project located in {location_name_display}",
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created LOCATED_IN relationship: {project_entity.id} → {location_id}"
                    )
                except Exception as e:
                    context.log(
                        f"  ERROR: Failed to create LOCATED_IN relationship with location: {e}"
                    )
                    # Continue with other relationships even if one fails

            if province_id and province_entity:
                try:
                    province_name_display = (
                        province_entity.names[0].en.full
                        if province_entity.names[0].en
                        else province_name
                    )
                    rel = await context.publication.create_relationship(
                        source_entity_id=project_entity.id,
                        target_entity_id=province_id,
                        relationship_type="LOCATED_IN",
                        author_id=author_id,
                        change_description=f"Project located in {province_name_display}",
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created LOCATED_IN relationship: {project_entity.id} → {province_id}"
                    )
                except Exception as e:
                    context.log(
                        f"  ERROR: Failed to create LOCATED_IN relationship with province: {e}"
                    )
                    # Continue with other relationships even if one fails

            # Create FUNDED_BY relationship with Government of Nepal/related agency
            try:
                # Create Government of Nepal organization entity for project funding relationship
                # Use a consistent slug to avoid duplicates
                govn_slug = "government-of-nepal"
                govn_entity = None

                # First try to create the entity, handle if it already exists
                try:
                    govn_entity = await context.publication.create_entity(
                        entity_type=EntityType.ORGANIZATION,
                        entity_subtype=EntitySubType.GOVERNMENT_AGENCY,
                        entity_data={
                            "slug": govn_slug,
                            "names": [
                                Name(
                                    kind=NameKind.PRIMARY,
                                    en=NameParts(full="Government of Nepal"),
                                    ne=NameParts(full="नेपाल सरकार"),
                                ).model_dump()
                            ],
                            "attributions": attributions,
                            "description": LangText(
                                en=LangTextValue(
                                    value="Government of Nepal - implementing domestic development projects",
                                    provenance="imported"
                                ),
                            ).model_dump(),
                        },
                        author_id=author_id,
                        change_description="Government of Nepal organization entity",
                    )
                    context.log(f"Created Government of Nepal entity {govn_entity.id}")
                except ValueError as e:
                    if "already exists" in str(e):
                        # Entity already exists, we'll skip creating relationship for this project
                        # Instead, search for the existing entity
                        govn_entity = await context.search.search_entity_by_slug(govn_slug)
                        context.log(f"Government of Nepal entity already exists, using existing entity {govn_entity.id if govn_entity else 'None'}")
                    else:
                        raise e  # Re-raise if it's a different error

                # Only create relationship if we have a Government of Nepal entity
                if govn_entity:
                    # Create AFFILIATED_WITH relationship (since FUNDED_BY is not a valid type)
                    rel = await context.publication.create_relationship(
                        source_entity_id=project_entity.id,
                        target_entity_id=govn_entity.id,
                        relationship_type="AFFILIATED_WITH",
                        author_id=author_id,
                        change_description=f"Funded by Government of Nepal: {funding_source}",
                        attributes={
                            "funding_amount": total_budget,
                            "loan_amount": loan_amount,
                            "grant_amount": grant_amount,
                            "relationship_type": "FUNDED_BY",  # Store original intent in attributes
                        } if total_budget or loan_amount or grant_amount else None,
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created AFFILIATED_WITH relationship: {project_entity.id} → {govn_entity.id}"
                    )
                else:
                    context.log("  Skipped AFFILIATED_WITH relationship: Government of Nepal entity not available")
            except Exception as e:
                context.log(
                    f"  ERROR: Failed to create FUNDED_BY relationship with Government of Nepal: {e}"
                )
                # Continue with other relationships even if one fails

            # Create IMPLEMENTED_BY relationship with implementing agency if available
            if implementing_agency:
                try:
                    # Create or find implementing agency entity
                    agency_slug = text_to_slug(implementing_agency)
                    agency_entity = await context.search.search_entity_by_slug(agency_slug)

                    if not agency_entity:
                        # Create implementing agency entity if it doesn't exist
                        agency_entity = await context.publication.create_entity(
                            entity_type=EntityType.ORGANIZATION,
                            entity_subtype=EntitySubType.GOVERNMENT_AGENCY,
                            entity_data={
                                "slug": agency_slug,
                                "names": [
                                    Name(
                                        kind=NameKind.PRIMARY,
                                        en=NameParts(full=implementing_agency),
                                        ne=NameParts(full=implementing_agency),  # Would need translation
                                    ).model_dump()
                                ],
                                "attributions": attributions,
                            },
                            author_id=author_id,
                            change_description="NPC project implementing agency",
                        )
                        context.log(f"Created implementing agency entity {agency_entity.id}")

                    # Create AFFILIATED_WITH relationship (since IMPLEMENTS is not a valid type)
                    rel = await context.publication.create_relationship(
                        source_entity_id=agency_entity.id,
                        target_entity_id=project_entity.id,
                        relationship_type="AFFILIATED_WITH",
                        author_id=author_id,
                        change_description=f"Implements NPC project",
                        attributes={
                            "relationship_type": "IMPLEMENTS",  # Store original intent in attributes
                        }
                    )
                    relationships_count += 1
                    created_relationship_ids.append(rel.id)
                    context.log(
                        f"  Created IMPLEMENTS relationship: {agency_entity.id} → {project_entity.id}"
                    )
                except Exception as e:
                    context.log(
                        f"  ERROR: Failed to create IMPLEMENTS relationship with agency: {e}"
                    )
                    # Continue with processing even if relationship creation fails

            count += 1
            if count % 100 == 0:
                context.log(f"Processed {count} NPC projects...")

    except Exception as e:
        context.log(f"ERROR during NPC project migration: {e}")
        raise

    context.log(
        f"NPC migration completed: {count} projects created, {skipped_count} skipped, "
        f"{linked_count} locations linked, {relationships_count} relationships created"
    )