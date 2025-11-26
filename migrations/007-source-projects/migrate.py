"""
Migration: 007-source-projects-world-bank
Description: Import World Bank projects for Nepal from scraped JSON data
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
from nes.core.utils.slug_helper import text_to_slug
from nes.services.migration.context import MigrationContext
from nes.services.scraping.normalization import NameExtractor

# Migration metadata
AUTHOR = "Nepal Development Project Team"
DATE = "2025-01-26"
DESCRIPTION = "Import World Bank projects for Nepal from scraped JSON data"
CHANGE_DESCRIPTION = "Initial sourcing from World Bank APIs"

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
    Import World Bank projects for Nepal from scraped JSON data

    Data source: World Bank APIs (projects.worldbank.org, FinancesOne)
    """
    context.log("Migration started: Importing World Bank projects for Nepal")

    # Create author
    author = Author(slug=text_to_slug(AUTHOR), name=AUTHOR)
    await context.db.put_author(author)
    author_id = author.id
    context.log(f"Created author: {author.name} ({author_id})")

    # Load projects from pre-scraped data file
    context.log("Loading projects from source data...")
    try:
        projects = context.read_json("007-source-projects/source/world_bank_projects.json")
        context.log(
            f"Loaded {len(projects)} projects from 007-source-projects/source/world_bank_projects.json"
        )
    except FileNotFoundError:
        context.log("ERROR: 007-source-projects/source/world_bank_projects.json not found.")
        context.log(
            "Please run: python -m migrations.007-source-projects.scrape_world_bank"
        )
        context.log("to scrape and save World Bank project data first.")
        raise

    if not projects:
        context.log(
            "WARNING: No projects in source data. Migration may be incomplete."
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
                        scheme="world_bank",
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

            if primary_loc_name and not location_entity:
                fixed = _normalize_location_name(primary_loc_name)
                raise ValueError(
                    f"Unresolvable location '{primary_loc_name}' (normalized='{fixed}')"
                )

            if province_name and not province_entity:
                fixed = _normalize_location_name(province_name)
                raise ValueError(
                    f"Unresolvable province '{province_name}' (normalized='{fixed}')"
                )

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

            entity_data = dict(
                slug=slug_candidate,
                names=names,
                attributions=attributions,
                identifiers=identifiers if identifiers else None,
                description=description.model_dump() if description else None,
            )

            # Add Project-specific fields (only if not None)
            if funding_source:
                entity_data["funding_source"] = funding_source

            if total_budget:
                entity_data["total_allocated_budget"] = total_budget

            if spending:
                entity_data["real_time_spending"] = spending

            if status:
                entity_data["status"] = status

            if start_date:
                entity_data["start_date"] = start_date

            if end_date:
                entity_data["end_date"] = end_date

            if physical_progress:
                entity_data["physical_progress"] = physical_progress

            if financial_progress:
                entity_data["financial_progress"] = financial_progress

            if implementing_agency:
                entity_data["implementing_agency"] = implementing_agency

            if sector:
                entity_data["sector"] = sector

            if borrower:
                entity_data["borrower"] = borrower

            # Address - only add if it exists and has valid data
            if address:
                # Use model_dump with exclude to remove deprecated description field
                address_dict = address.model_dump(
                    exclude={"description"}, exclude_none=True
                )
                if address_dict:
                    entity_data["address"] = address_dict

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
                        entity_data["slug"] = f"{base_slug}-{i}"
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
                # Create or find World Bank organization entity
                wb_slug = text_to_slug("World Bank")
                wb_entity = await context.search.search_entity_by_slug(wb_slug)
                
                if not wb_entity:
                    # Create World Bank entity if it doesn't exist
                    wb_entity = await context.publication.create_entity(
                        entity_type=EntityType.ORGANIZATION,
                        entity_subtype=EntitySubType.INTERNATIONAL_ORGANIZATION,
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
                
                # Create FUNDED_BY relationship
                rel = await context.publication.create_relationship(
                    source_entity_id=project_entity.id,
                    target_entity_id=wb_entity.id,
                    relationship_type="FUNDED_BY",
                    author_id=author_id,
                    change_description=f"Funded by World Bank: {funding_source}",
                    attributes={
                        "funding_amount": total_budget,
                        "loan_amount": loan_amount,
                        "grant_amount": grant_amount,
                    } if total_budget or loan_amount or grant_amount else None,
                )
                relationships_count += 1
                created_relationship_ids.append(rel.id)
                context.log(
                    f"  Created FUNDED_BY relationship: {project_entity.id} → {wb_entity.id}"
                )
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
                    
                    # Create IMPLEMENTED_BY relationship
                    rel = await context.publication.create_relationship(
                        source_entity_id=agency_entity.id,
                        target_entity_id=project_entity.id,
                        relationship_type="IMPLEMENTS",
                        author_id=author_id,
                        change_description=f"Implements World Bank project",
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
        context.log(f"ERROR during project migration: {e}", error=True)
        raise

    context.log(
        f"Migration completed: {count} projects created, {skipped_count} skipped, "
        f"{linked_count} locations linked, {relationships_count} relationships created"
    )