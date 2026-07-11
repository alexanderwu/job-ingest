"""
Pydantic schema for the *.json.gz files in cache/json/.

Each file is a gzip-compressed JSON document (a Next.js SSG page-props
payload) describing a single job listing scraped from a job board.

Derived from the observed structure:
    {pageProps: {job: {...}}, __N_SSG: true}

Usage:
    import gzip, json
    from job_schema import JobPage

    with gzip.open(path, "rt") as f:
        page = JobPage.model_validate(json.load(f))
    job = page.pageProps.job
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from typing_extensions import Annotated


def _none_to_empty_list(v):
    """Several list fields are serialized as explicit `null` rather than
    being omitted or set to `[]`. Normalize null -> empty list."""
    return [] if v is None else v


NullableList = Annotated[list[str], BeforeValidator(_none_to_empty_list)]


class GeoPoint(BaseModel):
    """One entry of the job's `_geoloc` array (Algolia-style geo point)."""

    lat: float
    lon: float


class JobInformation(BaseModel):
    """`pageProps.job.job_information` — raw listing content + user activity."""

    title: str
    job_title_raw: str
    description: str

    # User-interaction arrays (Firebase-style UIDs). Any of these may be
    # entirely absent from, or explicitly null in, the source document.
    viewedByUsers: NullableList = Field(default_factory=list)
    savedFromUsers: NullableList = Field(default_factory=list)
    hiddenFromUsers: NullableList = Field(default_factory=list)
    appliedFromUsers: NullableList = Field(default_factory=list)


class V5ProcessedJobData(BaseModel):
    """`pageProps.job.v5_processed_job_data` — LLM-extracted/normalized job attributes."""

    model_config = ConfigDict(populate_by_name=True)

    core_job_title: Optional[str] = None
    requirements_summary: Optional[str] = None
    technical_tools: NullableList = Field(default_factory=list)
    licenses_or_certifications: NullableList = Field(default_factory=list)
    licenses_or_certifications_not_mentioned: Optional[bool] = None

    # Education requirements
    associates_degree_requirement: Optional[str] = None
    associates_degree_fields_of_study: NullableList = Field(default_factory=list)
    bachelors_degree_requirement: Optional[str] = None
    bachelors_degree_fields_of_study: NullableList = Field(default_factory=list)
    masters_degree_requirement: Optional[str] = None
    masters_degree_fields_of_study: NullableList = Field(default_factory=list)
    doctorate_degree_requirement: Optional[str] = None
    doctorate_degree_fields_of_study: NullableList = Field(default_factory=list)
    is_high_school_required: Optional[bool] = None

    # Experience requirements
    min_industry_and_role_yoe: Optional[float] = None
    is_min_industry_and_role_yoe_not_mentioned: Optional[bool] = None
    min_management_and_leadership_yoe: Optional[float] = None
    is_min_management_and_leadership_yoe_not_mentioned: Optional[bool] = None

    # Role classification
    job_category: Optional[str] = None
    role_activities: NullableList = Field(default_factory=list)
    commitment: NullableList = Field(default_factory=list)
    role_type: Optional[str] = None
    seniority_level: Optional[str] = None
    security_clearance: Optional[str] = None
    position_employer_type: Optional[str] = None

    # Workplace / location
    workplace_type: Optional[str] = None
    workplace_physical_environment: Optional[str] = None
    formatted_workplace_location: Optional[str] = None
    workplace_cities: NullableList = Field(default_factory=list)
    workplace_counties: NullableList = Field(default_factory=list)
    workplace_states: NullableList = Field(default_factory=list)
    workplace_countries: NullableList = Field(default_factory=list)
    workplace_continents: NullableList = Field(default_factory=list)
    boundless_workplace_states: NullableList = Field(default_factory=list)
    boundless_workplace_countries: NullableList = Field(default_factory=list)
    boundless_workplace_continents: NullableList = Field(default_factory=list)
    number_of_workplace_cities: Optional[int] = None
    number_of_workplace_counties: Optional[int] = None
    number_of_workplace_states: Optional[int] = None
    number_of_workplace_countries: Optional[int] = None
    number_of_workplace_continents: Optional[int] = None
    is_workplace_worldwide_ok: Optional[bool] = None

    # Physical / schedule demands
    oral_communication_level: Optional[str] = None
    physical_labor_intensity: Optional[str] = None
    physical_position: Optional[str] = None
    computer_usage: Optional[str] = None
    cognitive_demand: Optional[str] = None
    air_travel_requirement: Optional[str] = None
    land_travel_requirement: Optional[str] = None
    morning_shift_work: Optional[str] = None
    evening_shift_work: Optional[str] = None
    overnight_work: Optional[str] = None
    on_call_requirement: Optional[str] = None
    weekend_availability_required: Optional[bool] = None
    holiday_availability_required: Optional[bool] = None
    overtime_required: Optional[bool] = None
    is_driver_license_required: Optional[bool] = None

    # Benefits / perks
    generous_paid_time_off: Optional[bool] = None
    four_day_work_week: Optional[bool] = None
    fair_chance: Optional[bool] = None
    visa_sponsorship: Optional[bool] = None
    relocation_assistance: Optional[bool] = None
    military_veterans: Optional[bool] = None
    tuition_reimbursement: Optional[bool] = None
    retirement_plan: Optional[bool] = None
    generous_parental_leave: Optional[bool] = None
    field_401k_matching: Optional[bool] = Field(default=None, alias="401k_matching")

    # Compensation
    yearly_min_compensation: Optional[float] = None
    yearly_max_compensation: Optional[float] = None
    monthly_min_compensation: Optional[float] = None
    monthly_max_compensation: Optional[float] = None
    weekly_min_compensation: Optional[float] = None
    weekly_max_compensation: Optional[float] = None
    biweekly_min_compensation: Optional[float] = Field(
        default=None, alias="bi-weekly_min_compensation"
    )
    biweekly_max_compensation: Optional[float] = Field(
        default=None, alias="bi-weekly_max_compensation"
    )
    daily_min_compensation: Optional[float] = None
    daily_max_compensation: Optional[float] = None
    hourly_min_compensation: Optional[float] = None
    hourly_max_compensation: Optional[float] = None
    is_compensation_transparent: Optional[bool] = None
    listed_compensation_currency: Optional[str] = None
    listed_compensation_frequency: Optional[str] = None

    # Misc
    language_requirements: NullableList = Field(default_factory=list)
    num_language_requirements: Optional[int] = None
    estimated_publish_date: Optional[datetime] = None
    estimated_publish_date_millis: Optional[int] = None

    # Company info (as extracted from the raw listing, not enrichment)
    company_name: Optional[str] = None
    company_website: Optional[str] = None
    company_sector_and_industry: Optional[str] = None
    company_activities: NullableList = Field(default_factory=list)
    company_tagline: Optional[str] = None


class EnrichedCompanyData(BaseModel):
    """`pageProps.job.enriched_company_data` — third-party company enrichment.

    May be entirely absent from the job record (enrichment never run), or
    present but empty (enrichment ran and found nothing) — every field here
    is optional to cover both cases.
    """

    enriched_at: Optional[datetime] = None
    status: Optional[str] = None  # e.g. "VALID_COMPANY"
    name: Optional[str] = None
    homepage_uri: Optional[str] = None
    hq_country: Optional[str] = None
    parent_company: Optional[str] = None
    subsidiaries: NullableList = Field(default_factory=list)
    industries: NullableList = Field(default_factory=list)
    activities: NullableList = Field(default_factory=list)
    nb_employees: Optional[int] = None
    year_founded: Optional[int] = None
    tagline: Optional[str] = None
    organization_type: Optional[str] = None
    latest_funding_investors: NullableList = Field(default_factory=list)
    latest_funding_type: Optional[str] = None
    latest_funding_year: Optional[int] = None
    latest_funding_amount: Optional[float] = None
    stock_exchange: Optional[str] = None
    stock_symbol: Optional[str] = None


class Job(BaseModel):
    """`pageProps.job` — the core job-listing record."""

    id: str
    board_token: str | int
    source: str
    apply_url: str
    source_and_board_token: str
    original_source_id: Optional[str | int] = None
    requisition_id: str
    collapse_key: str
    is_expired: bool
    objectID: str

    job_information: JobInformation
    v5_processed_job_data: V5ProcessedJobData
    # Absent when company enrichment was never run for this listing.
    enriched_company_data: Optional[EnrichedCompanyData] = None

    # Geo coordinates for the workplace location(s); may be absent.
    geoloc: list[GeoPoint] = Field(default_factory=list, alias="_geoloc")

    model_config = ConfigDict(populate_by_name=True)


class PageProps(BaseModel):
    job: Job


class JobPage(BaseModel):
    """Top-level schema for a single decompressed cache/json/*.json.gz file."""

    pageProps: PageProps
    N_SSG: bool = Field(alias="__N_SSG")

    model_config = ConfigDict(populate_by_name=True)
