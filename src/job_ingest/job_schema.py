"""
msgspec schema for the *.json.gz files in data/raw/json/.

Each file is a gzip-compressed JSON document (a Next.js SSG page-props
payload) describing a single job listing scraped from a job board.

Derived from the observed structure:
    {pageProps: {job: {...}}, __N_SSG: true}

This module is the Python source of truth shared by ingest_and_benchmark.py
(the default msgspec engine) and verify_parity.py (the Rust-parity check):
the Struct definitions, the 33-column COLUMNS order, and flatten() all live
here. fastingest/src/schema.rs mirrors these Structs field-for-field and in
the same order, so the JSON blob columns produced by both engines carry
identical key order.

Usage:
    import gzip
    from job_schema import decode_page

    with open(path, "rb") as f:
        page = decode_page(gzip.decompress(f.read()))
    job = page.pageProps.job

Notes vs the old Pydantic version:
    - decode+validate happen in one msgspec C pass (no separate json.loads);
    - msgspec is strict like serde: no "5" -> 5.0 coercion, datetimes must
      be RFC 3339 with an offset (this corpus is clean under both);
    - NullableList (explicit JSON `null` -> []) is normalized in
      __post_init__ instead of a BeforeValidator.
"""

import typing
from datetime import datetime
from typing import Optional, Union

import msgspec

# Several list fields are serialized as explicit `null` rather than being
# omitted or set to `[]`; _Struct.__post_init__ normalizes null -> [].
NullableList = Optional[list[str]]

_NULL_LIST_FIELDS: dict[type, tuple[str, ...]] = {}


class _Struct(msgspec.Struct, kw_only=True):
    """Base Struct: kw_only so defaulted fields can keep the exact Pydantic
    declaration order (which fixes blob key order), plus null->[] cleanup."""

    def __post_init__(self):
        cls = type(self)
        fields = _NULL_LIST_FIELDS.get(cls)
        if fields is None:
            hints = typing.get_type_hints(cls)
            fields = tuple(n for n, t in hints.items() if t == NullableList)
            _NULL_LIST_FIELDS[cls] = fields
        for name in fields:
            if getattr(self, name) is None:
                setattr(self, name, [])


class GeoPoint(_Struct, kw_only=True):
    """One entry of the job's `_geoloc` array (Algolia-style geo point)."""

    lat: float
    lon: float


class JobInformation(_Struct, kw_only=True):
    """`pageProps.job.job_information` — raw listing content + user activity."""

    title: str
    job_title_raw: str
    description: str

    # User-interaction arrays (Firebase-style UIDs). Any of these may be
    # entirely absent from, or explicitly null in, the source document.
    viewedByUsers: NullableList = None
    savedFromUsers: NullableList = None
    hiddenFromUsers: NullableList = None
    appliedFromUsers: NullableList = None


class V5ProcessedJobData(_Struct, kw_only=True):
    """`pageProps.job.v5_processed_job_data` — LLM-extracted/normalized job attributes."""

    core_job_title: Optional[str] = None
    requirements_summary: Optional[str] = None
    technical_tools: NullableList = None
    licenses_or_certifications: NullableList = None
    licenses_or_certifications_not_mentioned: Optional[bool] = None

    # Education requirements
    associates_degree_requirement: Optional[str] = None
    associates_degree_fields_of_study: NullableList = None
    bachelors_degree_requirement: Optional[str] = None
    bachelors_degree_fields_of_study: NullableList = None
    masters_degree_requirement: Optional[str] = None
    masters_degree_fields_of_study: NullableList = None
    doctorate_degree_requirement: Optional[str] = None
    doctorate_degree_fields_of_study: NullableList = None
    is_high_school_required: Optional[bool] = None

    # Experience requirements
    min_industry_and_role_yoe: Optional[float] = None
    is_min_industry_and_role_yoe_not_mentioned: Optional[bool] = None
    min_management_and_leadership_yoe: Optional[float] = None
    is_min_management_and_leadership_yoe_not_mentioned: Optional[bool] = None

    # Role classification
    job_category: Optional[str] = None
    role_activities: NullableList = None
    commitment: NullableList = None
    role_type: Optional[str] = None
    seniority_level: Optional[str] = None
    security_clearance: Optional[str] = None
    position_employer_type: Optional[str] = None

    # Workplace / location
    workplace_type: Optional[str] = None
    workplace_physical_environment: Optional[str] = None
    formatted_workplace_location: Optional[str] = None
    workplace_cities: NullableList = None
    workplace_counties: NullableList = None
    workplace_states: NullableList = None
    workplace_countries: NullableList = None
    workplace_continents: NullableList = None
    boundless_workplace_states: NullableList = None
    boundless_workplace_countries: NullableList = None
    boundless_workplace_continents: NullableList = None
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
    field_401k_matching: Optional[bool] = msgspec.field(
        default=None, name="401k_matching"
    )

    # Compensation
    yearly_min_compensation: Optional[float] = None
    yearly_max_compensation: Optional[float] = None
    monthly_min_compensation: Optional[float] = None
    monthly_max_compensation: Optional[float] = None
    weekly_min_compensation: Optional[float] = None
    weekly_max_compensation: Optional[float] = None
    biweekly_min_compensation: Optional[float] = msgspec.field(
        default=None, name="bi-weekly_min_compensation"
    )
    biweekly_max_compensation: Optional[float] = msgspec.field(
        default=None, name="bi-weekly_max_compensation"
    )
    daily_min_compensation: Optional[float] = None
    daily_max_compensation: Optional[float] = None
    hourly_min_compensation: Optional[float] = None
    hourly_max_compensation: Optional[float] = None
    is_compensation_transparent: Optional[bool] = None
    listed_compensation_currency: Optional[str] = None
    listed_compensation_frequency: Optional[str] = None

    # Misc
    language_requirements: NullableList = None
    num_language_requirements: Optional[int] = None
    estimated_publish_date: Optional[datetime] = None
    estimated_publish_date_millis: Optional[int] = None

    # Company info (as extracted from the raw listing, not enrichment)
    company_name: Optional[str] = None
    company_website: Optional[str] = None
    company_sector_and_industry: Optional[str] = None
    company_activities: NullableList = None
    company_tagline: Optional[str] = None


class EnrichedCompanyData(_Struct, kw_only=True):
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
    subsidiaries: NullableList = None
    industries: NullableList = None
    activities: NullableList = None
    nb_employees: Optional[int] = None
    year_founded: Optional[int] = None
    tagline: Optional[str] = None
    organization_type: Optional[str] = None
    latest_funding_investors: NullableList = None
    latest_funding_type: Optional[str] = None
    latest_funding_year: Optional[int] = None
    latest_funding_amount: Optional[float] = None
    stock_exchange: Optional[str] = None
    stock_symbol: Optional[str] = None


class Job(_Struct, kw_only=True):
    """`pageProps.job` — the core job-listing record."""

    id: str
    board_token: Union[str, int]
    source: str
    apply_url: str
    source_and_board_token: str
    original_source_id: Optional[Union[str, int]] = None
    requisition_id: str
    collapse_key: str
    is_expired: bool
    objectID: str

    job_information: JobInformation
    v5_processed_job_data: V5ProcessedJobData
    # Absent when company enrichment was never run for this listing.
    enriched_company_data: Optional[EnrichedCompanyData] = None

    # Geo coordinates for the workplace location(s); may be absent.
    geoloc: list[GeoPoint] = msgspec.field(default=[], name="_geoloc")


class PageProps(_Struct, kw_only=True):
    job: Job


class JobPage(_Struct, kw_only=True):
    """Top-level schema for a single decompressed data/raw/json/*.json.gz file."""

    pageProps: PageProps
    N_SSG: bool = msgspec.field(name="__N_SSG")


_decoder = msgspec.json.Decoder(JobPage)
_encoder = msgspec.json.Encoder()


def decode_page(raw: bytes | str) -> JobPage:
    """Parse + validate one decompressed JSON document in a single pass."""
    return _decoder.decode(raw)


# Flat 33-column row layout shared by both DBs, the Arrow handoff, and the
# parity checker. Order matters everywhere.
COLUMNS = [
    "id",
    "source",
    "board_token",
    "apply_url",
    "requisition_id",
    "collapse_key",
    "is_expired",
    "title",
    "job_title_raw",
    "description",
    "core_job_title",
    "job_category",
    "seniority_level",
    "role_type",
    "workplace_type",
    "formatted_workplace_location",
    "workplace_countries",
    "min_industry_and_role_yoe",
    "yearly_min_compensation",
    "yearly_max_compensation",
    "listed_compensation_currency",
    "technical_tools",
    "estimated_publish_date",
    "company_name",
    "company_website",
    "enriched_status",
    "nb_employees",
    "year_founded",
    "latitude",
    "longitude",
    "job_information_json",
    "v5_processed_job_data_json",
    "enriched_company_data_json",
]


def flatten(page: JobPage) -> tuple:
    """One validated JobPage -> one flat row in COLUMNS order."""
    job = page.pageProps.job
    ji = job.job_information
    v5 = job.v5_processed_job_data
    ec = job.enriched_company_data

    lat = job.geoloc[0].lat if job.geoloc else None
    lon = job.geoloc[0].lon if job.geoloc else None

    # `description` has its own column, so it's excluded from the ji blob
    # (mirrors #[serde(skip_serializing)] in the Rust schema).
    ji_dict = msgspec.to_builtins(ji)
    del ji_dict["description"]

    return (
        job.id,
        job.source,
        str(job.board_token),
        job.apply_url,
        job.requisition_id,
        job.collapse_key,
        job.is_expired,
        ji.title,
        ji.job_title_raw,
        ji.description,
        v5.core_job_title,
        v5.job_category,
        v5.seniority_level,
        v5.role_type,
        v5.workplace_type,
        v5.formatted_workplace_location,
        _encoder.encode(v5.workplace_countries).decode(),
        v5.min_industry_and_role_yoe,
        v5.yearly_min_compensation,
        v5.yearly_max_compensation,
        v5.listed_compensation_currency,
        _encoder.encode(v5.technical_tools).decode(),
        v5.estimated_publish_date.isoformat() if v5.estimated_publish_date else None,
        v5.company_name,
        v5.company_website,
        ec.status if ec else None,
        ec.nb_employees if ec else None,
        ec.year_founded if ec else None,
        lat,
        lon,
        _encoder.encode(ji_dict).decode(),
        _encoder.encode(v5).decode(),
        _encoder.encode(ec).decode() if ec is not None else None,
    )
