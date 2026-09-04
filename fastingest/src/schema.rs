//! serde mirror of `job-ingest/job_schema.py` (Pydantic v2).
//!
//! Invariants that keep the JSON blob columns equivalent to the Python
//! output (`model_dump_json(by_alias=True)` / `model_dump()`):
//! - field order in each struct matches the Pydantic class definition order;
//! - `#[serde(rename = ...)]` reproduces Pydantic aliases in both directions;
//! - `Option` fields serialize as `null` (never skipped), like Pydantic;
//! - no `deny_unknown_fields` — Pydantic ignores extra keys by default.

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Deserializer, Serialize};

/// Pydantic `NullableList`: explicit JSON `null` -> `[]`.
/// (`#[serde(default)]` still handles the key being absent entirely.)
fn null_to_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

/// `board_token: str | int` (and `original_source_id`).
#[derive(Deserialize)]
#[serde(untagged)]
pub enum StrOrInt {
    S(String),
    I(i64),
}

impl StrOrInt {
    /// Matches Python's `str(job.board_token)`.
    pub fn to_string_val(&self) -> String {
        match self {
            Self::S(s) => s.clone(),
            Self::I(i) => i.to_string(),
        }
    }
}

#[derive(Deserialize)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Deserialize, Serialize)]
pub struct JobInformation {
    pub title: String,
    pub job_title_raw: String,
    // Excluded from the ji blob (has its own column), mirroring
    // `model_dump(exclude={"description"})`.
    #[serde(skip_serializing)]
    pub description: String,
    #[serde(rename = "viewedByUsers", default, deserialize_with = "null_to_vec")]
    pub viewed_by_users: Vec<String>,
    #[serde(rename = "savedFromUsers", default, deserialize_with = "null_to_vec")]
    pub saved_from_users: Vec<String>,
    #[serde(rename = "hiddenFromUsers", default, deserialize_with = "null_to_vec")]
    pub hidden_from_users: Vec<String>,
    #[serde(rename = "appliedFromUsers", default, deserialize_with = "null_to_vec")]
    pub applied_from_users: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct V5ProcessedJobData {
    #[serde(default)]
    pub core_job_title: Option<String>,
    #[serde(default)]
    pub requirements_summary: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub technical_tools: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub licenses_or_certifications: Vec<String>,
    #[serde(default)]
    pub licenses_or_certifications_not_mentioned: Option<bool>,

    // Education requirements
    #[serde(default)]
    pub associates_degree_requirement: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub associates_degree_fields_of_study: Vec<String>,
    #[serde(default)]
    pub bachelors_degree_requirement: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub bachelors_degree_fields_of_study: Vec<String>,
    #[serde(default)]
    pub masters_degree_requirement: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub masters_degree_fields_of_study: Vec<String>,
    #[serde(default)]
    pub doctorate_degree_requirement: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub doctorate_degree_fields_of_study: Vec<String>,
    #[serde(default)]
    pub is_high_school_required: Option<bool>,

    // Experience requirements
    #[serde(default)]
    pub min_industry_and_role_yoe: Option<f64>,
    #[serde(default)]
    pub is_min_industry_and_role_yoe_not_mentioned: Option<bool>,
    #[serde(default)]
    pub min_management_and_leadership_yoe: Option<f64>,
    #[serde(default)]
    pub is_min_management_and_leadership_yoe_not_mentioned: Option<bool>,

    // Role classification
    #[serde(default)]
    pub job_category: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub role_activities: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub commitment: Vec<String>,
    #[serde(default)]
    pub role_type: Option<String>,
    #[serde(default)]
    pub seniority_level: Option<String>,
    #[serde(default)]
    pub security_clearance: Option<String>,
    #[serde(default)]
    pub position_employer_type: Option<String>,

    // Workplace / location
    #[serde(default)]
    pub workplace_type: Option<String>,
    #[serde(default)]
    pub workplace_physical_environment: Option<String>,
    #[serde(default)]
    pub formatted_workplace_location: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub workplace_cities: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub workplace_counties: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub workplace_states: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub workplace_countries: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub workplace_continents: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub boundless_workplace_states: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub boundless_workplace_countries: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub boundless_workplace_continents: Vec<String>,
    #[serde(default)]
    pub number_of_workplace_cities: Option<i64>,
    #[serde(default)]
    pub number_of_workplace_counties: Option<i64>,
    #[serde(default)]
    pub number_of_workplace_states: Option<i64>,
    #[serde(default)]
    pub number_of_workplace_countries: Option<i64>,
    #[serde(default)]
    pub number_of_workplace_continents: Option<i64>,
    #[serde(default)]
    pub is_workplace_worldwide_ok: Option<bool>,

    // Physical / schedule demands
    #[serde(default)]
    pub oral_communication_level: Option<String>,
    #[serde(default)]
    pub physical_labor_intensity: Option<String>,
    #[serde(default)]
    pub physical_position: Option<String>,
    #[serde(default)]
    pub computer_usage: Option<String>,
    #[serde(default)]
    pub cognitive_demand: Option<String>,
    #[serde(default)]
    pub air_travel_requirement: Option<String>,
    #[serde(default)]
    pub land_travel_requirement: Option<String>,
    #[serde(default)]
    pub morning_shift_work: Option<String>,
    #[serde(default)]
    pub evening_shift_work: Option<String>,
    #[serde(default)]
    pub overnight_work: Option<String>,
    #[serde(default)]
    pub on_call_requirement: Option<String>,
    #[serde(default)]
    pub weekend_availability_required: Option<bool>,
    #[serde(default)]
    pub holiday_availability_required: Option<bool>,
    #[serde(default)]
    pub overtime_required: Option<bool>,
    #[serde(default)]
    pub is_driver_license_required: Option<bool>,

    // Benefits / perks
    #[serde(default)]
    pub generous_paid_time_off: Option<bool>,
    #[serde(default)]
    pub four_day_work_week: Option<bool>,
    #[serde(default)]
    pub fair_chance: Option<bool>,
    #[serde(default)]
    pub visa_sponsorship: Option<bool>,
    #[serde(default)]
    pub relocation_assistance: Option<bool>,
    #[serde(default)]
    pub military_veterans: Option<bool>,
    #[serde(default)]
    pub tuition_reimbursement: Option<bool>,
    #[serde(default)]
    pub retirement_plan: Option<bool>,
    #[serde(default)]
    pub generous_parental_leave: Option<bool>,
    #[serde(rename = "401k_matching", default)]
    pub field_401k_matching: Option<bool>,

    // Compensation
    #[serde(default)]
    pub yearly_min_compensation: Option<f64>,
    #[serde(default)]
    pub yearly_max_compensation: Option<f64>,
    #[serde(default)]
    pub monthly_min_compensation: Option<f64>,
    #[serde(default)]
    pub monthly_max_compensation: Option<f64>,
    #[serde(default)]
    pub weekly_min_compensation: Option<f64>,
    #[serde(default)]
    pub weekly_max_compensation: Option<f64>,
    #[serde(rename = "bi-weekly_min_compensation", default)]
    pub biweekly_min_compensation: Option<f64>,
    #[serde(rename = "bi-weekly_max_compensation", default)]
    pub biweekly_max_compensation: Option<f64>,
    #[serde(default)]
    pub daily_min_compensation: Option<f64>,
    #[serde(default)]
    pub daily_max_compensation: Option<f64>,
    #[serde(default)]
    pub hourly_min_compensation: Option<f64>,
    #[serde(default)]
    pub hourly_max_compensation: Option<f64>,
    #[serde(default)]
    pub is_compensation_transparent: Option<bool>,
    #[serde(default)]
    pub listed_compensation_currency: Option<String>,
    #[serde(default)]
    pub listed_compensation_frequency: Option<String>,

    // Misc
    #[serde(default, deserialize_with = "null_to_vec")]
    pub language_requirements: Vec<String>,
    #[serde(default)]
    pub num_language_requirements: Option<i64>,
    #[serde(default)]
    pub estimated_publish_date: Option<DateTime<FixedOffset>>,
    #[serde(default)]
    pub estimated_publish_date_millis: Option<i64>,

    // Company info (as extracted from the raw listing, not enrichment)
    #[serde(default)]
    pub company_name: Option<String>,
    #[serde(default)]
    pub company_website: Option<String>,
    #[serde(default)]
    pub company_sector_and_industry: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub company_activities: Vec<String>,
    #[serde(default)]
    pub company_tagline: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub struct EnrichedCompanyData {
    #[serde(default)]
    pub enriched_at: Option<DateTime<FixedOffset>>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub homepage_uri: Option<String>,
    #[serde(default)]
    pub hq_country: Option<String>,
    #[serde(default)]
    pub parent_company: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub subsidiaries: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub industries: Vec<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub activities: Vec<String>,
    #[serde(default)]
    pub nb_employees: Option<i64>,
    #[serde(default)]
    pub year_founded: Option<i64>,
    #[serde(default)]
    pub tagline: Option<String>,
    #[serde(default)]
    pub organization_type: Option<String>,
    #[serde(default, deserialize_with = "null_to_vec")]
    pub latest_funding_investors: Vec<String>,
    #[serde(default)]
    pub latest_funding_type: Option<String>,
    #[serde(default)]
    pub latest_funding_year: Option<i64>,
    #[serde(default)]
    pub latest_funding_amount: Option<f64>,
    #[serde(default)]
    pub stock_exchange: Option<String>,
    #[serde(default)]
    pub stock_symbol: Option<String>,
}

#[derive(Deserialize)]
pub struct Job {
    pub id: String,
    pub board_token: StrOrInt,
    pub source: String,
    pub apply_url: String,
    // Required in the Pydantic schema — kept so its absence still fails
    // validation, even though flatten() never reads it.
    #[allow(dead_code)]
    pub source_and_board_token: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub original_source_id: Option<StrOrInt>,
    pub requisition_id: String,
    pub collapse_key: String,
    pub is_expired: bool,
    #[serde(rename = "objectID")]
    #[allow(dead_code)]
    pub object_id: String,
    pub job_information: JobInformation,
    pub v5_processed_job_data: V5ProcessedJobData,
    #[serde(default)]
    pub enriched_company_data: Option<EnrichedCompanyData>,
    #[serde(rename = "_geoloc", default)]
    pub geoloc: Vec<GeoPoint>,
}

#[derive(Deserialize)]
pub struct PageProps {
    pub job: Job,
}

#[derive(Deserialize)]
pub struct JobPage {
    #[serde(rename = "pageProps")]
    pub page_props: PageProps,
    #[serde(rename = "__N_SSG", alias = "__N_SSP")]
    #[allow(dead_code)]
    pub n_ssg: bool,
}
