//! One validated `JobPage` -> one flat 33-column row. Field order here is the
//! canonical column order, mirrored by `arrow_out.rs` and `sqlite_out.rs`.
//!
//! `Serialize` is derived so the golden tests can snapshot a whole row; the
//! pipeline itself never serializes `FlatRow`.

use crate::schema::JobPage;
use chrono::SecondsFormat;
use serde::Serialize;

#[derive(Serialize)]
pub struct FlatRow {
    pub id: String,
    pub source: String,
    pub board_token: String,
    pub apply_url: String,
    pub requisition_id: String,
    pub collapse_key: String,
    pub is_expired: bool,
    pub title: String,
    pub job_title_raw: String,
    pub description: String,
    pub core_job_title: Option<String>,
    pub job_category: Option<String>,
    pub seniority_level: Option<String>,
    pub role_type: Option<String>,
    pub workplace_type: Option<String>,
    pub formatted_workplace_location: Option<String>,
    pub workplace_countries: String, // JSON-array text
    pub min_industry_and_role_yoe: Option<f64>,
    pub yearly_min_compensation: Option<f64>,
    pub yearly_max_compensation: Option<f64>,
    pub listed_compensation_currency: Option<String>,
    pub technical_tools: String, // JSON-array text
    pub estimated_publish_date: Option<String>,
    pub company_name: Option<String>,
    pub company_website: Option<String>,
    pub enriched_status: Option<String>,
    pub nb_employees: Option<i64>,
    pub year_founded: Option<i64>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub job_information_json: String,
    pub v5_processed_job_data_json: String,
    pub enriched_company_data_json: Option<String>,
}

pub fn flatten(page: JobPage) -> FlatRow {
    let job = page.page_props.job;

    let (lat, lon) = job
        .geoloc
        .first()
        .map_or((None, None), |g| (Some(g.lat), Some(g.lon)));

    // Serialize the blobs before moving fields out of the structs.
    // (`description` is excluded from the ji blob via #[serde(skip_serializing)].)
    let job_information_json = serde_json::to_string(&job.job_information).unwrap();
    let v5_processed_job_data_json = serde_json::to_string(&job.v5_processed_job_data).unwrap();
    let enriched_company_data_json = job
        .enriched_company_data
        .as_ref()
        .map(|ec| serde_json::to_string(ec).unwrap());

    let ji = job.job_information;
    let v5 = job.v5_processed_job_data;
    let ec = job.enriched_company_data;

    FlatRow {
        id: job.id,
        source: job.source,
        board_token: job.board_token.to_string_val(),
        apply_url: job.apply_url,
        requisition_id: job.requisition_id,
        collapse_key: job.collapse_key,
        is_expired: job.is_expired,
        title: ji.title,
        job_title_raw: ji.job_title_raw,
        description: ji.description,
        core_job_title: v5.core_job_title,
        job_category: v5.job_category,
        seniority_level: v5.seniority_level,
        role_type: v5.role_type,
        workplace_type: v5.workplace_type,
        formatted_workplace_location: v5.formatted_workplace_location,
        workplace_countries: serde_json::to_string(&v5.workplace_countries).unwrap(),
        min_industry_and_role_yoe: v5.min_industry_and_role_yoe,
        yearly_min_compensation: v5.yearly_min_compensation,
        yearly_max_compensation: v5.yearly_max_compensation,
        listed_compensation_currency: v5.listed_compensation_currency,
        technical_tools: serde_json::to_string(&v5.technical_tools).unwrap(),
        estimated_publish_date: v5
            .estimated_publish_date
            .map(|d| d.to_rfc3339_opts(SecondsFormat::AutoSi, false)),
        company_name: v5.company_name,
        company_website: v5.company_website,
        enriched_status: ec.as_ref().and_then(|e| e.status.clone()),
        nb_employees: ec.as_ref().and_then(|e| e.nb_employees),
        year_founded: ec.as_ref().and_then(|e| e.year_founded),
        latitude: lat,
        longitude: lon,
        job_information_json,
        v5_processed_job_data_json,
        enriched_company_data_json,
    }
}
