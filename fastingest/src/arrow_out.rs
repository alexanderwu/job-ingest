//! Build a 33-column RecordBatch from the flattened rows and write it as an
//! Arrow IPC (Feather v2) file. Field names/order must match `COLUMNS` in
//! `ingest_and_benchmark.py`.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use crate::flatten::FlatRow;

pub fn write_ipc(rows: &[FlatRow], out: &Path) -> Result<(), ArrowError> {
    let mut id = StringBuilder::new();
    let mut source = StringBuilder::new();
    let mut board_token = StringBuilder::new();
    let mut apply_url = StringBuilder::new();
    let mut requisition_id = StringBuilder::new();
    let mut collapse_key = StringBuilder::new();
    let mut is_expired = BooleanBuilder::new();
    let mut title = StringBuilder::new();
    let mut job_title_raw = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut core_job_title = StringBuilder::new();
    let mut job_category = StringBuilder::new();
    let mut seniority_level = StringBuilder::new();
    let mut role_type = StringBuilder::new();
    let mut workplace_type = StringBuilder::new();
    let mut formatted_workplace_location = StringBuilder::new();
    let mut workplace_countries = StringBuilder::new();
    let mut min_industry_and_role_yoe = Float64Builder::new();
    let mut yearly_min_compensation = Float64Builder::new();
    let mut yearly_max_compensation = Float64Builder::new();
    let mut listed_compensation_currency = StringBuilder::new();
    let mut technical_tools = StringBuilder::new();
    let mut estimated_publish_date = StringBuilder::new();
    let mut company_name = StringBuilder::new();
    let mut company_website = StringBuilder::new();
    let mut enriched_status = StringBuilder::new();
    let mut nb_employees = Int64Builder::new();
    let mut year_founded = Int64Builder::new();
    let mut latitude = Float64Builder::new();
    let mut longitude = Float64Builder::new();
    let mut job_information_json = StringBuilder::new();
    let mut v5_processed_job_data_json = StringBuilder::new();
    let mut enriched_company_data_json = StringBuilder::new();

    for r in rows {
        id.append_value(&r.id);
        source.append_value(&r.source);
        board_token.append_value(&r.board_token);
        apply_url.append_value(&r.apply_url);
        requisition_id.append_value(&r.requisition_id);
        collapse_key.append_value(&r.collapse_key);
        is_expired.append_value(r.is_expired);
        title.append_value(&r.title);
        job_title_raw.append_value(&r.job_title_raw);
        description.append_value(&r.description);
        core_job_title.append_option(r.core_job_title.as_deref());
        job_category.append_option(r.job_category.as_deref());
        seniority_level.append_option(r.seniority_level.as_deref());
        role_type.append_option(r.role_type.as_deref());
        workplace_type.append_option(r.workplace_type.as_deref());
        formatted_workplace_location.append_option(r.formatted_workplace_location.as_deref());
        workplace_countries.append_value(&r.workplace_countries);
        min_industry_and_role_yoe.append_option(r.min_industry_and_role_yoe);
        yearly_min_compensation.append_option(r.yearly_min_compensation);
        yearly_max_compensation.append_option(r.yearly_max_compensation);
        listed_compensation_currency.append_option(r.listed_compensation_currency.as_deref());
        technical_tools.append_value(&r.technical_tools);
        estimated_publish_date.append_option(r.estimated_publish_date.as_deref());
        company_name.append_option(r.company_name.as_deref());
        company_website.append_option(r.company_website.as_deref());
        enriched_status.append_option(r.enriched_status.as_deref());
        nb_employees.append_option(r.nb_employees);
        year_founded.append_option(r.year_founded);
        latitude.append_option(r.latitude);
        longitude.append_option(r.longitude);
        job_information_json.append_value(&r.job_information_json);
        v5_processed_job_data_json.append_value(&r.v5_processed_job_data_json);
        enriched_company_data_json.append_option(r.enriched_company_data_json.as_deref());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("board_token", DataType::Utf8, false),
        Field::new("apply_url", DataType::Utf8, false),
        Field::new("requisition_id", DataType::Utf8, false),
        Field::new("collapse_key", DataType::Utf8, false),
        Field::new("is_expired", DataType::Boolean, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("job_title_raw", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("core_job_title", DataType::Utf8, true),
        Field::new("job_category", DataType::Utf8, true),
        Field::new("seniority_level", DataType::Utf8, true),
        Field::new("role_type", DataType::Utf8, true),
        Field::new("workplace_type", DataType::Utf8, true),
        Field::new("formatted_workplace_location", DataType::Utf8, true),
        Field::new("workplace_countries", DataType::Utf8, false),
        Field::new("min_industry_and_role_yoe", DataType::Float64, true),
        Field::new("yearly_min_compensation", DataType::Float64, true),
        Field::new("yearly_max_compensation", DataType::Float64, true),
        Field::new("listed_compensation_currency", DataType::Utf8, true),
        Field::new("technical_tools", DataType::Utf8, false),
        Field::new("estimated_publish_date", DataType::Utf8, true),
        Field::new("company_name", DataType::Utf8, true),
        Field::new("company_website", DataType::Utf8, true),
        Field::new("enriched_status", DataType::Utf8, true),
        Field::new("nb_employees", DataType::Int64, true),
        Field::new("year_founded", DataType::Int64, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("job_information_json", DataType::Utf8, false),
        Field::new("v5_processed_job_data_json", DataType::Utf8, false),
        Field::new("enriched_company_data_json", DataType::Utf8, true),
    ]));

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(source.finish()),
        Arc::new(board_token.finish()),
        Arc::new(apply_url.finish()),
        Arc::new(requisition_id.finish()),
        Arc::new(collapse_key.finish()),
        Arc::new(is_expired.finish()),
        Arc::new(title.finish()),
        Arc::new(job_title_raw.finish()),
        Arc::new(description.finish()),
        Arc::new(core_job_title.finish()),
        Arc::new(job_category.finish()),
        Arc::new(seniority_level.finish()),
        Arc::new(role_type.finish()),
        Arc::new(workplace_type.finish()),
        Arc::new(formatted_workplace_location.finish()),
        Arc::new(workplace_countries.finish()),
        Arc::new(min_industry_and_role_yoe.finish()),
        Arc::new(yearly_min_compensation.finish()),
        Arc::new(yearly_max_compensation.finish()),
        Arc::new(listed_compensation_currency.finish()),
        Arc::new(technical_tools.finish()),
        Arc::new(estimated_publish_date.finish()),
        Arc::new(company_name.finish()),
        Arc::new(company_website.finish()),
        Arc::new(enriched_status.finish()),
        Arc::new(nb_employees.finish()),
        Arc::new(year_founded.finish()),
        Arc::new(latitude.finish()),
        Arc::new(longitude.finish()),
        Arc::new(job_information_json.finish()),
        Arc::new(v5_processed_job_data_json.finish()),
        Arc::new(enriched_company_data_json.finish()),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let file = File::create(out).map_err(ArrowError::from)?;
    let mut writer = FileWriter::try_new(BufWriter::new(file), &schema)?;
    writer.write(&batch)?;
    writer.finish()
}
