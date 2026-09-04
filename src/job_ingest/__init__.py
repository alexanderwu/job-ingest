"""Ingest gzipped HiringCafe job listings into SQLite and DuckDB.

The console scripts are wired in pyproject.toml:
    job-ingest  -> job_ingest.ingest_and_benchmark:main
    job-scrape  -> job_ingest.scrape_hiringcafe:app
"""
