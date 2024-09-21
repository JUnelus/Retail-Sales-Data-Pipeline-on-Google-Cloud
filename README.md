# Retail Sales Data Pipeline

This project demonstrates a simple data pipeline on Google Cloud Platform (GCP) that ingests, processes, and visualizes retail sales data using various GCP services like Google Cloud Storage, Dataflow, BigQuery, and Looker Studio.

## Services Used:
- Google Cloud Storage (GCS)
- Google Cloud Dataflow (Apache Beam)
- BigQuery
- Cloud Composer (Airflow)
- Google Looker Studio

## Pipeline Steps:
1. **Ingest Data**: Upload CSV file to GCS.
2. **Process Data**: Use Apache Beam (Dataflow) to transform and filter the data.
3. **Load Data**: Write processed data to BigQuery.
4. **Visualize Data**: Create reports using Looker Studio.

## Running the Project:
1. Clone this repository.
2. Set up GCP services (GCS, Dataflow, BigQuery, Cloud Composer).
3. Run the Airflow DAG to automate the pipeline.
4. Visualize the output using Looker Studio.
