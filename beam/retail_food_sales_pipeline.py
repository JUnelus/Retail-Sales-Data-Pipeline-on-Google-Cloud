# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
# import csv
#
# # Define a custom DoFn to process the data
# class TransformData(beam.DoFn):
#     def process(self, element):
#         # Use csv.reader to parse each row in the CSV file
#         record = next(csv.reader([element]))
#
#         # Assuming the CSV has 'region' in the first column and 'sales' in the second column
#         region, sales = record[0], record[1]
#
#         # Filter out rows with missing data
#         if region and sales:
#             try:
#                 # Convert sales to float and yield the result as a dictionary
#                 total_sales = float(sales)
#                 yield {
#                     'region': region,
#                     'total_sales': total_sales
#                 }
#             except ValueError:
#                 # Skip rows where sales cannot be converted to a float
#                 pass
#
# def run():
#     # Set up pipeline options
#     pipeline_options = PipelineOptions()
#
#     # Enable DataflowRunner for production or DirectRunner for local testing
#     google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
#     google_cloud_options.project = 'active-landing-434820-s5'  # Replace with your GCP project ID
#     google_cloud_options.region = 'us-south1'  # Replace with your region, e.g., 'us-central1'
#     google_cloud_options.temp_location = 'gs://ny-retail-food-stores-data-temp-bucket/'  # Replace with your GCS temp location
#     google_cloud_options.staging_location = 'gs://ny-retail-food-stores-data-staging-bucket/'  # Replace with your GCS staging location
#
#     # Set the runner to DataflowRunner for production or DirectRunner for local testing
#     pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'  # Use 'DirectRunner' for local testing
#
#     # Create the pipeline
#     with beam.Pipeline(options=pipeline_options) as pipeline:
#         (
#             pipeline
#             # Read the CSV file from Google Cloud Storage (GCS)
#             | 'Read from GCS' >> beam.io.ReadFromText('gs://ny-retail-food-stores-data-bucket/Retail_Food_Stores.csv')
#
#             # Apply the transformation logic
#             | 'Transform Data' >> beam.ParDo(TransformData())
#
#             # Write the transformed data to BigQuery
#             | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
#                 'active-landing-434820-s5:retail_data.sales_data',  # Change to your BigQuery project, dataset, and table
#                 schema='region:STRING,total_sales:FLOAT',
#                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Replace the table each time
#                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED  # Create the table if it doesn't exist
#             )
#         )
#
# if __name__ == '__main__':
#     run()
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import csv


# Define a custom DoFn to process the data
class TransformData(beam.DoFn):
    def process(self, element):
        # Use csv.reader to parse each row in the CSV file
        record = next(csv.reader([element]))

        # Assuming the CSV has 'region' in the first column and 'sales' in the second column
        region, sales = record[0], record[1]

        # Filter out rows with missing data
        if region and sales:
            try:
                # Convert sales to float and yield the result as a dictionary
                total_sales = float(sales)
                yield {
                    'region': region,
                    'total_sales': total_sales
                }
            except ValueError:
                # Skip rows where sales cannot be converted to a float
                pass


def run():
    # Set up pipeline options
    pipeline_options = PipelineOptions()

    # Enable DataflowRunner for production or DirectRunner for local testing
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'active-landing-434820-s5'  # Replace with your GCP project ID
    google_cloud_options.region = 'us-south1'  # Replace with your region, e.g., 'us-central1'
    google_cloud_options.temp_location = 'gs://ny-retail-food-stores-data-temp-bucket/'  # Replace with your GCS temp location
    google_cloud_options.staging_location = 'gs://ny-retail-food-stores-data-staging-bucket/'  # Replace with your GCS staging location

    # Set the runner to DataflowRunner for production or DirectRunner for local testing
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'  # Use 'DirectRunner' for local testing

    # Verify that GCP credentials are set up correctly
    import os
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        raise EnvironmentError('GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.')

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
                pipeline
                # Read the CSV file from Google Cloud Storage (GCS)
                | 'Read from GCS' >> beam.io.ReadFromText(
            'gs://ny-retail-food-stores-data-bucket/Retail_Food_Stores.csv')

                # Apply the transformation logic
                | 'Transform Data' >> beam.ParDo(TransformData())

                # Write the transformed data to BigQuery
                | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            'active-landing-434820-s5:retail_data.sales_data',  # Change to your BigQuery project, dataset, and table
            schema='region:STRING,total_sales:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Replace the table each time
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED  # Create the table if it doesn't exist
        )
        )


if __name__ == '__main__':
    run()