from google.cloud import storage, bigquery
import pandas as pd
import datetime


def load_data_gcs_bq():
    # Finding error csv files from translation-output and load them into a dataframe
    storage_client = storage.Client()
    bucket_name = 'teradata-queries'
    blobs = storage_client.list_blobs(bucket_name)
    csv_file_paths = []
    for blob in blobs:
        if blob.name.find('translation-output') != -1:
            csv_file_paths.append(f"gs://{bucket_name}/{blob.name}")

    df = pd.DataFrame(columns=['InputQuery', 'Line', 'Column', 'Category', 'Message', 'Type',
                               'Error_File', 'Error_File_Location', 'Timestamp'])
    for file in csv_file_paths:
        if file.find('.csv') != -1:
            filename = file.split('/')[-1]
            temp = pd.read_csv(file, encoding='utf-8')
            temp['Error_File'] = filename
            temp['Error_File_Location'] = file
            temp['Timestamp'] = datetime.datetime.now()
            df = pd.concat([df, temp])
    # To remove white spaces
    df = df.applymap(lambda x: str(x).replace("\r", " "))
    df = df.applymap(lambda x: str(x).replace("\n", " "))
    # Loading Data from Dataframe to Big Query table
    client = bigquery.Client()
    table_id = "badri-29apr2022-scrumteam.batch_translation_errors.translation_errors"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
        schema=[
            bigquery.SchemaField("InputQuery", "STRING"),
            bigquery.SchemaField("Line", "INTEGER"),
            bigquery.SchemaField("Column", "INTEGER"),
            bigquery.SchemaField("Category", "STRING"),
            bigquery.SchemaField("Message", "STRING"),
            bigquery.SchemaField("Type", "STRING"),
            bigquery.SchemaField("Error_File", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Error_File_Location", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="NULLABLE"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            df.shape[0], len(table.schema), table_id
        )
    )


if __name__ == '__main__':
    load_data_gcs_bq()
