import polling
from google.cloud import bigquery_migration_v2alpha as bm

from load_from_gcs_bq import load_data_gcs_bq


def translation_job():
    project_id = "badri-29apr2022-scrumteam"
    location = "us"
    input_path = 'gs://teradata-queries/teradata-queries'
    output_path = "gs://teradata-queries/translation-output"
    if project_id == '':
        print("Empty projectID specified, please provide a valid project ID")
    if location == "":
        print("Empty location specified, please provide a valid location")
    if input_path == "":
        print("Empty input_path specified, please provide a valid cloud storage path")
    if output_path == "":
        print("Empty output_path specified, please provide a valid cloud storage path")
    workflow = create_batch_translation_workflow(project_id, location, input_path, output_path)
    display_job_translation_status(workflow)


def poll_job(response):
    return response.state == 4


def create_batch_translation_workflow(project_id, location, input_path, output_path):
    parent = 'projects/' + project_id + '/locations/' + location

    # Create a client
    client = bm.MigrationServiceClient()

    # Initialize request argument(s)
    translation_task_details = bm.TranslationTaskDetails(
        input_path=input_path,
        output_path=output_path
    )
    task = bm.MigrationTask(
        translation_task_details=translation_task_details,
        type_='Translation_Teradata'
    )
    migration_workflow = bm.MigrationWorkflow(
        display_name='teradata-bq-batch-translation-job',
        tasks={'tasks': task}
    )
    request = bm.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=migration_workflow
    )
    # Make the request
    response = client.create_migration_workflow(request=request)

    # Poll the workflow until the job complted
    result = polling.poll(lambda: client.get_migration_workflow(
        request=bm.types.migration_service.GetMigrationWorkflowRequest(
            name=response.name)),
                          step=10,
                          poll_forever=True,
                          check_success=poll_job)
    return result


def display_job_translation_status(workflow):
    print(f'workflow details : {workflow}')
    # Display the response
    print(f'Migration workflow {workflow.name} completed with state {str(workflow.state)}')
    #Read the error files from output_path in GCS and load them into BigQuery table
    load_data_gcs_bq()

if __name__ == '__main__':
    translation_job()
