# based on https://github.com/google/adk-python/blob/main/src/google/adk/tools/bigquery/metadata_tool.py

from google.auth.credentials import Credentials
from google.cloud import bigquery


def list_dataset_ids(project_id: str):
    client = bigquery.Client(project=project_id)

    datasets = []
    for dataset in client.list_datasets(project_id):
        datasets.append(dataset.dataset_id)
    return datasets


def get_dataset_info(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)
    dataset = client.get_dataset(bigquery.DatasetReference(project_id, dataset_id))
    return dataset


def list_tables(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)
    return client.list_tables(bigquery.DatasetReference(project_id, dataset_id))


def get_table_info(project_id: str, dataset_id: str, table_id: str):
    client = bigquery.Client(project=project_id)
    return client.get_table(
        bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id), table_id
        )
    )


def get_tables(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)
    table_refs = client.list_tables(bigquery.DatasetReference(project_id, dataset_id))
    tables = []
    for t_ref in table_refs:
        tables.append(client.get_table(t_ref))
    return tables


def get_table_info_direct(project_id: str, table_reference):
    client = bigquery.Client(project=project_id)
    return client.get_table(table_reference)


def get_job_info(project_id: str, job_id: str):
    client = bigquery.Client(project=project_id)

    job = client.get_job(job_id)
    # We need to use _properties to get the job info because it contains all
    # the job info.
    # pylint: disable=protected-access
    return job._properties
