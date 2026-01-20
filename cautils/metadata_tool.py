# based on https://github.com/google/adk-python/blob/main/src/google/adk/tools/bigquery/metadata_tool.py
import json
from pathlib import Path
from google.cloud import bigquery
from typing import List

import yaml


def list_dataset_ids(project_id: str) -> list[str]:
    """List BigQuery dataset ids in a Google Cloud project.

    Args:
        project_id (str): The Google Cloud project id.
    Returns:
        list[str]: List of the BigQuery dataset ids present in the project.
    """
    client = bigquery.Client(project=project_id)

    datasets = []
    for dataset in client.list_datasets(project_id):
        datasets.append(dataset.dataset_id)
    return datasets


def get_dataset_info(project_id: str, dataset_id: str):
    """Get metadata information about a BigQuery dataset.

    Args:
        project_id (str): The Google Cloud project id containing the dataset.
        dataset_id (str): The BigQuery dataset id.

    Returns:
        dataset.
    """
    client = bigquery.Client(project=project_id)
    dataset = client.get_dataset(bigquery.DatasetReference(project_id, dataset_id))
    return dataset


def list_tables(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)
    return client.list_tables(bigquery.DatasetReference(project_id, dataset_id))


def replace_fields_recursively(data):
    """
    Recursively finds all dictionary keys named 'fields' and replaces them
    with 'subfields'. It handles nested dictionaries and lists.

    Args:
        data (dict, list, or other): The data structure to process.

    Returns:
        dict, list, or other: The processed data structure with keys replaced.
    """

    # --- 1. Base Case: If the data is not a collection, return it as is. ---
    if not isinstance(data, (dict, list)):
        return data

    # --- 2. Handle Lists: Recurse on each element of the list. ---
    if isinstance(data, list):
        return [replace_fields_recursively(item) for item in data]

    # --- 3. Handle Dictionaries: Traverse keys and values. ---
    new_data = {}
    for key, value in data.items():
        # Define the new key name
        new_key = "subfields" if key == "fields" else key

        # Recursively process the value
        new_value = replace_fields_recursively(value)

        # Assign the processed value to the new key
        new_data[new_key] = new_value

    return new_data


def export_table(table_metadata: dict) -> dict:
    table_metadata["schema"]["fields"] = replace_fields_recursively(
        table_metadata["schema"]["fields"]
    )
    table_ref = table_metadata["tableReference"]
    return {
        "projectId": table_ref["projectId"],
        "datasetId": table_ref["datasetId"],
        "tableId": table_ref["tableId"],
        "schema": table_metadata["schema"],
    }


def get_table_metadata(project_id: str, dataset_id: str, table_id: str) -> dict:
    """Get metadata information about a BigQuery table fields.

    Args:
        project_id (str): The Google Cloud project id containing the dataset.
        dataset_id (str): The BigQuery dataset id containing the table.
        table_id (str): The BigQuery table id.

    Returns:
        table fields information.
    """
    client = bigquery.Client(project=project_id)
    return client.get_table(
        bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id), table_id
        )
    ).to_api_repr()


def get_tables_metadata(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)
    table_refs = client.list_tables(bigquery.DatasetReference(project_id, dataset_id))
    tables = []
    for t_ref in table_refs:
        tables.append(client.get_table(t_ref).to_api_repr())
    return tables


def get_table_ids_in_dataset(project_id: str, dataset_id: str) -> list[str]:
    """List table ids in a BigQuery dataset.

    Args:
        project_id (str): The Google Cloud project id containing the dataset.
        dataset_id (str): The BigQuery dataset id.
        credentials (Credentials): The credentials to use for the request.

    Returns:
        list[str]: List of the tables ids present in the dataset."""

    client = bigquery.Client(project=project_id)
    table_refs = client.list_tables(bigquery.DatasetReference(project_id, dataset_id))
    table_ids = []
    for t_ref in table_refs:
        table_ids.append(t_ref.table_id)
    return table_ids


def get_table_info_direct(project_id: str, table_reference):
    client = bigquery.Client(project=project_id)
    return client.get_table(table_reference)


def get_job_info(project_id: str, job_id: str):
    client = bigquery.Client(project=project_id)

    job = client.get_job(job_id)
    # We need to use _properties to get the job info because it contains all
    # the job info.


def get_table_schema_and_sample_rows_old(
    project_id: str, dataset_id: str, table_id: str
) -> dict:
    """Get schema and sample rows for a BigQuery table.

    Args:
        project_id (str): The Google Cloud project id containing the dataset.
        dataset_id (str): The BigQuery dataset id containing the table.
        table_id (str): The BigQuery table id.

    Returns:
        A dictionary with these entries:
        - schema: metadata for table fields
        - rows: sample table rows
    """
    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)

    # list_rows calls tabledata.list (very cost efficient)
    rows = client.list_rows(table_ref, max_results=5)

    return {
        "schema": [item.to_api_repr() for item in rows.schema],
        "rows": [row.values() for row in rows],
    }


def get_sample_rows_json(project_id: str, dataset_id: str, table_id: str) -> str:
    """Get sample rows for a BigQuery table.

    Args:
        project_id (str): The Google Cloud project id containing the dataset.
        dataset_id (str): The BigQuery dataset id containing the table.
        table_id (str): The BigQuery table id.

    Returns:
        A list of rows in JSON format
    """
    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)

    # list_rows calls tabledata.list (very cost efficient)
    rows = client.list_rows(table_ref, max_results=5)

    full_rows = []
    for row in rows:
        one_row = {}
        for k in row.keys():
            one_row[k] = row[k]
        full_rows.append(one_row)
    return json.dumps(full_rows)


def split_using_dots(input: str) -> list[str]:
    """splits a string having dots into individual strings"""
    return input.split(".")


def dry_run_sql(project_id: str, queries: list[str]):
    client = bigquery.Client(project=project_id)

    job_config = bigquery.QueryJobConfig(dry_run=True)  # , use_query_cache=False)

    for query in queries:
        print(f"  dry run of '{query}'")
        client.query(query, job_config=job_config)


def _get_column_fqns_from_datasource_references(
    datasource_references_path: Path,
) -> set[str]:
    """Extracts fully qualified column names from datasourceReferences.yaml."""
    with open(datasource_references_path, "r") as file:
        data = yaml.safe_load(file)

    column_fqns = set()
    if not data or "bq" not in data or "tableReferences" not in data["bq"]:
        return column_fqns

    for table_ref in data["bq"]["tableReferences"]:
        project_id = table_ref["projectId"]
        dataset_id = table_ref["datasetId"]
        table_id = table_ref["tableId"]
        table_fqn_prefix = f"bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        if "schema" in table_ref and "fields" in table_ref["schema"]:

            def _extract_fields(fields: list[dict], current_path: list[str]):
                for field in fields:
                    field_name = field["name"]
                    full_field_path = current_path + [field_name]
                    column_name_part = ".".join(full_field_path)
                    column_fqns.add(f"{table_fqn_prefix}:{column_name_part}")

                    if field.get("type") == "RECORD" and "subfields" in field:
                        _extract_fields(field["subfields"], full_field_path)

            _extract_fields(table_ref["schema"]["fields"], [])
    return column_fqns


def check_schema_relationships_columns(
    schema_relationships_path: Path, datasource_references_path: Path
) -> list[str]:
    """
    Checks if all columns mentioned in schemaRelationships.yaml exist in datasourceReferences.yaml.

    Returns:
        A tuple: (True if all columns exist, list of missing columns).
    """
    with open(schema_relationships_path, "r") as file:
        schema_relationships = yaml.safe_load(file)

    if not schema_relationships:
        return []

    existing_columns = _get_column_fqns_from_datasource_references(
        datasource_references_path
    )

    missing_columns = []
    for relationship in schema_relationships:

        def _check_paths(schema_paths_key: str):
            if schema_paths_key in relationship:
                table_fqn = relationship[schema_paths_key]["tableFqn"]
                column_name_part = ".".join(relationship[schema_paths_key]["paths"])
                column_fqn = f"{table_fqn}:{column_name_part}"
                if column_fqn not in existing_columns:
                    missing_columns.append(column_fqn)

        _check_paths("leftSchemaPaths")
        _check_paths("rightSchemaPaths")

    return missing_columns


if __name__ == "__main__":
    PROJECT = "as-alf-argolis"
    DATASET = "fcc_political_ads"
    TABLE = "broadcast_tv_radio_station"
    import json

    result = get_sample_rows_json(PROJECT, DATASET, TABLE)
    print(result)
