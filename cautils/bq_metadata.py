from cyclopts import App
from google.cloud.bigquery import TableReference
from yaml import dump
import json
from . import metadata_tool

app = App(
    "bq-metadata",
    help="commands related to bigquery metadata extraction (this section is obsolete, but hasn't been deleted)",
)


# Note: this is not being used, it was replaced by
# data_agent.autogen
@app.command
def export(project_id: str, dataset_id: str):
    """Exports BigQuery table metadata.

    Args:
        project_id: The Google Cloud project ID.
        dataset_id: The ID of the BigQuery dataset.
    """
    tables = metadata_tool.get_tables_metadata(
        project_id=project_id, dataset_id=dataset_id
    )
    table_extracts = []
    for t in tables:
        table_extracts.append(metadata_tool.export_table(t))

    print(dump({"tableReferences": table_extracts}))
    # print("---")
    # print(json.dumps({"tableReferences": references}, indent=2))
    # for table in tables:
    #     print(table.to_api_repr())
