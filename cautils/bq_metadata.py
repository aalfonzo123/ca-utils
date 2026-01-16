from cyclopts import App
from yaml import dump
import json


app = App(
    "bq-metadata",
    help="commands related to bigquery metadata extraction (this section is obsolete, but hasn't been deleted)",
)


def get_entry_info(
    project_id, location, entry_group, entry, aspect_types=None, api_version="v1"
):
    """Gets information about a Dataplex entry."""
    token = get_access_token()
    if not token:
        return

    url = f"https://dataplex.googleapis.com/{api_version}/projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}"
    if aspect_types:
        url += f"?view=CUSTOM&aspectTypes={','.join(aspect_types)}"
    else:
        url += "?view=FULL"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        entry_data = response.json()
        print(f"--- Entry Info (from {api_version} API) ---")
        print(json.dumps(entry_data, indent=2))
        if "aspects" in entry_data:
            print(f"\n--- Entry Aspects (from {api_version} API) ---")
            print(json.dumps(entry_data["aspects"], indent=2))
        else:
            print("\nNo aspects found for this entry.")
        return entry_data
    else:
        print(
            f"Error getting entry information from {api_version} API: {response.status_code}"
        )
        print(response.text)
        return None


# Note: this is not being used, it was replaced by
# data_agent.autogen
@app.command
def export(project_id: str, dataset_id: str):
    from . import metadata_tool

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
