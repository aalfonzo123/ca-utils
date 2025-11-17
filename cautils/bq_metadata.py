from cyclopts import App
from yaml import dump
import json
from . import metadata_tool

app = App("bq-metadata")


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

# Note: this is not being used, it was replaced by
# data_agent.autogen
# This one is 100% deterministic and runs almost immediately,
# unlike the LLM based autogen.
@app.command
def export(project_id: str, dataset_id: str):
    tables = metadata_tool.get_tables(project_id=project_id, dataset_id=dataset_id)
    table_extracts = []
    for t in tables:
        obj = t.to_api_repr()
        tableReference = obj["tableReference"]
        fixed_fields = replace_fields_recursively(obj["schema"]["fields"])
        obj["schema"]["fields"] = fixed_fields
        table_extracts.append(
            {
                "projectId": tableReference["projectId"],
                "datasetId": tableReference["datasetId"],
                "tableId": tableReference["tableId"],
                "schema": obj["schema"],
            }
        )

    print(dump({"tableReferences": table_extracts}))
    # print("---")
    # print(json.dumps({"tableReferences": references}, indent=2))
    # for table in tables:
    #     print(table.to_api_repr())
