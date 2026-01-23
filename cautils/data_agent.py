import json
from pathlib import Path

from google.api_core.exceptions import BadRequest
import yaml
from cyclopts import App
from requests.exceptions import HTTPError
from rich import print as rprint
from rich.prompt import Prompt
from typing import Callable

from importlib.resources import files

from cautils.metadata_tool import MultiErrorException


from .helpers import GeminiDataAnalyticsRequestHelper, paginate
from .print_list_helper import (
    after_last_slash_multi,
    get_table_generic,
    after_last_slash,
    DEFAULT_VALUE,
)

app = App("data-agent", help="commands related to conversational analytics api agents")

DATA_AGENT_ELEMENTS = [
    "datasourceReferences",
    "exampleQueries",
    "glossaryTerms",
    "schemaRelationships",
    "systemInstruction",
]


def copy_if_exists(from_dict: dict, to_dict: dict, keys: list[str]):
    if not from_dict:
        return
    for k in keys:
        if k in from_dict:
            to_dict[k] = from_dict[k]


def read_json(filename: str):
    with open(filename, "r") as f:
        data = json.load(f)

    return data


def read_bytes(file_path: Path):
    with open(file_path, "rb") as f:
        data = f.read()

    return data


def _resource_write_after_confirm(
    content_generator: Callable[[], str], path: Path, ask: bool
):
    if path.exists() and ask:
        choice = Prompt.ask(
            f"File {path} exists, overwrite? [Y]es,[N]o,[A]ll",
            choices=["y", "n", "a"],
            default="n",
        )
        if choice == "n":
            return True
        elif choice == "a":
            ask = False
    path.write_text(content_generator())
    print(f"Wrote {path}")
    return ask


def _yaml_dump_after_confirm(
    content_generator: Callable[[], dict], path: Path, ask: bool
):
    if path.exists() and ask:
        choice = Prompt.ask(
            f"File {path} exists, overwrite? [Y]es,[N]o,[A]ll",
            choices=["y", "n", "a"],
            default="n",
        )
        if choice == "n":
            return True
        elif choice == "a":
            ask = False
    with open(path, "w") as file:
        yaml.safe_dump(content_generator(), file)
        print(f"Wrote {path}")
    return ask


def _gen_example_queries(
    project_id: str, location: str, data_source_references_path: Path
):
    from google.genai.types import (
        Content,
        GenerateContentConfig,
        Part,
    )
    from google import genai

    """Generates the schemaRelationships.yaml file, by calling an LLM with:
    - input: the data_sourceReferences.yaml file
    - output schema: a json schema file that matches the expected output
    """
    history = [
        Content(
            role="user",
            parts=[
                Part.from_bytes(
                    data=read_bytes(data_source_references_path),
                    mime_type="text/plain",
                ),
            ],
        )
    ]

    rel_schema = files("cautils").joinpath("exampleQueries_schema.json")

    genai_client = genai.Client(vertexai=True, project=project_id, location=location)
    response = genai_client.models.generate_content(
        model="gemini-2.0-flash",
        contents=history,
        config=GenerateContentConfig(
            system_instruction="Your goal is to create one sample natural language query and its corresponding SQL statement\n"
            "For input, you will be given the metadata for the tables in a yaml format\n",
            response_json_schema=json.loads(rel_schema.read_text(encoding="utf-8")),
            response_mime_type="application/json",
        ),
    )
    if response.candidates:
        return json.loads(response.candidates[0].content.parts[0].text)
    else:
        raise Exception("no response from LLM")


def _gen_schema_relationships(
    project_id: str, location: str, data_source_references_path: Path
):
    from google.genai.types import (
        Content,
        GenerateContentConfig,
        Part,
    )
    from google import genai

    """Generates the schemaRelationships.yaml file, by calling an LLM with:
    - input: the data_sourceReferences.yaml file
    - output schema: a json schema file that matches the expected output
    """
    history = [
        Content(
            role="user",
            parts=[
                Part.from_bytes(
                    data=read_bytes(data_source_references_path),
                    mime_type="text/plain",
                ),
            ],
        )
    ]

    rel_schema = files("cautils").joinpath("schemaRelationships_schema.json")

    genai_client = genai.Client(vertexai=True, project=project_id, location=location)
    response = genai_client.models.generate_content(
        model="gemini-2.0-flash",
        contents=history,
        config=GenerateContentConfig(
            system_instruction="Your goal is to infer foreign key relationships between tables in a database schema\n"
            "For input, you will be given the metadata for the tables in a yaml format\n",
            response_json_schema=json.loads(rel_schema.read_text(encoding="utf-8")),
            response_mime_type="application/json",
        ),
    )
    if response.candidates:
        return json.loads(response.candidates[0].content.parts[0].text)
    else:
        raise Exception("no response from LLM")


@app.command
def init():
    """Copies initial config files to the current directory."""
    try:
        ask = True
        for resource in files("cautils.init_files").iterdir():
            if resource.is_file():
                dest_file = Path(resource.name)

                ask = _resource_write_after_confirm(
                    lambda: resource.read_text(),
                    dest_file,
                    ask,
                )
        rprint("[green]Init succeeded[/green]")
    except FileExistsError as e:
        rprint(f"[bright_red]{e}[/bright_red]")


def autogen_internal(
    project_id: str,
    location: str,
    gen_data_source_references: bool = True,
    gen_schema_relationships: bool = True,
    gen_example_queries: bool = True,
    ask: bool = True,
):
    """Actual implementation of autogen, in
    a way that can be called by the command or other functions.
    """
    # TODO: add warning if some columns are missing descriptions
    data_source_references_path = Path("datasourceReferences.yaml")
    if gen_data_source_references:
        from . import metadata_tool as mt

        with open("autogen.yaml", "r") as file:
            autogen = yaml.safe_load(file)

        if not autogen or "bqDataSources" not in autogen:
            raise ValueError("autogen.yaml must specify bqDataSources")
        table_extracts = []
        for named_table in autogen["bqDataSources"]:
            parts = named_table.strip().split(".")
            print(f"exporting {named_table}")
            if parts[2] == "*":
                for table_meta in mt.get_tables_metadata(parts[0], parts[1]):
                    table_extracts.append(mt.export_table(table_meta))
            else:
                table_meta = mt.get_table_metadata(parts[0], parts[1], parts[2])
                table_extracts.append(mt.export_table(table_meta))

        ask = _yaml_dump_after_confirm(
            lambda: {"bq": {"tableReferences": table_extracts}},
            data_source_references_path,
            ask,
        )

    if not data_source_references_path.exists():
        raise FileNotFoundError(
            f"Cannot generate content if {data_source_references_path} does not exist"
        )

    if gen_example_queries:
        ask = _yaml_dump_after_confirm(
            lambda: _gen_example_queries(
                project_id, location, data_source_references_path
            ),
            Path("exampleQueries.yaml"),
            ask,
        )

    if gen_schema_relationships:
        ask = _yaml_dump_after_confirm(
            lambda: _gen_schema_relationships(
                project_id, location, data_source_references_path
            ),
            Path("schemaRelationships.yaml"),
            ask,
        )


@app.command
def autogen(
    project_id: str,
    location: str,
    gen_data_source_references: bool = True,
    gen_schema_relationships: bool = True,
    gen_example_queries: bool = True,
    ask: bool = True,
):
    """Auto generates data agent files based on specification.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        gen_data_source_references: Whether to generate data source references.
        gen_schema_relationships: Whether to generate schema relationships.
        gen_example_queries: Whether to generate example queries.
    """
    try:
        autogen_internal(
            project_id,
            location,
            gen_data_source_references,
            gen_schema_relationships,
            gen_example_queries,
            ask,
        )
        rprint("[green]Files auto generated[/green]")
    except (FileExistsError, OSError, ValueError) as e:
        rprint(f"[bright_red]{e}[/bright_red]")


@app.command
def upload(
    project_id: str,
    location: str,
    patch: bool = False,
):
    """Uploads data agent files to the specified project and location.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        patch: Whether to patch an existing agent.
    """
    ca_agent_id = Path().resolve().name
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    publishedContext = {}
    # TODO: reconsider having systemInstruction as yaml, causes issues with colon ":"
    for element in DATA_AGENT_ELEMENTS:
        path = Path(f"{element}.yaml")
        if path.exists():
            with open(path, "r") as file:
                publishedContext[element] = yaml.safe_load(file)
            print(f"Added {element}")

    payload = {
        "dataAnalyticsAgent": {"publishedContext": publishedContext},
    }
    # add metadata
    path = Path("agentMetadata.yaml")
    if path.exists():
        with open(path, "r") as file:
            metadata = yaml.safe_load(file)
        copy_if_exists(metadata, payload, ["displayName", "description"])
        print("Added metadata")
    # print(json.dumps(payload, indent=2))
    try:
        print(f"Uploading agent {ca_agent_id}")
        if patch:
            params = {
                "updateMask": "dataAnalyticsAgent.publishedContext,displayName,description"
            }
            response = helper.patch(f"dataAgents/{ca_agent_id}", payload, params)
        else:
            params = {"dataAgentId": ca_agent_id}
            response = helper.post("dataAgents", payload, params)

        name_parts = response["name"].split("/")
        project_number = name_parts[1]
        location = name_parts[3]
        lro_id = name_parts[5]

        rprint("[green]Deployment started[/green]")
        rprint(
            f"To follow status of lro, run [green]ca-utils da-lro follow {project_number} {location} {lro_id}[/green]"
        )
    #        print(json.dumps(response, indent=2))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


def print_agent_list(data):
    app.console.print(
        get_table_generic(
            data.get("dataAgents", []),
            {
                "Name": {
                    "opts": {"style": "bright_green"},
                    "path": "name",
                    "proc": after_last_slash,
                },
                "Display Name": "displayName",
                "Description": "description",
                "System Instruction": {
                    "path": "dataAnalyticsAgent.publishedContext.systemInstruction",
                    "proc": lambda x: x[:80] if x else DEFAULT_VALUE,
                },
                "Data Source": {
                    "path": "dataAnalyticsAgent.publishedContext.datasourceReferences",
                    "proc": lambda dsr: (
                        f"bq: {','.join([f'{t["datasetId"]}.{t.get("tableId", "*")}' for t in dsr['bq']['tableReferences']])} "
                        if dsr.get("bq")
                        else "looker studio"
                        if dsr.get("studio")
                        else "looker"
                        if dsr.get("looker")
                        else "?"
                    ),
                },
            },
        )
    )


@app.command(name="list")
def list_agents(project_id: str, location: str, format_raw: bool = False):
    """Lists data agents in the specified project and location.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        format_raw: Whether to print the raw JSON output.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    paginate(
        lambda params: helper.get("dataAgents", params),
        lambda data: print_agent_list(data),
        format_raw,
    )


def print_conversation_list(data):
    app.console.print(
        get_table_generic(
            data.get("conversations"),
            {
                "Name": {
                    "opts": {"style": "bright_green"},
                    "path": "name",
                    "proc": after_last_slash,
                },
                "Agents": {"path": "agents", "proc": after_last_slash_multi},
                "Dates": {
                    "path": ["createTime", "lastUsedTime"],
                    "proc": lambda values: f"created:{values['createTime']}\nlast updated:{values['lastUsedTime']}",
                },
            },
        )
    )


@app.command
def list_conversation(project_id: str, location: str, format_raw: bool = False):
    """Lists conversations in the specified project and location.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    paginate(
        lambda params: helper.get("conversations", params),
        lambda data: print_conversation_list(data),
        format_raw,
    )


@app.command
def delete_conversation(project_id: str, location: str, conversation_id: str):
    """Deletes a specific conversation by ID.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        conversation_id: The ID of the conversation to delete.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    try:
        helper.delete(f"conversations/{conversation_id}")
        rprint("[green]Conversation deleted[/green]")
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


@app.command
def download(project_id: str, location: str, dry_run: bool = False, ask: bool = True):
    """Downloads a data agent to the local filesystem.

    The name of the agent is inferred from the name of the current directory.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        dry_run: If true, prints the downloaded content instead of writing to files.
    """
    ca_agent_id = Path().resolve().name
    rprint(f"[green]Downloading agent '{ca_agent_id}' to the current folder[/green]")
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    try:
        response = helper.get(f"dataAgents/{ca_agent_id}")
        # print(json.dumps(response, indent=2))
        for element in DATA_AGENT_ELEMENTS:
            content = response["dataAnalyticsAgent"]["publishedContext"].get(element)
            if not content:
                continue
            if dry_run:
                print(f"{element}: {content}")
                continue
            ask = _yaml_dump_after_confirm(
                lambda: content, Path(f"{element}.yaml"), ask
            )

        metadata = {}
        copy_if_exists(response, metadata, ["displayName", "description"])
        if metadata and not dry_run:
            ask = _yaml_dump_after_confirm(
                lambda: metadata, Path("agentMetadata.yaml"), ask
            )
        rprint("[green]Data Agent downloaded to the current folder[/green]")
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")
    except FileExistsError as e:
        rprint(f"[bright_red]{e}[/bright_red]")


@app.command
def chat(project_id: str, location: str, ca_agent_id: str, prompt: str):
    """Initiates a chat with a specified data agent.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        ca_agent_id: The ID of the data agent to chat with.
        prompt: The user's prompt.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    payload = {
        "messages": [{"userMessage": {"text": prompt}}],
        "dataAgentContext": {
            "dataAgent": f"projects/{project_id}/locations/{location}/dataAgents/{ca_agent_id}"
        },
    }
    try:
        response = helper.post(":chat", payload)
        rprint(json.dumps(response, indent=2))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


def sanity_check_internal(
    project_id: str,
    location: str,
    check_examplequeries_dry_run: bool = True,
    check_schemarelationship_cols: bool = True,
):
    """Actual implementation of sanity_check, in
    a way that can be called by the command or other functions.
    """
    from . import metadata_tool as mt

    errors = []
    if check_examplequeries_dry_run:
        print("Checking example queries using dry run")
        with open("exampleQueries.yaml", "r") as file:
            exampleQueries = yaml.safe_load(file)
        try:
            mt.dry_run_sql(project_id, [e["sqlQuery"] for e in exampleQueries])
        except MultiErrorException as e:
            errors.extend(e.errors)
    if check_schemarelationship_cols:
        print("Checking schema relationship columns")
        schema_relationships_path = Path("schemaRelationships.yaml")
        datasource_references_path = Path("datasourceReferences.yaml")
        if not schema_relationships_path.exists():
            raise FileNotFoundError(f"{schema_relationships_path} not found.")
        if not datasource_references_path.exists():
            raise FileNotFoundError(f"{datasource_references_path} not found.")

        missing_cols = mt.check_schema_relationships_columns(
            schema_relationships_path, datasource_references_path
        )
        if missing_cols:
            errors.append(
                f"schemaRelationships.yaml mentions these columns that don't exist in datasourceReferences.yaml: {missing_cols}"
            )
    if errors:
        raise MultiErrorException("Sanity check errors found", errors)


@app.command
def sanity_check(
    project_id: str,
    location: str,
    check_examplequeries_dry_run: bool = True,
    check_schemarelationship_cols: bool = True,
):
    """Performs sanity checks on data agent configuration files.

    This command validates the integrity and correctness of various data agent
    configuration files, such as example queries and schema relationships.

    Args:
        project_id: The Google Cloud project ID for BigQuery operations.
        location: The Google Cloud location (currently not directly used in checks).
        check_examplequeries_dry_run: If True, performs a dry run on SQL queries
                                      defined in `exampleQueries.yaml` to check for syntax errors.
        check_schemarelationship_cols: If True, verifies that all columns referenced
                                       in `schemaRelationships.yaml` exist in
                                       `datasourceReferences.yaml`.

    Raises:
        ValueError: If required files (e.g., `exampleQueries.yaml`,
                    `schemaRelationships.yaml`, `datasourceReferences.yaml`) are
                    not found, or if schema relationship checks fail due to
                    missing columns.
        Exception: For any other errors encountered during the checks.
    """
    try:
        sanity_check_internal(
            project_id,
            location,
            check_examplequeries_dry_run,
            check_schemarelationship_cols,
        )
        rprint("[green]Checks succeeded[/green]")
    except MultiErrorException as e:
        rprint(
            f"[bright_red]Validation errors found:\n{'\n'.join(e.errors)}[/bright_red]"
        )
    except Exception as e:
        rprint(f"[bright_red]{e}[/bright_red]")


def introspect_autogen_internal(
    project_id: str,  # pyright: ignore [reportUnusedVariable]
    location: str,
    ask: bool = True,
):
    """Actual implementation of introspect_autogen, in
    a way that can be called by the command or other functions.
    """
    from . import metadata_tool as mt

    print("Introspecting autogen")
    table_extracts = mt.introspect_autogen(Path("datasourceReferences.yaml"))
    ask = _yaml_dump_after_confirm(
        lambda: {"bqDataSources": table_extracts},
        Path("autogen.yaml"),
        ask,
    )


@app.command
def introspect_autogen(
    project_id: str,  # pyright: ignore [reportUnusedVariable]
    location: str,
    ask: bool = True,
):
    """Generates the autogen.yaml file from the datasourceReferences.yaml file,
    which is the opposite of the normal workflow.

    This is only useful if the original autogen.yaml is unavailable (i.e. it was not stored
        in source control), but the datasourceReferences.yaml is available
        (i.e. it was retrieved from GCP using download).

    Note that the instrospected file never has wildcard lines (i.e. prj.ds.*)
        so it can be different to the original autogen.yaml

    Args:
        project_id: The Google Cloud project ID. Unused, only here for consistency.
        location: The Google Cloud location. Unused, only here for consistency.
        ask: Whether to ask when overwriting files or not.
    """
    try:
        introspect_autogen_internal(project_id, location, ask)
        rprint("[green]Introspect autogen succeeded[/green]")
    except Exception as e:
        rprint(f"[bright_red]{e}[/bright_red]")
