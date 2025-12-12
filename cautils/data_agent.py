import json
from pathlib import Path

import yaml
from cyclopts import App
from requests.exceptions import HTTPError
from rich import box
from rich import print as rprint
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from typing import Callable
from . import metadata_tool as mt
from importlib.resources import files

from google.genai.types import (
    Content,
    GenerateContentConfig,
    Part,
)
from google import genai

from .helpers import GeminiDataAnalyticsRequestHelper

app = App("data-agent")

DATA_AGENT_ELEMENTS = [
    "datasourceReferences",
    "exampleQueries",
    "glossaryTerms",
    "schemaRelationships",
    "systemInstruction",
]


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


@app.command
def autogen(
    project_id: str,
    location: str,
    gen_data_source_references: bool = True,
    gen_schema_relationships: bool = True,
    gen_example_queries: bool = True,
):
    """Auto generates data agent files based on specification"""
    try:
        data_source_references_path = Path("datasourceReferences.yaml")
        ask = True
        if gen_data_source_references:
            with open("autogen.yaml", "r") as file:
                autogen = yaml.safe_load(file)

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
        rprint("[green]Files auto generated[/green]")
    except (FileExistsError, OSError) as e:
        rprint(f"[bright_red]{e}[/bright_red]")


@app.command
def upload(
    project_id: str,
    location: str,
    patch: bool = False,
):
    ca_agent_id = Path().resolve().name
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    publishedContext = {}
    for element in DATA_AGENT_ELEMENTS:
        path = Path(f"{element}.yaml")
        if path.exists():
            with open(path, "r") as file:
                publishedContext[element] = yaml.safe_load(file)
            print(f"Added {element}")

    payload = {
        # "displayName": display_name,
        "dataAnalyticsAgent": {"publishedContext": publishedContext},
    }
    # print(json.dumps(payload, indent=2))
    try:
        print(f"Uploading agent {ca_agent_id}")
        if patch:
            params = {"updateMask": "dataAnalyticsAgent.publishedContext"}
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
    table = Table(box=box.SQUARE, show_lines=True)
    table.add_column("Name", style="bright_green")
    table.add_column("Display Name")
    table.add_column("System Instruction")
    table.add_column("Data Source")

    for item in data.get("dataAgents", []):
        name = item["name"].split("/")[-1]
        display_name = item.get("displayName", "N.A")
        da = item.get("dataAnalyticsAgent", {})
        pc = da.get("publishedContext", {})
        system_instruction = pc.get("systemInstruction", "N.A")[:80]
        dsr = pc.get("datasourceReferences", {})
        bq = dsr.get("bq", {})
        bq_table_count = len(bq.get("tableReferences", []))

        if bq:
            data_source = f"bq. {bq_table_count} tables"
        elif dsr.get("studio"):
            data_source = "looker studio"
        elif dsr.get("looker"):
            data_source = "looker"
        else:
            data_source = "?"

        table.add_row(name, display_name, system_instruction, data_source)

    console = Console(highlight=False)
    console.print(table)


@app.command
def list(project_id: str, location: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    params = {"pageSize": 10}
    try:
        response = helper.get("dataAgents", params)
        # rprint(response)
        print_agent_list(response)
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


def print_conversation_list(data):
    # print(json.dumps(data, indent=2))
    # return
    table = Table(box=box.SQUARE, show_lines=True)
    table.add_column("Name", style="bright_green")
    table.add_column("Agents")
    table.add_column("Dates")

    for item in data.get("conversations", []):
        name = item["name"].split("/")[-1]
        agents = ",".join([a.split("/")[-1] for a in item.get("agents", [])])
        dates = f"created:{item['createTime']}\nlast updated:{item['lastUsedTime']}"

        table.add_row(name, agents, dates)

    console = Console(highlight=False)
    console.print(table)


@app.command
def list_conversation(project_id: str, location: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    params = {
        "pageSize": 5,
        # "filter": '1=2 AND agents:"abc"',
        "filter": 'createTime > "2026-01-28T06:51:56-08:00"',
    }
    try:
        print_conversation_list(helper.get("conversations", params))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


@app.command
def delete_conversation(project_id: str, location: str, conversation_id: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    try:
        helper.delete(f"conversations/{conversation_id}")
        rprint("[green]Conversation deleted[/green]")
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


@app.command
def download(project_id: str, location: str, dry_run: bool = False):
    """Downloads a data agent to the local filesystem, the name of the agent
    is inferred from the name of the current directory."""
    ca_agent_id = Path().resolve().name
    rprint(f"[green]Downloading agent '{ca_agent_id}' to the current folder[/green]")
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    try:
        response = helper.get(f"dataAgents/{ca_agent_id}")
        # print(json.dumps(response, indent=2))
        ask = True
        for element in DATA_AGENT_ELEMENTS:
            content = response["dataAnalyticsAgent"]["publishedContext"].get(element)
            if not content:
                continue
            if dry_run:
                print(f"{element}: {content}")
                continue
            path = Path(f"{element}.yaml")
            ask = _yaml_dump_after_confirm(lambda: content, path, ask)

        rprint("[green]Data Agent downloaded to the current folder[/green]")
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")
    except FileExistsError as e:
        rprint(f"[bright_red]{e}[/bright_red]")


@app.command
def chat(project_id: str, location: str, ca_agent_id: str, prompt: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    payload = {
        "messages": [{"userMessage": {"text": prompt}}],
        "dataAgentContext": {"dataAgent": ca_agent_id},
    }
    try:
        response = helper.post(":chat", payload)
        rprint(json.dumps(response, indent=2))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")
