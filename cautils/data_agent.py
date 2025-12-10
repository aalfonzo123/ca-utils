import json
from pathlib import Path

import yaml
from cyclopts import App
from requests.exceptions import HTTPError
from rich import box
from rich import print as rprint
from rich.console import Console
from rich.table import Table
from . import metadata_tool as mt

from google.genai.types import (
    Content,
    GenerateContentConfig,
    ToolConfig,
    Part,
)

from google import genai

# from . import llm_adk
from .helpers import GeminiDataAnalyticsRequestHelper

import asyncio

app = App("data-agent")


def read_json(filename: str):
    with open(filename, "r") as f:
        data = json.load(f)

    return data


def read_bytes(filename: str):
    with open(filename, "rb") as f:
        data = f.read()

    return data


def _gen_schema_relationships(
    project_id: str, location: str, data_source_references_path: str
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

    genai_client = genai.Client(vertexai=True, project=project_id, location=location)
    response = genai_client.models.generate_content(
        model="gemini-2.0-flash",
        contents=history,
        config=GenerateContentConfig(
            system_instruction="Your goal is to infer foreign key relationships between tables in a database schema\n"
            "For input, you will be given the metadata for the tables in a yaml format\n",
            response_json_schema=read_json("cautils/schemaRelationships_schema.json"),
            response_mime_type="application/json",
        ),
    )
    if response.candidates:
        return json.loads(response.candidates[0].content.parts[0].text)
    else:
        raise Exception("no response from LLM")


@app.command
def autogen(
    project_id: str,
    location: str,
    base_dir: str,
    ca_agent_id: str,
    gen_schema_relationships: bool = True,
):
    """Auto generates data agent files based on specification"""
    try:
        path_name = f"{base_dir}/{ca_agent_id}"
        with open(f"{path_name}/autogen.yaml", "r") as file:
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

        data_source_references_path = f"{path_name}/datasourceReferences.yaml"
        with open(data_source_references_path, "w") as dsr_file:
            yaml.safe_dump({"bq": {"tableReferences": table_extracts}}, dsr_file)

        if gen_schema_relationships:
            print("exporting schema relationships")
            with open(f"{path_name}/schemaRelationships.yaml", "w") as rel_file:
                yaml.safe_dump(
                    _gen_schema_relationships(
                        project_id, location, data_source_references_path
                    ),
                    rel_file,
                )

        rprint("[green]Files auto generated[/green]")
    except OSError as e:
        rprint(f"[bright_red]{e}[/bright_red]")


@app.command
def upload(
    project_id: str,
    location: str,
    base_dir: str,
    ca_agent_id: str,
    patch: bool = False,
):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    path_name = f"{base_dir}/{ca_agent_id}"
    with open(f"{path_name}/systemInstruction.txt", "r") as file:
        systemInstruction = file.read()
    with open(f"{path_name}/datasourceReferences.yaml", "r") as file:
        datasourceReferences = yaml.safe_load(file)
    with open(f"{path_name}/schemaRelationships.yaml", "r") as file:
        schemaRelationships = yaml.safe_load(file)

    payload = {
        # "displayName": display_name,
        "dataAnalyticsAgent": {
            "publishedContext": {
                "systemInstruction": systemInstruction,
                "datasourceReferences": datasourceReferences,
                "schemaRelationships": schemaRelationships,
            }
        },
    }
    # print(json.dumps(payload, indent=2))
    try:
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


@app.command
def list(project_id: str, location: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    params = {"pageSize": 10}
    try:
        print_agent_list(helper.get("dataAgents", params))
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
        rprint(f"[green]Conversation deleted[/green]")
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


@app.command
def download(
    project_id: str,
    location: str,
    base_dir: str,
    ca_agent_id: str,
    overwrite_ok: bool = False,
):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    try:
        response = helper.get(f"dataAgents/{ca_agent_id}")
        # print(json.dumps(response, indent=2))
        path_name = f"{base_dir}/{ca_agent_id}"
        Path(path_name).mkdir(exist_ok=overwrite_ok)
        with open(f"{path_name}/systemInstruction.txt", "w") as file:
            file.write(
                response["dataAnalyticsAgent"]["publishedContext"]["systemInstruction"]
            )
        with open(f"{path_name}/datasourceReferences.yaml", "w") as file:
            yaml.dump(
                response["dataAnalyticsAgent"]["publishedContext"][
                    "datasourceReferences"
                ],
                file,
            )
        with open(f"{path_name}/schemaRelationships.yaml", "w") as file:
            yaml.dump(
                response["dataAnalyticsAgent"]["publishedContext"][
                    "schemaRelationships"
                ],
                file,
            )

        rprint(f"[green]Data Agent downloaded to {path_name}[/green]")
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
