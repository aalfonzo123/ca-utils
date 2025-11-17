import json
from pathlib import Path

import yaml
from cyclopts import App
from requests.exceptions import HTTPError
from rich import box
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from . import llm_adk
from .helpers import GeminiDataAnalyticsRequestHelper

import asyncio

app = App("data-agent")


@app.command
def autogen(
    project_id: str,
    location: str,
    base_dir: str,
    ca_agent_id: str,
    gen_datasource_references: bool = True,
    gen_schema_relationships: bool = True
):
    """Auto generates data agent files based on specification"""
    try:
        path_name = f"{base_dir}/{ca_agent_id}"
        with open(f"{path_name}/autogen.yaml", "r") as file:
            autogen = yaml.safe_load(file)
        #print(autogen)
        rprint("[green]Autogen using LLM starting[/green]")
        if gen_schema_relationships:
            llm_adk.autogen_schema_relationships(autogen["bqDataSources"], 
                                        f"{path_name}/schemaRelationships.yaml")
            rprint("[green]Inferred schema relationships written[/green]")
        if gen_datasource_references:
            llm_adk.autogen_ds_references(autogen["bqDataSources"], 
                                        f"{path_name}/datasourceReferences.yaml")
            rprint("[green]Table fields and inferred descriptions written[/green]")
        rprint("[green]Autogen finished[/green]")
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
    #print(json.dumps(payload, indent=2))
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


def print_list(data):
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
        print_list(helper.get("dataAgents", params))
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


