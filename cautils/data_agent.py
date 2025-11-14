from cyclopts import App
from requests.exceptions import HTTPError
from rich import print as rprint
import json
import yaml
from .helpers import GeminiDataAnalyticsRequestHelper
from rich.console import Console
from rich.table import Table
from rich import box

app = App("data-agent")


@app.command
def create(
    project_id: str, location: str, name: str, display_name: str, definitions_dir: str
):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    with open(f"{definitions_dir}/dataSourceReferences.yaml", "r") as file:
        dataSourceReferences = yaml.safe_load(file)
    payload = {
        "displayName": display_name,
        "dataAnalyticsAgent": {
            "publishedContext": {
                # "systemInstruction": string,
                "datasourceReferences": {"bq": dataSourceReferences}
            }
        },
    }
    params = {"dataAgentId": name}
    print(json.dumps(payload, indent=2))
    try:
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
        # print(json.dumps(response, indent=2))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


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


# TO-DO: export command
