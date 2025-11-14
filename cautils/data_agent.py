from cyclopts import App
from requests.exceptions import HTTPError
from rich import print as rprint
import json
import yaml
from .helpers import GeminiDataAnalyticsRequestHelper

app = App("data-agent")


@app.command
def create(project_id: str, location: str, definitions_dir: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    with open(f"{definitions_dir}/dataSourceReferences.yaml", "r") as file:
        dataSourceReferences = yaml.safe_load(file)
    payload = {
        "dataAnalyticsAgent": {
            "publishedContext": {
                # "systemInstruction": string,
                "datasourceReferences": {"bq": dataSourceReferences}
            }
        }
    }
    print(json.dumps(payload, indent=2))
    try:
        response = helper.post("dataAgents", payload)
        print(json.dumps(response, indent=2))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")


@app.command
def list(project_id: str, location: str):
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    params = {"pageSize": 10}
    try:
        response = helper.get("dataAgents", params)
        print(json.dumps(response, indent=2))
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")
