from rich.console import Console
import json
from cyclopts import App
from rich.table import Table
from rich import box
from rich.live import Live
from rich import print as rprint
import time

from .helpers import GeminiDataAnalyticsRequestHelper

app = App(
    "da-lro",
    "commands related to conversational analytics long running operations. i.e. deployments",
)


def generate_table(data):
    table = Table(box=box.SQUARE, show_lines=True)
    table.add_column("LRO IDs", style="bright_green")
    table.add_column("Verb\nTarget", overflow="fold")
    table.add_column("Status\nDates")
    table.add_column("Response", overflow="fold")

    for item in data.get("operations", []):
        name = "\n".join(item["name"].split("/")[4:])
        metadata = item.get("metadata", {})
        verb = metadata.get("verb", "N/A")
        target = metadata.get("target", "N/A")
        create_time = metadata.get("createTime", "N.A")
        end_time = metadata.get("updateTime", "N.A")
        dates = f"create: {create_time}\nupdate: {end_time}"

        if item.get("done"):
            status = "done"
            error = item.get("error")
            if error:
                status = "[bright_red]error[/bright_red]"
                response = f"code:{error.get('code')}\nmessage:{error.get('message')}"
            item_response = item.get("response")
            if item_response:
                status = "[bright_green]success[/bright_green]"
                response = ""
        else:
            status = "running"
            response = "N/A"

        table.add_row(name, verb + "\n" + target, status + "\n" + dates, response)

    return table


@app.command()
def list(project_id: str, location: str):
    """Lists long running operations (LROs) in the specified project and location.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    params = {"pageSize": 10}
    data = helper.get("operations", params)
    console = Console(highlight=False)
    # print(json.dumps(data, indent=2))
    console.print(generate_table(data))


@app.command()
def follow(project_id: str, location: str, lro_id: str):
    """Follows the status of a specific long running operation (LRO).

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
        lro_id: The ID of the long running operation to follow.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)

    SLEEP = 15
    rprint(f"Updates are made every {SLEEP}s. Times are in UTC.")
    rprint(
        "This will exit when LRO is done. [yellow]To cancel before that, press Ctrl+C[/yellow]"
    )
    with Live(Table(), auto_refresh=False) as live:
        start_time = time.monotonic()
        while True:
            current_elapsed_seconds = time.monotonic() - start_time
            lro_data = helper.get(f"operations/{lro_id}")
            live.update(generate_table({"operations": [lro_data]}), refresh=True)
            if lro_data.get("done", False):
                break
            time.sleep(SLEEP)
