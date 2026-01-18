from rich.console import Console
from cyclopts import App
from rich.table import Table
from rich import box
from rich.live import Live
from rich import print as rprint
import time

from .helpers import GeminiDataAnalyticsRequestHelper, paginate
from .print_list_helper import DEFAULT_VALUE, get_table_generic

app = App(
    "da-lro",
    "commands related to conversational analytics long running operations. i.e. deployments",
)


def print_list(data):
    app.console.print(
        get_table_generic(
            data.get("operations"),
            {
                "LRO IDs": {
                    "path": "name",
                    "opts": {"style": "bright_green"},
                    "proc": lambda name: "\n".join(name.split("/")[4:]),
                },
                "Verb\nTarget": {
                    "path": ["verb", "target"],
                    "base_path": "metadata",
                    "opts": {"overflow": "fold"},
                    "proc": lambda item: f"{item.get('verb') or DEFAULT_VALUE}\n{item.get('target') or DEFAULT_VALUE}",
                },
                "Status\nDates": {
                    "path": [
                        "metadata.createTime",
                        "metadata.updateTime",
                        "done",
                        "error",
                    ],
                    "proc": lambda item: (
                        "[bright_red]error[/bright_red]"
                        if item.get("error")
                        else "[bright_green]success[/bright_green]"
                        if item.get("done")
                        else "running"
                    )
                    + f"\ncreate: {item.get('metadata.createTime') or DEFAULT_VALUE}\nupdate: {item.get('metadata.updateTime') or DEFAULT_VALUE}",
                },
                "Response": {
                    "path": ["done", "error", "response"],
                    "opts": {"overflow": "fold"},
                    "proc": lambda item: (
                        f"code:{item['error'].get('code')}\nmessage:{item['error'].get('message')}"
                        if item.get("error")
                        else ("" if item.get("response") else DEFAULT_VALUE)
                    )
                    if item.get("done")
                    else DEFAULT_VALUE,
                },
            },
        )
    )


@app.command()
def list(project_id: str, location: str):
    """Lists long running operations (LROs) in the specified project and location.

    Args:
        project_id: The Google Cloud project ID.
        location: The Google Cloud location.
    """
    helper = GeminiDataAnalyticsRequestHelper(project_id, location)
    paginate(
        lambda params: helper.get("operations", params),
        lambda data: print_list(data),
    )


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
            live.update(print_list({"operations": [lro_data]}), refresh=True)
            if lro_data.get("done", False):
                break
            time.sleep(SLEEP)
