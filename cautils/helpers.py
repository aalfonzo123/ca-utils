from .google_request_helper import GoogleRequestHelper

from rich.prompt import Prompt
from typing import Callable
from requests.exceptions import HTTPError
from rich import print as rprint


class GeminiDataAnalyticsRequestHelper(GoogleRequestHelper):
    def __init__(self, project_id, location):
        self.base_url = f"https://geminidataanalytics.googleapis.com/v1beta/projects/{project_id}/locations/{location}/"
        super().__init__(project_id, self.base_url)


def paginate(retriever: Callable, printer: Callable):
    page_size = 5
    data = retriever({"pageSize": page_size})
    try:
        while True:
            printer(data)
            if next_page_token := data.get("nextPageToken"):
                show_next = Prompt.ask(
                    "show next page?", choices=["y", "n"], default="n"
                )
                if show_next == "n":
                    break
                data = retriever({"pageToken": next_page_token, "pageSize": page_size})
            else:
                break
    except HTTPError as e:
        rprint(f"[bright_red]{e.response.text}[/bright_red]")
