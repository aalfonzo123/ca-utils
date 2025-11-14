import json
import requests
from google import auth as google_auth
from google.auth.transport import requests as google_requests
from urllib.parse import urlencode


class GoogleRequestHelper:
    def __init__(self, project_id, base_url):
        self.project_id = project_id
        self.base_url = base_url

    def _get_access_token(self) -> str:
        try:
            credentials, _ = google_auth.default()
            auth_request = google_requests.Request()
            credentials.refresh(auth_request)
            return credentials.token
        except Exception as e:
            raise Exception(
                f"FATAL: Could not get Google credentials. "
                f"Ensure you have run 'gcloud auth application-default login'"
            )

    def _execute_request(
        self, method: str, url: str, data: dict = None, params: dict = None
    ) -> dict:
        """
        Executes an HTTP request using the requests library.

        Args:
            method: The HTTP method (e.g., 'POST', 'GET', 'DELETE', 'PATCH').
            url: The API endpoint URL. Do not include the base
            data: The JSON payload for the request.

        Returns:
            The JSON response from the API.
        """
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
            "X-Goog-User-Project": self.project_id,
        }

        response = requests.request(
            method, self.base_url + url, headers=headers, json=data, params=params
        )
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()

    def get_project_number(self):
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
            "X-Goog-User-Project": self.project_id,
        }

        response = requests.request(
            "GET",
            f"https://cloudresourcemanager.googleapis.com/v1/projects/{self.project_id}",
            headers=headers,
        )
        response.raise_for_status()
        return response.json().get("projectNumber")

    def get(self, url, params: dict = None):
        return self._execute_request("GET", url, params=params)

    def post(self, url, data, params: dict[str, str] = None):
        return self._execute_request("POST", url, data, params)

    def delete(self, url, params: dict = None):
        return self._execute_request("DELETE", url, params=params)

    def patch(self, url, data):
        return self._execute_request("PATCH", url, data)
