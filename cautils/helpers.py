from .google_request_helper import GoogleRequestHelper

class GeminiDataAnalyticsRequestHelper(GoogleRequestHelper):
    def __init__(self, project_id, location):
        self.base_url = f"https://geminidataanalytics.googleapis.com/v1beta/projects/{project_id}/locations/{location}/"
        super().__init__(project_id, self.base_url)
        
