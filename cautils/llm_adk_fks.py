# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from . import metadata_tool
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# import vertexai
#
# # Initialize the Vertex AI environment with your details
# vertexai.init(project="as-alf-argolis", location="us-central1")

# Load variables from .env file
load_dotenv()

APP_NAME = "weather_sentiment_agent"
USER_ID = "user1234"
SESSION_ID = "1234"
MODEL_ID = "gemini-2.0-flash"


# Agent
weather_sentiment_agent = Agent(
    model=MODEL_ID,
    name="weather_sentiment_agent",
    instruction="""Get table information using your tools""",
    tools=[
        FunctionTool(func=metadata_tool.get_table_field_metadata),
        FunctionTool(func=metadata_tool.get_sample_rows_json),
    ],
)


async def main():
    """Main function to run the agent asynchronously."""
    # Session and Runner Setup
    session_service = InMemorySessionService()
    # Use 'await' to correctly create the session
    await session_service.create_session(
        app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID
    )

    runner = Runner(
        agent=weather_sentiment_agent,
        app_name=APP_NAME,
        session_service=session_service,
    )

    query = """get field metadata and sample rows for table broadcast_tv_radio_station in dataset fcc_political_ads in project as-alf-argolis
    list all fields, and all data you can from each field.
    list subfields of records, use the "subfields" key for those.
    infer descriptions for fields and add that to the output.
    use a yaml format as output
    """
    print(f"User Query: {query}")
    content = types.Content(role="user", parts=[types.Part(text=query)])

    # The runner's run method handles the async loop internally
    events = runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    for event in events:
        if event.is_final_response():
            final_response = event.content.parts[0].text
            print("Agent Response:", final_response)


# Standard way to run the main async function
if __name__ == "__main__":
    asyncio.run(main())
