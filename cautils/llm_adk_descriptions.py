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

# import vertexai
#
# # Initialize the Vertex AI environment with your details
# vertexai.init(project="as-alf-argolis", location="us-central1")

from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

APP_NAME = "bq_agent"
USER_ID = "user1234"
SESSION_ID = "1234"
MODEL_ID = "gemini-2.0-flash"


# Agent
bq_agent = Agent(
    model=MODEL_ID,
    name="bq_agent",
    instruction="""Get table information using your tools""",
    # if description is empty, infer one yourself
    #     instruction="""You are a helpful assistant that provides weather information and analyzes the sentiment of user feedback.
    # **If the user asks about the weather in a specific city, use the 'get_weather_report' tool to retrieve the weather details.**
    # **If the 'get_weather_report' tool returns a 'success' status, provide the weather report to the user.**
    # **If the 'get_weather_report' tool returns an 'error' status, inform the user that the weather information for the specified city is not available and ask if they have another city in mind.**
    # **After providing a weather report, if the user gives feedback on the weather (e.g., 'That's good' or 'I don't like rain'), use the 'analyze_sentiment' tool to understand their sentiment.** Then, briefly acknowledge their sentiment.
    # You can handle these tasks sequentially if needed.""",
    #    tools=[weather_tool, sentiment_tool],
    tools=[FunctionTool(func=metadata_tool.get_sample_rows_json)],
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
        agent=bq_agent,
        app_name=APP_NAME,
        session_service=session_service,
    )

    # Agent Interaction
    query = """get sample rows from table broadcast_tv_radio_station in dataset fcc_political_ads in project as-alf-argolis
    infer the description of each field and subfield based on the sample rows.
    list field names and their descriptions in yaml format
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
