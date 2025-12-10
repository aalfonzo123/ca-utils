from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from . import metadata_tool
import logging
from dotenv import load_dotenv
import re
from typing import Optional
import asyncio

logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

load_dotenv()


def extract_yaml_from_text(text: str) -> Optional[str]:
    pattern = re.compile(r"```yaml\s*\n(.*?)\s*```", re.DOTALL)
    match = pattern.search(text)

    if not match:
        return ""

    return match.group(1)
    # 4. Infer descriptions for fields that do not have one already.


def autogen_ds_references(bq_data_sources: list[str], output_path: str):
    query = f"""Given this list of bigquery data sources:
    {bq_data_sources} 

    Follow these steps:
    1. Split each data source using dots. Interpret parts as project_id, dataset_id, table_id
    2. If the table_id is "*" (an asterisk), use the get_table_ids_in_dataset tool to find all
       the table_ids in the specified dataset.
    3. Inspect the resulting tables using the get_table_field_metadata tool.
    4. Output all table fields in a yaml format, infer descriptions for fields that do not have one already,
       follow this example: 
bq:
  tableReferences:
  - datasetId: fcc_political_ads
    projectId: as-alf-argolis
    schema:
      fields:
      - description: "Unique identifier for the station."
        mode: NULLABLE
        name: stationId
        type: STRING
      - description: "Main studio contact information."
        mode: NULLABLE
        name: mainStudioContact
        subfields:
        - description: "Address line 1 of the main studio."
          mode: NULLABLE
          name: address1
          type: STRING
        type: RECORD
    tableId: broadcast_tv_radio_station
  - datasetId: fcc_political_ads
    projectId: as-alf-argolis
    schema:
      fields:
      - mode: NULLABLE
        name: fileHistoryId
        type: STRING
    tableId: file_history
    """
    asyncio.run(execute_bq_llm_prompt(query, output_path))
    logging.info(f"database schema and inferred descriptions written to: {output_path}")


def autogen_schema_relationships(bq_data_sources: list[str], output_path: str):
    query = f"""Given this list of bigquery data sources:
    {bq_data_sources} 

    Follow these steps:
    1. Split each data source using dots. Interpret parts as project_id, dataset_id, table_id
    2. If the table_id is "*" (an asterisk), use the get_table_ids_in_dataset tool to find all
       the table_ids in the specified dataset.
    3. Inspect the resulting tables using the get_table_field_metadata tool, infer relationships
       between the tables. 
    4. Output found relationships in a yaml format, follow this example:
- confidenceScore: 10
  leftSchemaPaths:
    paths:
    - id
    tableFqn: bigquery.googleapis.com/projects/my-project/datasets/my-dataset/tables/material
  rightSchemaPaths:
    paths:
    - material_id
    tableFqn: bigquery.googleapis.com/projects/other-project/datasets/other-dataset/tables/inventory
  sources:
    - LLM_SUGGESTED

    """
    asyncio.run(execute_bq_llm_prompt(query, output_path))
    logging.info(f"inferred database schema relationships written to: {output_path}")


async def execute_bq_llm_prompt(query: str, output_path: str):
    APP_NAME = "bq_agent"
    USER_ID = "user1234"
    SESSION_ID = "1234"
    MODEL_ID = "gemini-2.0-flash"

    bq_agent = Agent(
        model=MODEL_ID,
        name="bq_agent",
        instruction="""Get table information using your tools""",
        tools=[
            FunctionTool(func=metadata_tool.get_table_metadata),
            FunctionTool(func=metadata_tool.get_table_ids_in_dataset),
            FunctionTool(func=metadata_tool.get_sample_rows_json),
            FunctionTool(func=metadata_tool.split_using_dots),
        ],
    )

    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID
    )

    runner = Runner(agent=bq_agent, app_name=APP_NAME, session_service=session_service)

    # print(f"User Query: {query}")
    content = types.Content(role="user", parts=[types.Part(text=query)])

    events = runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    for event in events:
        if event.is_final_response():
            final_response = event.content.parts[0].text
            with open(output_path, "w") as file:
                file.write(extract_yaml_from_text(final_response))
