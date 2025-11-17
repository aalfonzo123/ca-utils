import asyncio
import os

from google import genai
from google.genai.types import (
    Content,
    FunctionDeclaration,
    GenerateContentConfig,
    Part,
    Tool,
)

from toolbox_core import ToolboxClient


project = os.environ.get("GCP_PROJECT") or "as-alf-argolis"


prompt = """
  You respond to bigquery metadata questions using your tools"""

queries = [
    """get fields from table broadcast_tv_radio_station in dataset fcc_political_ads in project as-alf-argolis
    list all data you can from each field, including mode and repeated
    list subfields of records
    use a yaml format as output
    if description is empty, infer one yourself
    """
]


async def main():
    async with ToolboxClient("http://127.0.0.1:5000") as toolbox_client:
        # The toolbox_tools list contains Python callables (functions/methods) designed for LLM tool-use
        # integration. While this example uses Google's genai client, these callables can be adapted for
        # various function-calling or agent frameworks. For easier integration with supported frameworks
        # (https://github.com/googleapis/mcp-toolbox-python-sdk/tree/main/packages), use the
        # provided wrapper packages, which handle framework-specific boilerplate.
        toolbox_tools = await toolbox_client.load_toolset("my-toolset")
        genai_client = genai.Client(
            vertexai=True, project=project, location="us-central1"
        )

        genai_tools = [
            Tool(
                function_declarations=[
                    FunctionDeclaration.from_callable_with_api_option(callable=tool)
                ]
            )
            for tool in toolbox_tools
        ]
        history = []
        for query in queries:
            user_prompt_content = Content(
                role="user",
                parts=[Part.from_text(text=query)],
            )
            history.append(user_prompt_content)

            response = genai_client.models.generate_content(
                model="gemini-2.0-flash-001",
                contents=history,
                config=GenerateContentConfig(
                    system_instruction=prompt,
                    tools=genai_tools,
                ),
            )
            history.append(response.candidates[0].content)
            function_response_parts = []

            if response.function_calls:
                for function_call in response.function_calls:
                    fn_name = function_call.name
                    # The tools are sorted alphabetically
                    if fn_name == "bigquery_get_table_info":
                        function_result = await toolbox_tools[0](**function_call.args)
                    else:
                        raise ValueError(f"Function name {fn_name} not present.")

                    function_response = {"result": function_result}
                    function_response_part = Part.from_function_response(
                        name=function_call.name,
                        response=function_response,
                    )
                    function_response_parts.append(function_response_part)

            if function_response_parts:
                tool_response_content = Content(
                    role="tool", parts=function_response_parts
                )
                history.append(tool_response_content)

                response2 = genai_client.models.generate_content(
                    model="gemini-2.0-flash-001",
                    contents=history,
                    config=GenerateContentConfig(
                        tools=genai_tools,
                    ),
                )
                final_model_response_content = response2.candidates[0].content
                history.append(final_model_response_content)
                print(response2.text)
            else:
                print(response.text)


asyncio.run(main())
