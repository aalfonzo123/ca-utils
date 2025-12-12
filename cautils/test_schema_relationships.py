from . import data_agent

import os
from dotenv import load_dotenv
import yaml

# Load variables from .env file
load_dotenv()


def test_gen_schema_relationships():
    print(
        data_agent._gen_schema_relationships(
            project_id=os.environ["GOOGLE_CLOUD_PROJECT"],
            location=os.environ["GOOGLE_CLOUD_DER"],
            data_source_references_path="sample_definitions/sample1/datasourceReferences.yaml",
        )
    )


def test_gen_example_queries():
    print(
        yaml.safe_dump(
            data_agent._gen_example_queries(
                project_id=os.environ["GOOGLE_CLOUD_PROJECT"],
                location=os.environ["GOOGLE_CLOUD_DER"],
                data_source_references_path="sample_definitions/sample1/datasourceReferences.yaml",
            )
        )
    )


if __name__ == "__main__":
    test_gen_example_queries()
