from . import data_agent


def test_gen_schema_relationships():
    print(
        data_agent._gen_schema_relationships(
            project_id="as-alf-argolis",
            location="global",
            data_source_references_path="sample_definitions/sample1/datasourceReferences.yaml",
        )
    )


if __name__ == "__main__":
    test_gen_schema_relationships()
