from pathlib import Path
from cautils.metadata_tool import (
    _get_column_fqns_from_datasource_references,
    _reverse_autogen_tables,
)
import yaml

import pytest


@pytest.fixture
def empty_dsr_yaml(tmp_path):
    test_dir = tmp_path / "etc"
    test_dir.mkdir()
    dsr_path = test_dir / "dsr.yaml"
    with open(dsr_path, "w") as f:
        f.write("")
    return dsr_path


def test_reverse_autogen_tables():
    expected_tables = ["as-alf-argolis.alf_test.chain", "as-alf-argolis.alf_test.store"]
    dsr_path = Path("cautils/tests/dsr.yaml")
    actual_tables = _reverse_autogen_tables(dsr_path)
    assert actual_tables == expected_tables


def test_get_column_fqns_from_datasource_references():
    expected_fqns = {
        "bigquery.googleapis.com/projects/as-alf-argolis/datasets/alf_test/tables/chain:id",
        "bigquery.googleapis.com/projects/as-alf-argolis/datasets/alf_test/tables/chain:name",
        "bigquery.googleapis.com/projects/as-alf-argolis/datasets/alf_test/tables/store:chain_info",
        "bigquery.googleapis.com/projects/as-alf-argolis/datasets/alf_test/tables/store:chain_info.chain_id",
        "bigquery.googleapis.com/projects/as-alf-argolis/datasets/alf_test/tables/store:id",
        "bigquery.googleapis.com/projects/as-alf-argolis/datasets/alf_test/tables/store:name",
    }
    dsr_path = Path("cautils/tests/dsr.yaml")
    actual_fqns = _get_column_fqns_from_datasource_references(dsr_path)
    assert actual_fqns == expected_fqns


def test_get_column_fqns_from_empty_datasource_references(empty_dsr_yaml):
    dsr_path = empty_dsr_yaml

    actual_fqns = _get_column_fqns_from_datasource_references(dsr_path)
    assert actual_fqns == set()

    # Test with a YAML file with no 'bq' key
    with open(dsr_path, "w") as f:
        yaml.dump({"not_bq": {}}, f)
    actual_fqns = _get_column_fqns_from_datasource_references(dsr_path)
    assert actual_fqns == set()

    # Test with a YAML file with 'bq' but no 'tableReferences'
    with open(dsr_path, "w") as f:
        yaml.dump({"bq": {"not_tableReferences": {}}}, f)
    actual_fqns = _get_column_fqns_from_datasource_references(dsr_path)
    assert actual_fqns == set()


def test_reverse_autogen_tables_empty(empty_dsr_yaml):
    dsr_path = empty_dsr_yaml
    with pytest.raises(ValueError, match="missing bq.tableReferences in file"):
        _reverse_autogen_tables(dsr_path)

    # Test with a YAML file with no 'bq' key
    with open(dsr_path, "w") as f:
        yaml.dump({"not_bq": {}}, f)
    with pytest.raises(ValueError, match="missing bq.tableReferences in file"):
        _reverse_autogen_tables(dsr_path)

    # Test with a YAML file with 'bq' but no 'tableReferences'
    with open(dsr_path, "w") as f:
        yaml.dump({"bq": {"not_tableReferences": {}}}, f)
    with pytest.raises(ValueError, match="missing bq.tableReferences in file"):
        _reverse_autogen_tables(dsr_path)
