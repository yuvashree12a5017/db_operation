import pytest
from pydantic import ValidationError

from common.processors.action_engine.models import DbToDbRequest, TargetColumnMapping


def _request():
    return {
        "version": "1.0",
        "sources": [
            {
                "name": "source_a",
                "type": "postgres",
                "connection": {"sqlalchemy_url": "postgresql+asyncpg://localhost/db"},
                "dataset": {"schema": "public", "table": "users", "columns": ["id"]},
            }
        ],
        "actions": [
            {
                "action": "action-sample-limit",
                "name": "final",
                "input": ["source_a"],
                "config": {"limit": 10},
            }
        ],
        "target": {
            "type": "postgres",
            "connection": {"sqlalchemy_url": "postgresql://localhost/db"},
            "schema": "analytics",
            "table": "users",
            "write_mode": "overwrite",
            "column_mappings": [{"target_column": "customer_id", "source_column": "id"}],
        },
        "node_runId": "run-1",
    }


def test_valid_request_requires_explicit_mapping():
    request = DbToDbRequest.model_validate(_request())
    assert request.target.column_mappings[0].target_column == "customer_id"


def test_target_mapping_requires_exactly_one_provider():
    with pytest.raises(ValidationError, match="exactly one"):
        TargetColumnMapping(target_column="id", source_column="id", literal=1)


def test_unknown_action_config_field_is_rejected():
    payload = _request()
    payload["actions"][0]["config"]["unexpected"] = True
    with pytest.raises(ValidationError, match="unexpected"):
        DbToDbRequest.model_validate(payload)


def test_forward_dataset_reference_is_rejected():
    payload = _request()
    payload["actions"][0]["input"] = ["later"]
    with pytest.raises(ValidationError, match="unavailable"):
        DbToDbRequest.model_validate(payload)


def test_duplicate_target_columns_are_rejected():
    payload = _request()
    payload["target"]["column_mappings"].append(
        {"target_column": "customer_id", "literal": 2}
    )
    with pytest.raises(ValidationError, match="duplicate"):
        DbToDbRequest.model_validate(payload)
