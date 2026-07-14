import polars as pl

from common.processors.action_engine import actions_dataset as dataset
from common.processors.action_engine import actions_multi as multi


def lazy(data):
    return pl.DataFrame(data).lazy()


def test_filter_and_hash():
    filtered = dataset.action_filter_dataset(
        [lazy({"id": [1, 2], "status": ["ACTIVE", "INACTIVE"]})],
        {"condition": {"column": "status", "op": "eq", "value": "ACTIVE"}},
    )
    result = dataset.action_row_hash(
        [filtered], {"columns": ["id"], "target_column": "row_hash", "algo": "sha256"}
    ).collect()
    assert result["id"].to_list() == [1]
    assert len(result["row_hash"][0]) == 64


def test_merge_join_and_anti_join():
    left = lazy({"id": [1, 2], "name": ["a", "b"]})
    right = lazy({"user_id": [1], "balance": [10]})
    joined = multi.action_merge_join(
        [left, right],
        {"type": "inner", "on": [{"left": "id", "right": "user_id"}], "suffix": "_right"},
    ).collect()
    assert joined["balance"].to_list() == [10]
    missing = multi.action_anti_join([left, right], {"on": [{"left": "id", "right": "user_id"}]}).collect()
    assert missing["id"].to_list() == [2]
