"""Unit tests: row hashes and deterministic arrival batches."""

from __future__ import annotations

import pandas as pd

from src.contract import load_contract
from src.ingest import UNPARSED_BATCH, assign_batches, content_checksum, row_hashes


def test_row_hash_is_deterministic_and_content_sensitive():
    frame = pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]})
    first, second = row_hashes(frame), row_hashes(frame.copy())
    assert first.tolist() == second.tolist()
    assert first.iloc[0] != first.iloc[1]
    changed = frame.copy()
    changed.loc[0, "b"] = "z"
    assert row_hashes(changed).iloc[0] != first.iloc[0]


def test_row_hash_distinguishes_missing_from_empty_text():
    with_missing = pd.DataFrame({"a": ["1"], "b": [None]})
    with_empty = pd.DataFrame({"a": ["1"], "b": [""]})
    assert row_hashes(with_missing).iloc[0] != row_hashes(with_empty).iloc[0]


def test_content_checksum_ignores_row_order():
    hashes = pd.Series(["b", "a", "c"])
    assert content_checksum(hashes) == content_checksum(hashes.iloc[::-1])


def test_quarter_and_month_batches(workspace):
    contract = load_contract(workspace)
    frame = pd.DataFrame({"Date": ["2021-03-31", "2021-04-01", "bad-date"],
                          "Time": ["23:45:00", "00:00:00", "00:00:00"]})
    quarters, _ = assign_batches(frame, contract, "quarter")
    months, _ = assign_batches(frame, contract, "month")
    assert quarters.tolist() == ["2021Q1", "2021Q2", UNPARSED_BATCH]
    assert months.tolist() == ["2021-03", "2021-04", UNPARSED_BATCH]
