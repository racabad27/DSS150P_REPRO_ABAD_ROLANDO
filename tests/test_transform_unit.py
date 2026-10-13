"""Unit tests: contract parsing rules (quarantine reasons) and derived features."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.contract import load_contract
from src.transform import add_features, standardise_rows

GOOD = {"Date": "2021-01-04", "Time": "10:15:00", "Factory": "Factory_B", "Region": "Urban",
        "Shift": "Night", "Workers": "7.0", "Exp": "Senior", "Training": "Yes", "Temp": "26.49",
        "Pressure": "25.31", "Humidity": "54.18", "Vibration": "1.63", "Speed": "1007.06",
        "Age": "17", "Service_Days": "202", "Gas": "8.9", "Sparks": "1", "Alarm": "Off",
        "Risk": "56.47", "Accident": "0"}


def _raw(**changes):
    row = dict(GOOD)
    row.update(changes)
    return pd.DataFrame([row])


@pytest.mark.parametrize("changes, reason", [
    ({"Date": "04/01/2021"}, "INVALID_TIMESTAMP"),          # spreadsheet re-save format
    ({"Time": "10:07:00"}, "OFF_CADENCE"),
    ({"Temp": ""}, "MISSING_REQUIRED"),
    ({"Factory": "Factory_Z"}, "INVALID_CATEGORY"),
    ({"Workers": "7.5"}, "INVALID_TYPE"),
    ({"Pressure": "abc"}, "INVALID_TYPE"),
    ({"Humidity": "120"}, "OUT_OF_RANGE"),
    ({"Accident": "1"}, "ACCIDENT_RISK_MISMATCH"),         # Risk 56.47 < 75 but accident = 1
    ({"Accident": "2"}, "INVALID_CATEGORY"),
])
def test_contract_violations_get_a_reason_code(workspace, changes, reason):
    _, issues = standardise_rows(_raw(**changes), load_contract(workspace))
    assert reason in set(issues["reason_code"])


def test_valid_row_passes_and_is_typed(workspace):
    typed, issues = standardise_rows(_raw(), load_contract(workspace))
    assert issues.empty
    assert typed.loc[0, "workers"] == 7                     # "7.0" is a whole number
    assert typed.loc[0, "reading_ts"] == pd.Timestamp("2021-01-04 10:15:00")
    assert typed.loc[0, "factory"] == "Factory_B"


def test_blank_workers_is_allowed_in_staging(workspace):
    typed, issues = standardise_rows(_raw(Workers=""), load_contract(workspace))
    assert issues.empty
    assert pd.isna(typed.loc[0, "workers"])


def test_risk_exactly_75_accepts_both_accident_values(workspace):
    contract = load_contract(workspace)
    for accident in ("0", "1"):
        _, issues = standardise_rows(_raw(Risk="75.0", Accident=accident), contract)
        assert issues.empty


def test_derived_features_match_the_notebook_definitions():
    frame = pd.DataFrame({
        "reading_ts": pd.to_datetime(["2021-01-03 23:45:00", "2021-01-04 06:15:00"]),  # Sunday, Monday
        "workers": [10, 20], "sparks": [3, 0], "temp": [20.5, 30.0], "pressure": [10.0, 2.5],
        "vibration": [1.5, 2.0], "speed": [1000.0, 2000.0],
    })
    out = add_features(frame)
    assert out["day_of_week"].tolist() == [6, 0]            # Monday = 0, Sunday = 6
    assert out["is_weekend"].tolist() == [1, 0]
    assert out["hour_of_day"].tolist() == [23, 6]
    assert out["month"].tolist() == [1, 1]
    assert out["workers_sparks_interaction"].tolist() == [30.0, 0.0]
    assert np.allclose(out["temp_pressure_interaction"], [205.0, 75.0])
    assert np.allclose(out["vibration_speed_interaction"], [1500.0, 4000.0])
