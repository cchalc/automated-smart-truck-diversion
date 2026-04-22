"""
Pytest fixtures for loading ShovelSense parquet data.
"""

from pathlib import Path

import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent.parent / "data" / "generated"


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Return the data directory path."""
    return DATA_DIR


@pytest.fixture(scope="session")
def block_model() -> pd.DataFrame:
    """Load block_model.parquet."""
    return pd.read_parquet(DATA_DIR / "block_model.parquet")


@pytest.fixture(scope="session")
def shovels() -> pd.DataFrame:
    """Load shovels.parquet."""
    return pd.read_parquet(DATA_DIR / "shovels.parquet")


@pytest.fixture(scope="session")
def trucks() -> pd.DataFrame:
    """Load trucks.parquet."""
    return pd.read_parquet(DATA_DIR / "trucks.parquet")


@pytest.fixture(scope="session")
def bucket_measurements() -> pd.DataFrame:
    """Load bucket_measurements.parquet."""
    return pd.read_parquet(DATA_DIR / "bucket_measurements.parquet")


@pytest.fixture(scope="session")
def truck_loads() -> pd.DataFrame:
    """Load truck_loads.parquet."""
    return pd.read_parquet(DATA_DIR / "truck_loads.parquet")


@pytest.fixture(scope="session")
def shift_summaries() -> pd.DataFrame:
    """Load shift_summaries.parquet."""
    return pd.read_parquet(DATA_DIR / "shift_summaries.parquet")


@pytest.fixture(scope="session")
def all_dataframes(
    block_model, shovels, trucks, bucket_measurements, truck_loads, shift_summaries
) -> dict[str, pd.DataFrame]:
    """Return all dataframes as a dict."""
    return {
        "block_model": block_model,
        "shovels": shovels,
        "trucks": trucks,
        "bucket_measurements": bucket_measurements,
        "truck_loads": truck_loads,
        "shift_summaries": shift_summaries,
    }
