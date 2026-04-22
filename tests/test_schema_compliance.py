"""
Schema compliance tests for ShovelSense data.

Tests:
- Required columns present
- Column data types correct
- Primary key uniqueness
"""

import pytest

# Expected schemas: column -> expected dtype
EXPECTED_SCHEMAS = {
    "block_model": {
        "block_id": "object",
        "bench": "int64",
        "easting": "float64",
        "northing": "float64",
        "elevation": "float64",
        "planned_cu_grade": "float64",
        "planned_fe_grade": "float64",
        "planned_classification": "object",
        "geological_domain": "object",
        "is_dyke": "bool",
        "blast_movement_m": "float64",
        "chalcopyrite_pct": "float64",
        "bornite_pct": "float64",
        "surface_volume_correlation": "float64",
        "nugget_effect_variance": "float64",
        "vein_density_class": "object",
    },
    "shovels": {
        "shovel_id": "object",
        "shovel_type": "object",
        "bucket_capacity_m3": "int64",
        "minesense_equipped": "bool",
        "sensor_heads": "int64",
        "commissioned_date": "object",
        "xrf_detector_type": "object",
        "detection_limit_ppm": "int64",
        "penetration_depth_mm": "float64",
    },
    "trucks": {
        "truck_id": "object",
        "truck_model": "object",
        "payload_capacity_tonnes": "int64",
        "fms_system": "object",
    },
    "bucket_measurements": {
        "measurement_id": "object",
        "timestamp": "object",
        "shovel_id": "object",
        "bucket_number": "int64",
        "truck_id": "object",
        "block_id": "object",
        "cu_grade_pct": "float64",
        "fe_grade_pct": "float64",
        "zn_grade_ppm": "float64",
        "as_grade_ppm": "float64",
        "laser_fill_level_pct": "float64",
        "xrf_confidence": "float64",
        "sensor_head_1_active": "bool",
        "sensor_head_2_active": "bool",
        "matrix_effect_factor": "float64",
        "heterogeneity_error_est": "float64",
        "geological_domain": "object",
    },
    "truck_loads": {
        "load_id": "object",
        "timestamp": "object",
        "truck_id": "object",
        "shovel_id": "object",
        "block_id": "object",
        "n_buckets": "int64",
        "avg_cu_grade_pct": "float64",
        "avg_fe_grade_pct": "float64",
        "planned_classification": "object",
        "shovelsense_classification": "object",
        "diversion_type": "object",
        "destination": "object",
        "payload_tonnes": "float64",
        "cycle_time_minutes": "float64",
        "estimated_cu_tonnes": "float64",
        "geological_domain": "object",
        "surface_volume_correlation": "float64",
        "avg_xrf_confidence": "float64",
    },
    "shift_summaries": {
        "date": "object",
        "shift": "object",
        "shovel_id": "object",
        "n_trucks": "int64",
        "avg_cu_grade": "float64",
        "total_tonnes": "float64",
        "total_cu_tonnes": "float64",
        "n_diversions": "int64",
        "avg_surface_volume_corr": "float64",
        "avg_xrf_confidence": "float64",
        "diversion_rate": "float64",
        "f1_factor_estimate": "float64",
    },
}


class TestBlockModelSchema:
    def test_required_columns_present(self, block_model):
        expected = set(EXPECTED_SCHEMAS["block_model"].keys())
        actual = set(block_model.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_primary_key_unique(self, block_model):
        duplicates = block_model["block_id"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate block_id values"


class TestShovelsSchema:
    def test_required_columns_present(self, shovels):
        expected = set(EXPECTED_SCHEMAS["shovels"].keys())
        actual = set(shovels.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_primary_key_unique(self, shovels):
        duplicates = shovels["shovel_id"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate shovel_id values"


class TestTrucksSchema:
    def test_required_columns_present(self, trucks):
        expected = set(EXPECTED_SCHEMAS["trucks"].keys())
        actual = set(trucks.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_primary_key_unique(self, trucks):
        duplicates = trucks["truck_id"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate truck_id values"


class TestBucketMeasurementsSchema:
    def test_required_columns_present(self, bucket_measurements):
        expected = set(EXPECTED_SCHEMAS["bucket_measurements"].keys())
        actual = set(bucket_measurements.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_primary_key_unique(self, bucket_measurements):
        duplicates = bucket_measurements["measurement_id"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate measurement_id values"


class TestTruckLoadsSchema:
    def test_required_columns_present(self, truck_loads):
        expected = set(EXPECTED_SCHEMAS["truck_loads"].keys())
        actual = set(truck_loads.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_primary_key_unique(self, truck_loads):
        duplicates = truck_loads["load_id"].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate load_id values"


class TestShiftSummariesSchema:
    def test_required_columns_present(self, shift_summaries):
        expected = set(EXPECTED_SCHEMAS["shift_summaries"].keys())
        actual = set(shift_summaries.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_composite_primary_key_unique(self, shift_summaries):
        duplicates = shift_summaries.duplicated(subset=["date", "shift", "shovel_id"]).sum()
        assert duplicates == 0, f"Found {duplicates} duplicate (date, shift, shovel_id) combinations"
