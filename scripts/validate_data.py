"""
Validate generated ShovelSense parquet data for schema, referential integrity,
value ranges, and business rules.

Usage:
    python scripts/validate_data.py

Exit codes:
    0 - All validations passed
    1 - Errors found (critical issues)
    2 - Warnings found (non-critical issues)
"""

import sys
from pathlib import Path

import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================
DATA_DIR = Path("data/generated")

# Expected schemas (column -> dtype)
SCHEMAS = {
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

# Primary keys
PRIMARY_KEYS = {
    "block_model": "block_id",
    "shovels": "shovel_id",
    "trucks": "truck_id",
    "bucket_measurements": "measurement_id",
    "truck_loads": "load_id",
    "shift_summaries": ["date", "shift", "shovel_id"],
}

# Foreign key relationships: (table, fk_column) -> (ref_table, ref_column)
FOREIGN_KEYS = {
    ("bucket_measurements", "shovel_id"): ("shovels", "shovel_id"),
    ("bucket_measurements", "truck_id"): ("trucks", "truck_id"),
    ("bucket_measurements", "block_id"): ("block_model", "block_id"),
    ("truck_loads", "shovel_id"): ("shovels", "shovel_id"),
    ("truck_loads", "truck_id"): ("trucks", "truck_id"),
    ("truck_loads", "block_id"): ("block_model", "block_id"),
    ("shift_summaries", "shovel_id"): ("shovels", "shovel_id"),
}

# Value range constraints
VALUE_RANGES = {
    "block_model": {
        "planned_cu_grade": (0, 2.1),
        "planned_fe_grade": (0, None),
        "chalcopyrite_pct": (0, 100),
        "bornite_pct": (0, 100),
        "surface_volume_correlation": (0, 1),
    },
    "bucket_measurements": {
        "cu_grade_pct": (0, None),
        "xrf_confidence": (0.70, 0.99),
        "laser_fill_level_pct": (0, 100),
    },
    "truck_loads": {
        "avg_cu_grade_pct": (0, None),
        "payload_tonnes": (0, 500),
        "avg_xrf_confidence": (0.70, 0.99),
    },
    "shift_summaries": {
        "diversion_rate": (0, 1),
    },
}

# Business rule thresholds
DIVERSION_RATE_MIN = 0.08  # 8%
DIVERSION_RATE_MAX = 0.15  # 15%
AVG_XRF_CONFIDENCE_MIN = 0.85
SENSOR_FAILURE_MAX = 0.10  # 10%


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================
class ValidationResult:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, msg: str):
        self.errors.append(msg)
        print(f"  ERROR: {msg}")

    def warn(self, msg: str):
        self.warnings.append(msg)
        print(f"  WARNING: {msg}")

    def ok(self, msg: str):
        print(f"  OK: {msg}")

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


def load_dataframes() -> dict[str, pd.DataFrame]:
    """Load all parquet files into a dict."""
    dfs = {}
    for name in SCHEMAS:
        path = DATA_DIR / f"{name}.parquet"
        if path.exists():
            dfs[name] = pd.read_parquet(path)
        else:
            dfs[name] = None
    return dfs


def validate_files_exist(dfs: dict, result: ValidationResult):
    """Check all required files exist."""
    print("\n1. Checking file existence...")
    for name, df in dfs.items():
        if df is None:
            result.error(f"Missing file: {DATA_DIR}/{name}.parquet")
        else:
            result.ok(f"{name}.parquet exists ({len(df):,} rows)")


def validate_schemas(dfs: dict, result: ValidationResult):
    """Validate all columns present and correct types."""
    print("\n2. Validating schemas...")
    for name, expected in SCHEMAS.items():
        df = dfs.get(name)
        if df is None:
            continue

        # Check required columns
        missing = set(expected.keys()) - set(df.columns)
        if missing:
            result.error(f"{name}: Missing columns: {missing}")

        extra = set(df.columns) - set(expected.keys())
        if extra:
            result.warn(f"{name}: Extra columns (not validated): {extra}")

        # Check dtypes
        for col, expected_dtype in expected.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if actual_dtype != expected_dtype:
                    result.warn(f"{name}.{col}: Expected {expected_dtype}, got {actual_dtype}")

        if not missing:
            result.ok(f"{name}: All {len(expected)} required columns present")


def validate_primary_keys(dfs: dict, result: ValidationResult):
    """Validate PK uniqueness."""
    print("\n3. Validating primary key uniqueness...")
    for name, pk in PRIMARY_KEYS.items():
        df = dfs.get(name)
        if df is None:
            continue

        if isinstance(pk, list):
            duplicates = df.duplicated(subset=pk, keep=False).sum()
            pk_str = ", ".join(pk)
        else:
            duplicates = df.duplicated(subset=[pk], keep=False).sum()
            pk_str = pk

        if duplicates > 0:
            result.error(f"{name}: {duplicates} duplicate values for PK ({pk_str})")
        else:
            result.ok(f"{name}: PK ({pk_str}) is unique")


def validate_foreign_keys(dfs: dict, result: ValidationResult):
    """Validate FK references resolve."""
    print("\n4. Validating foreign key integrity...")
    for (table, fk_col), (ref_table, ref_col) in FOREIGN_KEYS.items():
        df = dfs.get(table)
        ref_df = dfs.get(ref_table)

        if df is None or ref_df is None:
            continue

        valid_values = set(ref_df[ref_col])
        actual_values = set(df[fk_col])
        orphaned = actual_values - valid_values

        if orphaned:
            result.error(f"{table}.{fk_col} -> {ref_table}.{ref_col}: {len(orphaned)} orphaned values")
        else:
            result.ok(f"{table}.{fk_col} -> {ref_table}.{ref_col}: All references valid")


def validate_value_ranges(dfs: dict, result: ValidationResult):
    """Validate numeric values within expected ranges."""
    print("\n5. Validating value ranges...")
    for table, ranges in VALUE_RANGES.items():
        df = dfs.get(table)
        if df is None:
            continue

        for col, (min_val, max_val) in ranges.items():
            if col not in df.columns:
                continue

            violations = 0
            if min_val is not None:
                violations += (df[col] < min_val).sum()
            if max_val is not None:
                violations += (df[col] > max_val).sum()

            range_str = f"[{min_val}, {max_val}]"
            if violations > 0:
                result.error(f"{table}.{col}: {violations} values outside {range_str}")
            else:
                result.ok(f"{table}.{col}: All values within {range_str}")


def validate_business_rules(dfs: dict, result: ValidationResult):
    """Validate business rules specific to ShovelSense."""
    print("\n6. Validating business rules...")

    # Diversion rate check
    truck_loads = dfs.get("truck_loads")
    if truck_loads is not None:
        total = len(truck_loads)
        diversions = (truck_loads["diversion_type"] != "ALIGNED").sum()
        rate = diversions / total

        if rate < DIVERSION_RATE_MIN or rate > DIVERSION_RATE_MAX:
            result.warn(
                f"Diversion rate {rate:.1%} outside expected range "
                f"[{DIVERSION_RATE_MIN:.0%}, {DIVERSION_RATE_MAX:.0%}]"
            )
        else:
            result.ok(f"Diversion rate {rate:.1%} within expected range")

    # XRF confidence check
    bucket_measurements = dfs.get("bucket_measurements")
    if bucket_measurements is not None:
        avg_conf = bucket_measurements["xrf_confidence"].mean()
        if avg_conf < AVG_XRF_CONFIDENCE_MIN:
            result.warn(f"Average XRF confidence {avg_conf:.3f} below {AVG_XRF_CONFIDENCE_MIN}")
        else:
            result.ok(f"Average XRF confidence {avg_conf:.3f} >= {AVG_XRF_CONFIDENCE_MIN}")

    # Sensor failure rate
    if bucket_measurements is not None:
        sensor_failures = (~bucket_measurements["sensor_head_2_active"]).sum()
        failure_rate = sensor_failures / len(bucket_measurements)
        if failure_rate > SENSOR_FAILURE_MAX:
            result.warn(f"Sensor failure rate {failure_rate:.1%} exceeds {SENSOR_FAILURE_MAX:.0%}")
        else:
            result.ok(f"Sensor failure rate {failure_rate:.1%} <= {SENSOR_FAILURE_MAX:.0%}")


def main():
    print("=" * 70)
    print("ShovelSense Data Validation")
    print("=" * 70)

    if not DATA_DIR.exists():
        print(f"\nERROR: Data directory {DATA_DIR} does not exist.")
        print("Run 'just generate-data' first.")
        sys.exit(1)

    result = ValidationResult()
    dfs = load_dataframes()

    validate_files_exist(dfs, result)
    validate_schemas(dfs, result)
    validate_primary_keys(dfs, result)
    validate_foreign_keys(dfs, result)
    validate_value_ranges(dfs, result)
    validate_business_rules(dfs, result)

    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    print(f"Errors:   {len(result.errors)}")
    print(f"Warnings: {len(result.warnings)}")

    if result.passed:
        if result.has_warnings:
            print("\nValidation PASSED with warnings.")
            sys.exit(2)
        else:
            print("\nValidation PASSED.")
            sys.exit(0)
    else:
        print("\nValidation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
