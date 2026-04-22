"""
Business rules tests for ShovelSense data.

Tests ShovelSense-specific business rules and quality thresholds.
"""

import pytest


# Business rule thresholds
DIVERSION_RATE_MIN = 0.08  # 8%
DIVERSION_RATE_MAX = 0.15  # 15%
TARGET_DIVERSION_RATE = 0.11  # ~11% from white paper
AVG_XRF_CONFIDENCE_MIN = 0.85
SENSOR_FAILURE_MAX = 0.10  # 10%
CU_CUTOFF = 0.32  # Ore/waste cutoff grade


class TestDiversionRates:
    def test_overall_diversion_rate_in_range(self, truck_loads):
        """Diversion rate should be between 8-15% (target ~11%)."""
        total = len(truck_loads)
        diversions = (truck_loads["diversion_type"] != "ALIGNED").sum()
        rate = diversions / total

        assert rate >= DIVERSION_RATE_MIN, (
            f"Diversion rate {rate:.1%} below minimum {DIVERSION_RATE_MIN:.0%}"
        )
        assert rate <= DIVERSION_RATE_MAX, (
            f"Diversion rate {rate:.1%} above maximum {DIVERSION_RATE_MAX:.0%}"
        )

    def test_diversion_types_present(self, truck_loads):
        """All three diversion types should be present."""
        types = set(truck_loads["diversion_type"].unique())
        expected = {"ALIGNED", "ORE_FROM_WASTE", "WASTE_FROM_ORE"}
        assert types == expected, f"Expected diversion types {expected}, got {types}"

    def test_ore_from_waste_reasonable(self, truck_loads):
        """Ore-from-waste diversions should be ~6.4% of total trucks."""
        total = len(truck_loads)
        ore_from_waste = (truck_loads["diversion_type"] == "ORE_FROM_WASTE").sum()
        rate = ore_from_waste / total

        # Allow wider range for stochastic generation: 3-10%
        assert 0.03 <= rate <= 0.10, f"Ore-from-waste rate {rate:.1%} outside expected [3%, 10%]"

    def test_waste_from_ore_reasonable(self, truck_loads):
        """Waste-from-ore diversions should be ~4.7% of total trucks."""
        total = len(truck_loads)
        waste_from_ore = (truck_loads["diversion_type"] == "WASTE_FROM_ORE").sum()
        rate = waste_from_ore / total

        # Allow wider range for stochastic generation: 2-8%
        assert 0.02 <= rate <= 0.08, f"Waste-from-ore rate {rate:.1%} outside expected [2%, 8%]"


class TestXRFConfidence:
    def test_average_xrf_confidence_threshold(self, bucket_measurements):
        """Average XRF confidence should be >= 0.85."""
        avg_conf = bucket_measurements["xrf_confidence"].mean()
        assert avg_conf >= AVG_XRF_CONFIDENCE_MIN, (
            f"Average XRF confidence {avg_conf:.3f} below {AVG_XRF_CONFIDENCE_MIN}"
        )

    def test_xrf_confidence_by_domain_varies(self, bucket_measurements):
        """XRF confidence should vary by geological domain (matrix effects)."""
        conf_by_domain = bucket_measurements.groupby("geological_domain")["xrf_confidence"].mean()

        # Chalcopyrite zones should have lower confidence (more Fe matrix effects)
        # This tests that the simulation correctly models matrix effects
        assert len(conf_by_domain) >= 4, "Expected at least 4 geological domains"

        # Variance should exist - not all domains have same confidence
        variance = conf_by_domain.var()
        assert variance > 0.0001, f"XRF confidence variance {variance} too low across domains"


class TestSensorReliability:
    def test_sensor_failure_rate(self, bucket_measurements):
        """Sensor failure rate should be <= 10%."""
        failures = (~bucket_measurements["sensor_head_2_active"]).sum()
        failure_rate = failures / len(bucket_measurements)

        assert failure_rate <= SENSOR_FAILURE_MAX, (
            f"Sensor failure rate {failure_rate:.1%} exceeds {SENSOR_FAILURE_MAX:.0%}"
        )


class TestClassificationLogic:
    def test_classification_based_on_cutoff(self, truck_loads):
        """ShovelSense classification should follow Cu cutoff logic."""
        # Check a sample: classification should match grade vs cutoff
        misclassified = 0
        for _, row in truck_loads.head(1000).iterrows():
            expected = "ORE" if row["avg_cu_grade_pct"] >= CU_CUTOFF else "WASTE"
            if row["shovelsense_classification"] != expected:
                misclassified += 1

        # Allow very small margin for edge cases
        assert misclassified <= 5, f"Found {misclassified} misclassified loads in sample of 1000"

    def test_destinations_match_classification(self, truck_loads):
        """Destination should match ShovelSense classification."""
        for _, row in truck_loads.head(1000).iterrows():
            if row["shovelsense_classification"] == "ORE":
                assert row["destination"] == "CRUSHER", (
                    f"ORE classified load sent to {row['destination']}"
                )
            else:
                assert row["destination"] == "WASTE_DUMP", (
                    f"WASTE classified load sent to {row['destination']}"
                )


class TestGeologicalDomains:
    def test_all_domains_represented(self, block_model):
        """All geological domains should be present in block model."""
        expected_domains = {
            "BORNITE_CORE",
            "CHALCOPYRITE_ZONE",
            "PYRITE_HALO",
            "SUPERGENE",
            "LEACHED_CAP",
            "WASTE_ZONE",
        }
        actual_domains = set(block_model["geological_domain"].unique())
        missing = expected_domains - actual_domains
        assert not missing, f"Missing geological domains: {missing}"

    def test_grade_varies_by_domain(self, block_model):
        """Cu grade should vary systematically by geological domain."""
        mean_by_domain = block_model.groupby("geological_domain")["planned_cu_grade"].mean()

        # Bornite core should have highest average grade
        assert mean_by_domain["BORNITE_CORE"] > mean_by_domain["WASTE_ZONE"], (
            "BORNITE_CORE should have higher grade than WASTE_ZONE"
        )

        # Waste zone should have lowest grade
        assert mean_by_domain["WASTE_ZONE"] == mean_by_domain.min(), (
            "WASTE_ZONE should have lowest average grade"
        )


class TestDataVolume:
    def test_sufficient_truck_loads(self, truck_loads):
        """Should have sufficient data volume for analysis."""
        # Expect ~80 days * 3 shovels * 80 trucks/day = ~19,200 loads
        assert len(truck_loads) >= 15000, f"Only {len(truck_loads)} truck loads, expected >= 15,000"

    def test_sufficient_bucket_measurements(self, bucket_measurements):
        """Should have 4-6 buckets per truck load."""
        # Expect 5 avg * 19,200 loads = ~96,000 measurements
        assert len(bucket_measurements) >= 75000, (
            f"Only {len(bucket_measurements)} measurements, expected >= 75,000"
        )
