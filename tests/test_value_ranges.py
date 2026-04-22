"""
Value range tests for ShovelSense data.

Tests that numeric values fall within expected bounds.
"""

import pytest


class TestBlockModelValueRanges:
    def test_cu_grade_non_negative(self, block_model):
        negative = (block_model["planned_cu_grade"] < 0).sum()
        assert negative == 0, f"Found {negative} negative Cu grade values"

    def test_cu_grade_max(self, block_model):
        # Max expected is 2.1% Cu
        exceeds = (block_model["planned_cu_grade"] > 2.1).sum()
        assert exceeds == 0, f"Found {exceeds} Cu grade values > 2.1%"

    def test_fe_grade_non_negative(self, block_model):
        negative = (block_model["planned_fe_grade"] < 0).sum()
        assert negative == 0, f"Found {negative} negative Fe grade values"

    def test_chalcopyrite_pct_range(self, block_model):
        out_of_range = (
            (block_model["chalcopyrite_pct"] < 0) | (block_model["chalcopyrite_pct"] > 100)
        ).sum()
        assert out_of_range == 0, f"Found {out_of_range} chalcopyrite_pct values outside [0, 100]"

    def test_bornite_pct_range(self, block_model):
        out_of_range = (
            (block_model["bornite_pct"] < 0) | (block_model["bornite_pct"] > 100)
        ).sum()
        assert out_of_range == 0, f"Found {out_of_range} bornite_pct values outside [0, 100]"

    def test_surface_volume_correlation_range(self, block_model):
        out_of_range = (
            (block_model["surface_volume_correlation"] < 0)
            | (block_model["surface_volume_correlation"] > 1)
        ).sum()
        assert out_of_range == 0, f"Found {out_of_range} surface_volume_correlation values outside [0, 1]"


class TestBucketMeasurementsValueRanges:
    def test_cu_grade_non_negative(self, bucket_measurements):
        negative = (bucket_measurements["cu_grade_pct"] < 0).sum()
        assert negative == 0, f"Found {negative} negative Cu grade values"

    def test_xrf_confidence_min(self, bucket_measurements):
        below_min = (bucket_measurements["xrf_confidence"] < 0.70).sum()
        assert below_min == 0, f"Found {below_min} XRF confidence values < 0.70"

    def test_xrf_confidence_max(self, bucket_measurements):
        above_max = (bucket_measurements["xrf_confidence"] > 0.99).sum()
        assert above_max == 0, f"Found {above_max} XRF confidence values > 0.99"

    def test_laser_fill_level_range(self, bucket_measurements):
        out_of_range = (
            (bucket_measurements["laser_fill_level_pct"] < 0)
            | (bucket_measurements["laser_fill_level_pct"] > 100)
        ).sum()
        assert out_of_range == 0, f"Found {out_of_range} laser_fill_level_pct values outside [0, 100]"


class TestTruckLoadsValueRanges:
    def test_avg_cu_grade_non_negative(self, truck_loads):
        negative = (truck_loads["avg_cu_grade_pct"] < 0).sum()
        assert negative == 0, f"Found {negative} negative avg Cu grade values"

    def test_payload_non_negative(self, truck_loads):
        negative = (truck_loads["payload_tonnes"] < 0).sum()
        assert negative == 0, f"Found {negative} negative payload values"

    def test_payload_max(self, truck_loads):
        # Max realistic payload ~400 tonnes, allow some buffer to 500
        exceeds = (truck_loads["payload_tonnes"] > 500).sum()
        assert exceeds == 0, f"Found {exceeds} payload values > 500 tonnes"

    def test_avg_xrf_confidence_min(self, truck_loads):
        below_min = (truck_loads["avg_xrf_confidence"] < 0.70).sum()
        assert below_min == 0, f"Found {below_min} avg XRF confidence values < 0.70"

    def test_avg_xrf_confidence_max(self, truck_loads):
        above_max = (truck_loads["avg_xrf_confidence"] > 0.99).sum()
        assert above_max == 0, f"Found {above_max} avg XRF confidence values > 0.99"


class TestShiftSummariesValueRanges:
    def test_diversion_rate_range(self, shift_summaries):
        out_of_range = (
            (shift_summaries["diversion_rate"] < 0) | (shift_summaries["diversion_rate"] > 1)
        ).sum()
        assert out_of_range == 0, f"Found {out_of_range} diversion_rate values outside [0, 1]"

    def test_n_trucks_positive(self, shift_summaries):
        non_positive = (shift_summaries["n_trucks"] <= 0).sum()
        assert non_positive == 0, f"Found {non_positive} shifts with n_trucks <= 0"
