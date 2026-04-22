"""
Referential integrity tests for ShovelSense data.

Tests that all foreign key references resolve to valid primary keys.
"""

import pytest


class TestBucketMeasurementsReferences:
    def test_shovel_id_references_valid(self, bucket_measurements, shovels):
        valid_shovels = set(shovels["shovel_id"])
        actual_shovels = set(bucket_measurements["shovel_id"])
        orphaned = actual_shovels - valid_shovels
        assert not orphaned, f"Orphaned shovel_id values: {orphaned}"

    def test_truck_id_references_valid(self, bucket_measurements, trucks):
        valid_trucks = set(trucks["truck_id"])
        actual_trucks = set(bucket_measurements["truck_id"])
        orphaned = actual_trucks - valid_trucks
        assert not orphaned, f"Orphaned truck_id values: {orphaned}"

    def test_block_id_references_valid(self, bucket_measurements, block_model):
        valid_blocks = set(block_model["block_id"])
        actual_blocks = set(bucket_measurements["block_id"])
        orphaned = actual_blocks - valid_blocks
        assert not orphaned, f"Orphaned block_id values: {orphaned}"


class TestTruckLoadsReferences:
    def test_shovel_id_references_valid(self, truck_loads, shovels):
        valid_shovels = set(shovels["shovel_id"])
        actual_shovels = set(truck_loads["shovel_id"])
        orphaned = actual_shovels - valid_shovels
        assert not orphaned, f"Orphaned shovel_id values: {orphaned}"

    def test_truck_id_references_valid(self, truck_loads, trucks):
        valid_trucks = set(trucks["truck_id"])
        actual_trucks = set(truck_loads["truck_id"])
        orphaned = actual_trucks - valid_trucks
        assert not orphaned, f"Orphaned truck_id values: {orphaned}"

    def test_block_id_references_valid(self, truck_loads, block_model):
        valid_blocks = set(block_model["block_id"])
        actual_blocks = set(truck_loads["block_id"])
        orphaned = actual_blocks - valid_blocks
        assert not orphaned, f"Orphaned block_id values: {orphaned}"


class TestShiftSummariesReferences:
    def test_shovel_id_references_valid(self, shift_summaries, shovels):
        valid_shovels = set(shovels["shovel_id"])
        actual_shovels = set(shift_summaries["shovel_id"])
        orphaned = actual_shovels - valid_shovels
        assert not orphaned, f"Orphaned shovel_id values: {orphaned}"
