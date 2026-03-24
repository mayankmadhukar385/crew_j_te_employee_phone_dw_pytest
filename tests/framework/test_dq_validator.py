"""Tests for DQValidator — config-driven data quality checks.

Verifies that DQ rules correctly split DataFrames into passed/failed records.
"""

import pytest
from pyspark.sql import functions as F

from framework.common_utility import DQValidator


class TestNotNullRule:
    """Tests for the not_null DQ rule."""

    def test_catches_null_values(self, spark):
        config = {"dq_checks": [{"column": "id", "rules": ["not_null"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(1,), (None,)], ["id"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 1
        assert failed.count() == 1

    def test_catches_empty_strings(self, spark):
        config = {"dq_checks": [{"column": "name", "rules": ["not_null"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([("Alice",), ("",), ("  ",)], ["name"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 1
        assert failed.count() == 2

    def test_all_valid_passes_all(self, spark):
        config = {"dq_checks": [{"column": "id", "rules": ["not_null"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 3
        assert failed.count() == 0


class TestPositiveRule:
    """Tests for the positive DQ rule."""

    def test_catches_negatives_and_zero(self, spark):
        config = {"dq_checks": [{"column": "amount", "rules": ["positive"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(10,), (-5,), (0,)], ["amount"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 1
        assert failed.count() == 2


class TestMaxValueRule:
    """Tests for the max_value DQ rule."""

    def test_catches_values_above_max(self, spark):
        config = {"dq_checks": [{"column": "salary", "rules": ["max_value(100000)"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(50000,), (100001,), (100000,)], ["salary"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 2
        assert failed.count() == 1


class TestMinValueRule:
    """Tests for the min_value DQ rule."""

    def test_catches_values_below_min(self, spark):
        config = {"dq_checks": [{"column": "age", "rules": ["min_value(18)"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(25,), (17,), (18,)], ["age"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 2
        assert failed.count() == 1


class TestRegexRule:
    """Tests for the matches_regex DQ rule."""

    def test_catches_non_matching(self, spark):
        config = {"dq_checks": [{"column": "email", "rules": ['matches_regex("^[^@]+@[^@]+$")']}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([("a@b.com",), ("invalid",), ("x@y",)], ["email"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 2
        assert failed.count() == 1


class TestMultipleRules:
    """Tests for combining multiple DQ rules."""

    def test_multiple_rules_on_same_column(self, spark):
        config = {"dq_checks": [{"column": "val", "rules": ["not_null", "positive"]}]}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(10,), (None,), (-5,), (0,)], ["val"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 1

    def test_rules_across_columns(self, spark):
        config = {
            "dq_checks": [
                {"column": "id", "rules": ["not_null"]},
                {"column": "amount", "rules": ["positive"]},
            ]
        }
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(1, 10), (None, 5), (2, -1)], ["id", "amount"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 1


class TestNoChecks:
    """Tests for empty DQ config."""

    def test_empty_checks_passes_all(self, spark):
        config = {"dq_checks": []}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(1,), (None,)], ["id"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 2
        assert failed.count() == 0

    def test_missing_checks_key_passes_all(self, spark):
        config = {}
        dq = DQValidator(spark, config)
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        passed, failed = dq.validate(df, stage="test")
        assert passed.count() == 2
