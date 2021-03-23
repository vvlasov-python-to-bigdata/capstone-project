"""
This module contains tests for `src/jobs/task1/purchases_attribution_sql.py`.
"""
import unittest

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.jobs.task1 import purchases_attribution_sql
from src.shared.logging import Log4jWrapper
from src.shared.spark import start_spark_session


class PurchasesAttributionSQLTests(unittest.TestCase):
    """
    Tests for Spark job described in
    `src/jobs/task1/purchases_attribution_sql.py`.
    """

    def setUp(self):
        spark_session, spark_logger, config = start_spark_session(
            app_name="purchases_attribution_sql_test",
            files=["config/test/purchases_attribution_config.yaml"]
        )

        self.spark_session: SparkSession = spark_session
        self.spark_logger: Log4jWrapper = spark_logger
        self.config: dict = config

    def tearDown(self):
        if self.spark_session:
            self.spark_session.stop()

    def test_extract_data(self):
        """
        Test: Function `purchases_attribution_sql#extract_data()` correctly loads test data.
        """
        clickstream_expected_columns = ["userId", "eventId", "eventTime", "eventType", "attributes"]
        user_purchases_expected_columns = ["purchaseId", "purchaseTime", "billingCost",
                                           "isConfirmed"]

        clickstream_df, purchases_df = purchases_attribution_sql.extract_data(
            self.spark_session, self.config)

        self.assertIsNotNone(clickstream_df)
        self.assertIsNotNone(purchases_df)

        self.assertEqual(len(clickstream_expected_columns), len(clickstream_df.columns))
        self.assertEqual(len(user_purchases_expected_columns), len(purchases_df.columns))

        # pylint: disable=expression-not-assigned
        [self.assertIn(col, clickstream_expected_columns) for col in clickstream_df.columns]
        [self.assertIn(col, user_purchases_expected_columns) for col in purchases_df.columns]


    def test_transform_data(self):
        """
        Test: Function `purchases_attribution_sql#transform_data()` works correctly on dummy data.
        """
        # Input data
        dummy_user_purchases_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/user_purchases.csv",
            header=True
        )

        dummy_clickstream_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/mobile_app_clickstream.csv",
            header=True
        )

        # Expected results
        expected_columns = ["purchaseId", "purchaseTime", "billingCost", "isConfirmed",
                            "sessionId", "campaignId", "channelId"]

        # Actual results
        purchases_attribution_df = purchases_attribution_sql.transform_data(
            dummy_clickstream_df, dummy_user_purchases_df, self.spark_logger)

        total_rows = purchases_attribution_df.count()

        # Asserts
        # pylint: disable=expression-not-assigned
        [self.assertIn(col, expected_columns) for col in purchases_attribution_df.columns]
        self.assertEqual(3, total_rows)

        # Check sessioning works: two purchases grouped in one session
        purchases_for_second_user = (
            purchases_attribution_df
            .groupBy(F.col("sessionId"))
            .agg(F.count(F.col("sessionId")).alias("sessionsCount"))
            .select(F.col("sessionsCount"))
            .orderBy(F.col("sessionsCount"))
        ).collect()
        self.assertEqual(2, len(purchases_for_second_user))
        self.assertEqual(2, purchases_for_second_user[1]["sessionsCount"])

    def test_load_data(self):
        """
        Test: Function `purchases_attribution_sql#load_data()` saves data without errors.
        """
        # Input data
        dummy_data_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/most_popular_channels.csv",
            header=True
        )

        purchases_attribution_sql.load_data(dummy_data_df, self.config)

    # pylint: disable=no-self-use
    def test_main(self):
        """
        Test: Job `purchases_attribution_sql` is completing without errors on test data.
        """
        purchases_attribution_sql.main()


if __name__ == "__main__":
    unittest.main()
