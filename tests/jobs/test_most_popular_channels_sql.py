"""
This module contains tests for `src/jobs/task2/most_popular_channels_sql.py`.
"""
import unittest

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.jobs.task2 import most_popular_channels_sql
from src.shared.logging import Log4jWrapper
from src.shared.spark import start_spark_session


class MostPopularChannelsSQLTests(unittest.TestCase):
    """
    Tests for Spark job described in
    `src/jobs/task2/most_popular_channels_sql.py`.
    """

    def setUp(self):
        spark_session, spark_logger, config = start_spark_session(
            app_name="most_popular_channels_sql_test",
            files=["config/test/most_popular_channels_config.yaml"]
        )

        self.spark_session: SparkSession = spark_session
        self.spark_logger: Log4jWrapper = spark_logger
        self.config: dict = config

    def tearDown(self):
        if self.spark_session:
            self.spark_session.stop()

    def test_extract_data(self):
        """
        Test: Function `most_popular_channels_sql#extract_data()` correctly loads test data.
        """
        expected_columns = ["purchaseId", "purchaseTime", "billingCost", "isConfirmed",
                            "sessionId", "campaignId", "channelId"]

        purchases_attribution_df: DataFrame = most_popular_channels_sql.extract_data(
            self.spark_session, self.config)

        self.assertIsNotNone(purchases_attribution_df)

        self.assertEqual(7, len(purchases_attribution_df.columns))

        # pylint: disable=expression-not-assigned
        [self.assertIn(col, expected_columns) for col in purchases_attribution_df.columns]


    def test_transform_data(self):
        """
        Test: Function `most_popular_channels_sql#transform_data()` works correctly on dummy data.
        """
        # Input data
        dummy_data_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/most_popular_channels.csv",
            header=True
        )

        # Expected results
        expected_columns = ["campaignId", "channelId", "sessionsCount", "channelRank"]

        # Actual results
        most_popular_channels_df = most_popular_channels_sql.transform_data(
            dummy_data_df, self.spark_logger)

        total_rows = most_popular_channels_df.count()

        # Asserts
        # pylint: disable=expression-not-assigned
        [self.assertIn(col, expected_columns) for col in most_popular_channels_df.columns]
        self.assertEqual(3, total_rows)

        # Check grouping by channels and campaigns
        num_groups_for_first_campaign = (
            most_popular_channels_df
            .filter(F.col("campaignId") == "1")
            .count()
        )
        self.assertEqual(2, num_groups_for_first_campaign)

        # Check ranking
        top_channel_for_first_campaign = (
            most_popular_channels_df
            .select("channelId")
            .filter(F.col("campaignId") == "1")
            .filter(F.col("channelRank") == "1")
        ).collect()[0][0]
        self.assertEqual("Yandex Ads", top_channel_for_first_campaign)

    def test_load_data(self):
        """
        Test: Function `most_popular_channels_sql#load_data()` saves data without errors.
        """
        # Input data
        dummy_data_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/most_popular_channels.csv",
            header=True
        )

        most_popular_channels_sql.load_data(dummy_data_df, self.config)

    # pylint: disable=no-self-use
    def test_main(self):
        """
        Test: Job `most_popular_channels_sql` is completing without errors on test data.
        """
        most_popular_channels_sql.main()


if __name__ == "__main__":
    unittest.main()
