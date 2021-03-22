"""
This module contains tests for `src/jobs/task2/top_marketing_campaigns_sql.py`.
"""
import unittest

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.jobs.task2 import top_marketing_campaigns_sql
from src.shared.logging import Log4jWrapper
from src.shared.spark import start_spark_session


class TopMarketingCampaignsSQLTests(unittest.TestCase):
    """
    Tests for Spark job described in
    `src/jobs/task2/top_marketing_campaigns_sql.py`.
    """

    def setUp(self):
        spark_session, spark_logger, config = start_spark_session(
            app_name="top_marketing_campaigns_sql_test",
            files=["config/test/top_marketing_campaigns_config.yaml"]
        )

        self.spark_session: SparkSession = spark_session
        self.spark_logger: Log4jWrapper = spark_logger
        self.config: dict = config

    def tearDown(self):
        if self.spark_session:
            self.spark_session.stop()

    def test_extract_data(self):
        """
        Test: Function `top_marketing_campaigns_sql#extract_data()` correctly loads test data.
        """
        expected_columns = ["purchaseId", "purchaseTime", "billingCost", "isConfirmed",
                            "sessionId", "campaignId", "channelId"]

        purchases_attribution_df: DataFrame = top_marketing_campaigns_sql.extract_data(
            self.spark_session, self.config)

        self.assertIsNotNone(purchases_attribution_df)

        self.assertEqual(7, len(purchases_attribution_df.columns))

        # pylint: disable=expression-not-assigned
        [self.assertIn(col, expected_columns) for col in purchases_attribution_df.columns]


    def test_transform_data(self):
        """
        Test: Function `top_marketing_campaigns_sql#transform_data()` works correctly on dummy data.
        """
        # Input data
        dummy_data_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/top_marketing_campaigns.csv",
            header=True
        )

        # Expected results
        expected_columns = ["campaignId", "revenue"]

        # Actual results
        top_marketing_campaigns_df = top_marketing_campaigns_sql.transform_data(
            dummy_data_df, self.spark_logger)

        total_rows = top_marketing_campaigns_df.count()

        # Asserts
        # pylint: disable=expression-not-assigned
        [self.assertIn(col, expected_columns) for col in top_marketing_campaigns_df.columns]
        self.assertEqual(2, total_rows)

        # Check revenue calculation
        first_campaign_revenue = (
            top_marketing_campaigns_df
            .select(F.col("revenue"))
            .filter(F.col("campaignId") == "1")
        ).collect()
        self.assertEqual(30.0, first_campaign_revenue[0][0])

    def test_load_data(self):
        """
        Test: Function `top_marketing_campaigns_sql#load_data()` saves data without errors.
        """
        # Input data
        dummy_data_df: DataFrame = self.spark_session.read.csv(
            path="tests/_data/dummy/top_marketing_campaigns.csv",
            header=True
        )

        top_marketing_campaigns_sql.load_data(dummy_data_df, self.config)

    # pylint: disable=no-self-use
    def test_main(self):
        """
        Test: Job `top_marketing_campaigns_sql` is completing without errors on test data.
        """
        top_marketing_campaigns_sql.main()


if __name__ == "__main__":
    unittest.main()
