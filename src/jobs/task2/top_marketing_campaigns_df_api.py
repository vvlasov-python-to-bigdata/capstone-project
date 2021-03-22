"""
This Python module contains a job which calculates the top 10
marketing campaigns based on revenue (sum of costs of confirmed
purchases).

The job is implemented using plain Spark DataFrame API.

The job flow:
  * Read data from Parquet files (purchases projection from task #1)
  * Filter only confirmed purchases (by `isConfirmed==True`)
  * Calculate revenue for each campaign
  * Output only top 10 marketing campaigns
  * Save results as Parquet files

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""
import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

from shared.logging import Log4jWrapper
from shared.spark import start_spark_session


def main() -> None:
    """
    The job definition.

    :return: None
    """
    cfg_file = sys.argv[1] if len(sys.argv) > 1 \
                           else "config/test/top_marketing_campaigns_config.yaml"

    spark_session, spark_logger, config = start_spark_session(
        app_name = "top_marketing_campaigns_df_api",
        files=[cfg_file]
    )

    spark_logger.warning("job is up and running")

    purchases_attribution_df = extract_data(spark_session, config)
    top_marketing_campaigns_df = transform_data(purchases_attribution_df, spark_logger)

    spark_logger.warning(f"storing results as Parquet files to {config['output']}")
    load_data(top_marketing_campaigns_df, config)

    spark_logger.warning("job is finished")
    spark_session.stop()


def extract_data(ss: SparkSession, config: dict) -> DataFrame:
    """
    Loads data from Parquet files.

    :param ss: an instance of SparkSession.
    :param config: dictionary with the job params.
    :return: Spark DataFrame with input data.
    """
    purchases_attribution_df = ss.read.parquet(config["input"])
    return purchases_attribution_df


def transform_data(projection_df: DataFrame, logger: Log4jWrapper) -> DataFrame:
    """
    Tranforms original dataset.

    :param projection_df: dataset to transform as DataFrame.
    :param logger: Spark Log4j wrapper to write logs.
    :return: transformed DataFrame.
    """
    # pylint: disable=singleton-comparison
    top_marketing_campaigns_df = (
        projection_df
        .filter(F.col("isConfirmed") == True)
        .groupBy(F.col("campaignId"))
        .agg(F.sum(F.col("billingCost")).alias("revenue"))
        .orderBy(F.col("revenue").desc())
        .limit(10)
    )

    top_marketing_campaigns_df.show()

    return top_marketing_campaigns_df


def load_data(df: DataFrame, config: dict) -> None:
    """
    Collects data and writes results to Parquet files.

    :param df: DataFrame to process.
    :param config: dictionary with the job params.
    :return: None
    """
    df.write.parquet(config["output"], mode="overwrite")


if __name__ == "__main__":
    main()
