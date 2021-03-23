"""
This Python module contains a job which calculates the most popular
advertisement channel for each marketing campaign. A channel popularity
is based on the amount of unique user sessions with the App.

The job is implemented using plain SQL over Spark DataFrame.

The job flow:
  * Read data from Parquet files (purchases projection from task #1)
  * Calculate amount of unique sessions per channel in each campaign
  * Use window function to rank channels in each campaign by the amount of
    unique sessions
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
import os
import sys

from pyspark.sql import SparkSession, DataFrame

from shared.logging import Log4jWrapper
from shared.spark import start_spark_session


def main() -> None:
    """
    The job definition.

    :return: None
    """
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else "config/test/most_popular_channels_config.yaml"

    spark_session, spark_logger, config = start_spark_session(
        app_name = "most_popular_channels_sql",
        files=[cfg_file]
    )

    spark_logger.warning("job is up and running")

    purchases_attribution_df = extract_data(spark_session, config)
    most_popular_channels_df = transform_data(purchases_attribution_df, spark_logger)

    spark_logger.warning(f"storing results as Parquet files to {config['output']}")
    load_data(most_popular_channels_df, config)

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


def transform_data(purchases_attribution_df: DataFrame, logger: Log4jWrapper) -> DataFrame:
    """
    Tranforms purchases attribution dataframe to calculate the most
    popular advertisement channel per each marketing campaign.

    :param purchases_attribution_df: dataset to transform as DataFrame.
    :param logger: Spark Log4j wrapper to write logs.
    :return: transformed DataFrame.
    """
    logger.warning("storing purchases attribution to temporary table 'purchases' ...")
    purchases_attribution_df.registerTempTable("purchases")

    logger.warning("calculating the most popular ads channel per each marketing campaign ...")
    sql_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 "sql/most_popular_channels.sql")
    with open(sql_file_path, "r", encoding="utf-8") as sql_file:
        sql_query = sql_file.read()
        most_popular_channels_df = purchases_attribution_df.sql_ctx.sql(sql_query)

    most_popular_channels_df.show()

    return most_popular_channels_df


def load_data(df: DataFrame, config: dict) -> None:
    """
    Saves df as Parquet files.

    :param df: DataFrame to process.
    :param config: dictionary with the job params.
    :return: None
    """
    df.write.parquet(config["output"], mode="overwrite")


if __name__ == "__main__":
    main()
