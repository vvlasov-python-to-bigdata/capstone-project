"""
This Python module contains a job which builds purchases attribution
projection using default Spark SQL capabilities.

The job flow:
  * Load data from CSV files
  * Calculate user sessions and set unique id for each session
  * Join sessions with base data and select necessary fields
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
from pyspark.sql import SparkSession, DataFrame, Window

from shared.logging import Log4jWrapper
from shared.spark import start_spark_session

def main() -> None:
    """
    The job definition.

    :return: None
    """
    cfg_file = sys.argv[1] if len(sys.argv) > 1 \
                           else "config/test/purchases_attribution_config.yaml"

    spark_session, spark_logger, config = start_spark_session(
        app_name = "purchases_attribution_sql",
        files=[cfg_file]
    )

    spark_logger.warning("job is up and running")

    clickstream_df, purchases_df = extract_data(spark_session, config)
    purchases_attribution_df = transform_data(clickstream_df, purchases_df, spark_logger)

    spark_logger.warning(f"storing results as Parquet files to {config['output']}")
    load_data(purchases_attribution_df, config)

    spark_logger.warning("job is finished")
    spark_session.stop()


def extract_data(ss: SparkSession, config: dict) -> tuple[DataFrame]:
    """
    Loads data from CSV files.

    :param ss: an instance of SparkSession.
    :param config: a dictionary with job parameters.
    :return: Spark DataFrame-s with input data.
    """
    clickstream_df = (
        ss.read
        .option('escape',"\"")
        .csv(path=config["input_clickstream"], header=True)
    )

    purchases_df = (
        ss.read
        .csv(path=config["input_purchases"], header=True)
    )

    return clickstream_df, purchases_df


def transform_data(clickstream_df: DataFrame, purchases_df: DataFrame,
                   logger: Log4jWrapper) -> DataFrame:
    """
    Builds purchases attribution dataset from mobile app clickstream
    and user purchases data.

    :param df: dataset to transform as DataFrame.
    :return: transformed DataFrame.
    """
    user_window: Window = Window.partitionBy("userId").orderBy("eventTime")

    logger.warning("build leading events dataset ...")
    leading_events_df = (
        clickstream_df
        .filter(F.col("eventType").isin({"app_open", "app_close"}))
        .withColumn("nextEventId", F.lead(F.col("eventId"), 1).over(user_window))
        .select(
            F.col("userId"),
            F.col("eventTime").alias("startSessionEventTime"),
            F.col("eventType").alias("startSessionEventType"),
            F.col("attributes"),
            F.col("nextEventId")
        )
        .filter(F.col("startSessionEventType") == "app_open")
        .withColumn("campaignId", F.get_json_object(F.col("attributes"), "$.campaign_id"))
        .withColumn("channelId", F.get_json_object(F.col("attributes"), "$.channel_id"))
        .drop("attributes")
    )

    leading_events_df.show()

    logger.warning("calculate user sessions ...")
    sessions_df = (
        leading_events_df.alias("leading_events")
        .join(
            clickstream_df.alias("all_events"),
            on=F.col("leading_events.nextEventId") == F.col("all_events.eventId"),
            how="left"
        )
        .select(
            F.col("leading_events.userId"),
            F.col("leading_events.startSessionEventTime"),
            F.col("leading_events.campaignId"),
            F.col("leading_events.channelId"),
            F.col("all_events.eventTime").alias("endSessionEventTime"),
        )
        .withColumn("sessionId", F.monotonically_increasing_id())
    )

    sessions_df.show()

    logger.warning("append session to each event ...")
    sessioned_purchases_df = (
        clickstream_df.alias("c")
        .filter(F.col("c.eventType") == "purchase")
        .join(
            sessions_df.alias("s"),
            on=[
                F.col("c.userId") == F.col("s.userId"),
                F.col("c.eventTime") >= F.col("s.startSessionEventTime"),
                (F.col("c.eventTime") <= F.col("s.endSessionEventTime"))
                    | F.col("s.endSessionEventTime").isNull(),
            ]
        )
        .select(
            F.col("s.userId"),
            F.col("s.sessionId"),
            F.col("s.campaignId"),
            F.col("s.channelId"),
            F.get_json_object(F.col("c.attributes"), "$.purchase_id").alias("purchaseId"),
        )
        .orderBy(F.col("userId"), F.col("eventTime"))
    )

    sessioned_purchases_df.show()

    logger.warning("build purchases attribution ...")
    projection_df = (
        sessioned_purchases_df.alias("s")
        .join(purchases_df.alias("p"), on=F.col("p.purchaseId") == F.col("s.purchaseId"))
        .select(
            F.col("p.purchaseId"),
            F.col("p.purchaseTime"),
            F.col("p.billingCost"),
            F.col("p.isConfirmed"),
            F.col("s.sessionId"),
            F.col("s.campaignId"),
            F.col("s.channelId")
        )
    )

    projection_df.show()

    return projection_df


def load_data(df: DataFrame, config: dict) -> None:
    """
    Collects data and writes results to Parquet files.

    :param df: DataFrame to process.
    :param config: a dictionary with job parameters.
    :return: None
    """
    df.write.parquet(config["output"], mode="overwrite")


if __name__ == "__main__":
    main()
