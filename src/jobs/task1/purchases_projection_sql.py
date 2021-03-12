"""
Purchases Attribution Projection with Spark SQL.

This Python module contains a job which builds purchases attribution
projection using default Spark SQL capabilities.

The job flow:
  * ...

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
from pyspark.sql import SparkSession, DataFrame


def main() -> None:
    """
    The job definition.

    :return: None
    """


def extract_data(ss: SparkSession) -> DataFrame:
    """
    Loads data from Parquet files.

    :param ss: an instance of SparkSession.
    :return: Spark DataFrame with input data.
    """


def transform_data(df: DataFrame) -> DataFrame:
    """
    Tranforms original dataset.

    :param df: dataset to transform as DataFrame.
    :return: transformed DataFrame.
    """


def load_data(df: DataFrame) -> None:
    """
    Collects data and writes results to Parquet files.

    :param df: DataFrame to process.
    :return: None
    """


if __name__ == "__main__":
    main()
