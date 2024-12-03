"""
CSV ingestion in a dataframe and manipulation.

@author rambabu.posa
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_absolute_file_path(path, filename):
    # To get absolute path for a given filename
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path


def main():
    # The processing code.
    filename = "Restaurants_in_Wake_County_NC.csv"
    path = "../../../../data/"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Reads a CSV file with header, called
    # Restaurants_in_Wake_County_NC.csv,
    # stores it in a dataframe
    df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)

    logging.warning("*** Right after ingestion")
    df.show(5)
    df.printSchema()

    logging.warning("We have {} records.".format(df.count()))

    # Let's transform our dataframe
    df = (
        df.withColumn("county", F.lit("Wake"))
        .withColumnRenamed("HSISID", "datasetId")
        .withColumnRenamed("NAME", "name")
        .withColumnRenamed("ADDRESS1", "address1")
        .withColumnRenamed("ADDRESS2", "address2")
        .withColumnRenamed("CITY", "city")
        .withColumnRenamed("STATE", "state")
        .withColumnRenamed("POSTALCODE", "zip")
        .withColumnRenamed("PHONENUMBER", "tel")
        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
        .withColumnRenamed("FACILITYTYPE", "type")
        .withColumnRenamed("X", "geoX")
        .withColumnRenamed("Y", "geoY")
        .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")
    )

    df = df.withColumn(
        "id",
        F.concat(
            F.col("state"), F.lit("_"), F.col("county"), F.lit("_"), F.col("datasetId")
        ),
    )

    # Shows at most 5 rows from the dataframe
    logging.warning("*** Dataframe transformed")
    df.show(5)

    # for book only
    df_used_for_book = df.drop(
        "address2", "zip", "tel", "dateStart", "geoX", "geoY", "address1", "datasetId"
    )

    df_used_for_book.show(5, 15)
    # end

    df.printSchema()

    logging.warning("*** Looking at partitions")
    partition_count = df.rdd.getNumPartitions()
    logging.warning("Partition count before repartition: {}".format(partition_count))

    df = df.repartition(4)

    logging.warning(
        "Partition count after repartition: {}".format(df.rdd.getNumPartitions())
    )


if __name__ == "__main__":
    # Creates a session on a local master
    spark = (
        SparkSession.builder.appName("Restaurants in Wake County, NC")
        .master("local[*]")
        .getOrCreate()
    )

    # Comment this line to see full log
    spark.sparkContext.setLogLevel("warn")
    main()
    spark.stop()
