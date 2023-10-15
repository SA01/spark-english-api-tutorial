from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark_ai import SparkAI

from common import create_spark_session

spark = create_spark_session("PySpark English API")
spark_ai = SparkAI()
spark_ai.activate()

def explanation_of_data(spark: SparkSession):
    tips_data = read_trips_data(spark=spark)
    print(tips_data.ai.explain())
    tips_data.ai.plot(
        """the number of trips in each day as a bar chart with average fare (averge of total fare given by the 
        total_amount column) as a line on the second vertical axis. The X-axis should contain dates."""
    )


def popular_pickup_locations(spark: SparkSession):
    tips_data = read_trips_data(spark=spark)
    tips_data.ai.transform("Find the 10 most popular pickup locations, alongwith average number of trips per day and average fare").show(truncate = False)
    tips_data.ai.transform("Find the 10 most popular pickup locations on weekends, alongwith average number of trips per day and average fare").show(truncate = False)


def read_trips_data(spark: SparkSession):
    lookup_data = spark.read.option("header", "true").csv("data/ny_taxi_trips/taxi+_zone_lookup.csv")
    trips_data = (
        spark
        .read
        .parquet("data/ny_taxi_trips/*.parquet")
        .join(
            lookup_data
            .withColumnRenamed("Borough", "PU_Borough")
            .withColumnRenamed("Zone", "PU_Zone")
            .withColumnRenamed("service_zone", "PU_service_zone"),
            F.col("PULocationID") == F.col("LocationID"),
            "inner"
        )
        .drop("LocationID")
        .join(
            lookup_data
            .withColumnRenamed("Borough", "DO_Borough")
            .withColumnRenamed("Zone", "DO_Zone")
            .withColumnRenamed("service_zone", "DO_service_zone"),
            F.col("DOLocationID") == F.col("LocationID"),
            "inner"
        )
        .drop("LocationID")
    )

    trips_data = trips_data.ai.transform("The data starts from 2022-01-01 and ends on 2022-04-30, remove any rows that are outside of this range.")
    trips_data.printSchema()

    return trips_data


def popular_pickup_locations_by_4_hourly_segments(spark: SparkSession):
    trips_data = read_trips_data(spark=spark)

    prompt = """Divide the each into 4 four hour long time segments with first segment starting from 12:00am. 
    Which are the top 5 most popular pickup locations (based on average number of trips) in every time segment? 
    The result dataframe should be ordered by segment of day (ascending), and then ordered by most popular 
    locations within each segment. It should contain 30 rows, 5 for each time segment.
    The result should contain columns containing the start and end time of each segment, the borough and 
    zone of the pickup location, and also include average of total_fare column"""
    result_data = trips_data.ai.transform(prompt)

    result_data.show(truncate=False, n=30)


@spark_ai.udf
def calculate_trip_duration_in_seconds(start_time: datetime, end_time: datetime) -> int:
    """Calculate the duration between start_time and end_time in seconds"""
    ...


@spark_ai.udf
def calculate_trip_duration_in_minutes(start_time: datetime, end_time: datetime) -> int:
    """Calculate the duration between start_time and end_time in minutes, and also count starting minute and ending
    minute of the timestamps as full minutes for any value of seconds in the start and end times.
    The code should check both minutes and seconds in the timestamps to implement this.
    For example, a trip starting at 01:13:05 and ending at 01:16:02 is 4 minutes long, it should be considered as
    started at 01:13:00 and ended at 01:17:00.
    Additionally, as an exception to the rule, the duration
    of the trip should be 1 minute if the trip starts and end within the same minute e.g. 01:13:05 to 01:13:22."""
    ...


spark.udf.register("calculate_trip_duration_in_seconds", calculate_trip_duration_in_seconds)
spark.udf.register("calculate_trip_duration_in_minutes", calculate_trip_duration_in_minutes)


def calculate_trip_duration(spark: SparkSession):
    trips_data = (read_trips_data(spark=spark))
    trips_data.createOrReplaceTempView("trips_data")

    (
        spark.sql
        (
            """
            select 
                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, PU_Zone, DO_Zone, 
                calculate_trip_duration_in_seconds(tpep_pickup_datetime, tpep_dropoff_datetime) as duration_seconds,
                calculate_trip_duration_in_minutes(tpep_pickup_datetime, tpep_dropoff_datetime) as duration_minutes
            from trips_data limit 20
            """
        )
        .show(truncate=False)
    )


if __name__ == '__main__':
    # explanation_of_data(spark=spark)
    # popular_pickup_locations(spark_ai=spark_ai)
    # popular_pickup_locations_by_4_hourly_segments(spark = spark)
    calculate_trip_duration(spark=spark)

