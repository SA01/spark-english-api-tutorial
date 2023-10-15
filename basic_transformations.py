from pyspark_ai import SparkAI

from common import create_spark_session
from time import sleep

if __name__ == '__main__':
    spark = create_spark_session("PySpark English API")

    spark_ai = SparkAI()
    spark_ai.activate()

    df = spark_ai._spark.createDataFrame(
        [
            ("Normal", "Cellphone", 6000),
            ("Normal", "Tablet", 1500),
            ("Mini", "Tablet", 5500),
            ("Mini", "Cellphone", 5000),
            ("Foldable", "Cellphone", 6500),
            ("Foldable", "Tablet", 2500),
            ("Pro", "Cellphone", 3000),
            ("Pro", "Tablet", 4000),
            ("Pro Max", "Cellphone", 4500)
        ],
        ["product", "category", "revenue"]
    )

    df.show(truncate=False)

    # df.ai.transform("What are the best-selling and the second best-selling products in every category?").show()
    # df.ai.transform("What is the cumulative revenue by product, and what percentage of the total revenue is the cumulative revenue by product?").show()
    df.ai.transform("What is the total revenue by each product, and also, what percentage of the total revenue is the total revenue for each product?").show()

    # sleep(10000)