import findspark
findspark.init()
from pyspark.sql import SparkSession
from src.main.utility.logging_config import logger

def spark_session():
    """Initializes and returns a PySpark session configured for local execution with MySQL JDBC driver support."""
    spark = SparkSession.builder.master("local[*]") \
        .appName("manish_spark2")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-j-9.7.0.jar") \
        .getOrCreate()
    logger.info("Spark session initialized: %s", spark)
    return spark

