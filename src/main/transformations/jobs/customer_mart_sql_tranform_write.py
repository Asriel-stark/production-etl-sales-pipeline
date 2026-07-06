from pyspark.sql.functions import col, date_format, sum, concat, lit
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

def customer_mart_calculation_table_write(final_customer_data_mart_df):
    """Calculates each customer's total purchase amount per month and persists the result to MySQL customers_data_mart table."""
    # Partition window by customer ID and month
    window = Window.partitionBy("customer_id", "sales_date_month")
    
    # Calculate sum of purchases over the monthly window.
    # We append '-01' to the YYYY-MM substring and cast to DateType to guarantee compatibility with MySQL's DATE column.
    final_customer_data_mart = final_customer_data_mart_df.withColumn(
        "sales_date_month",
        concat(date_format(col("sales_date"), "yyyy-MM"), lit("-01")).cast(DateType())
    ).withColumn(
        "total_sales_every_month_by_each_customer",
        sum("total_cost").over(window)
    ).select(
        "customer_id",
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
        "address",
        "phone_number",
        "sales_date_month",
        col("total_sales_every_month_by_each_customer").alias("total_sales")
    ).distinct()

    # Write the calculated customer data mart to MySQL
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_customer_data_mart, config.customer_data_mart_table)


