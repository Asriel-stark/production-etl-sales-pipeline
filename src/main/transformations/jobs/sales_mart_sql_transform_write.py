from pyspark.sql.functions import col, date_format, sum, concat, lit, rank, when, round
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter
from src.main.utility.logging_config import logger

def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    """Calculates monthly sales volumes per salesperson, grants 1% incentive for top performers in each store, and persists data to MySQL."""
    logger.info("Initializing monthly sales calculations for sales team...")
    
    # Window partition to aggregate monthly sales per salesperson
    window = Window.partitionBy(
        "store_id",
        "sales_person_id",
        "sales_month"
    )

    final_sales_team_data_mart = (
        final_sales_team_data_mart_df
        .withColumn(
            "sales_month",
            date_format(col("sales_date"), "yyyy-MM")
        )
        .withColumn(
            "total_sales_every_month",
            sum(col("total_cost")).over(window)
        )
        .select(
            "store_id",
            "sales_person_id",
            concat(
                col("sales_person_first_name"),
                lit(" "),
                col("sales_person_last_name")
            ).alias("full_name"),
            "sales_month",
            "total_sales_every_month"
        )
        .distinct()
    )

    # Window partition to rank salespeople within each store based on monthly sales
    rank_window = (
        Window.partitionBy(
            "store_id",
            "sales_month"
        ).orderBy(
            col("total_sales_every_month").desc()
        )
    )

    # Rank team members and calculate top performer incentives
    final_sales_team_data_mart_table = (
        final_sales_team_data_mart
        .withColumn(
            "rnk",
            rank().over(rank_window)
        )
        .withColumn(
            "incentive",
            when(
                col("rnk") == 1,
                col("total_sales_every_month") * 0.01
            ).otherwise(0)
        )
        .withColumn(
            "incentive",
            round(col("incentive"), 2)
        )
        .withColumn(
            "total_sales",
            col("total_sales_every_month")
        )
        .select(
            "store_id",
            "sales_person_id",
            "full_name",
            "sales_month",
            "total_sales",
            "incentive"
        )
    )

    logger.info("Writing sales team calculation results to MySQL table: %s", config.sales_team_data_mart_table)

    db_writer = DatabaseWriter(
        url=config.url,
        properties=config.properties
    )

    db_writer.write_dataframe(
        final_sales_team_data_mart_table,
        config.sales_team_data_mart_table
    )