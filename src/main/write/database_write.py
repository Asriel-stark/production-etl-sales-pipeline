import traceback
from src.main.utility.logging_config import logger

class DatabaseWriter:
    """Utility class to write Spark DataFrames to relational database tables using JDBC."""
    
    def __init__(self, url, properties):
        """Initializes the database writer with JDBC URL and write properties."""
        self.url = url
        self.properties = properties

    def write_dataframe(self, df, table_name):
        """Writes the Spark DataFrame into the specified database table using append mode."""
        logger.info("Writing DataFrame into database table '%s'...", table_name)
        try:
            df.write.jdbc(
                url=self.url,
                table=table_name,
                mode="append",
                properties=self.properties
            )
            logger.info("Data successfully written into '%s' table.", table_name)
        except Exception as e:
            logger.error("Failed to write DataFrame to database table '%s'. Error: %s", table_name, e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise e

