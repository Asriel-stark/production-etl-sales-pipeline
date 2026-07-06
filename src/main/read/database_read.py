class DatabaseReader:
    """Utility class to read relational database tables into Spark DataFrames using JDBC."""
    
    def __init__(self, url, properties):
        """Initializes the database client with connection URL and properties."""
        self.url = url
        self.properties = properties

    def create_dataframe(self, spark, table_name):
        """Reads a table from database and returns a Spark DataFrame."""
        df = spark.read.jdbc(
            url=self.url,
            table=table_name,
            properties=self.properties
        )
        return df