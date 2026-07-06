import traceback
from src.main.utility.logging_config import logger

class ParquetWriter:
    """Utility class to write Spark DataFrames in specified file formats (e.g. Parquet) locally or on distributed filesystems."""
    
    def __init__(self, mode, data_format):
        """Initializes the writer with write mode (e.g. overwrite, append) and data format."""
        self.mode = mode
        self.data_format = data_format

    def dataframe_writer(self, df, file_path):
        """Writes the Spark DataFrame to the target file path in the configured format."""
        logger.info("Writing DataFrame to path '%s' in format '%s' (mode: %s)...", file_path, self.data_format, self.mode)
        try:
            df.write.format(self.data_format) \
                .option("header", "true") \
                .mode(self.mode) \
                .option("path", file_path) \
                .save()
            logger.info("DataFrame successfully written to '%s'.", file_path)
        except Exception as e:
            logger.error("Error occurred while writing DataFrame to path '%s': %s", file_path, e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise e