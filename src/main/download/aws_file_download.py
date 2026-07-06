import os
import traceback
from src.main.utility.logging_config import logger

class S3FileDownloader:
    """Utility class to download objects from an AWS S3 bucket to a local directory path."""
    
    def __init__(self, s3_client, bucket_name, local_directory):
        """Initializes the S3 downloader with an active client connection, bucket name, and local download destination."""
        self.bucket_name = bucket_name
        self.local_directory = local_directory
        self.s3_client = s3_client

    def download_files(self, list_files):
        """Downloads files matching the specified list of keys to the local destination directory."""
        logger.info("Starting download process for keys: %s", list_files)
        for key in list_files:
            file_name = os.path.basename(key)
            download_file_path = os.path.join(self.local_directory, file_name)
            logger.info("Downloading object %s to local path: %s", key, download_file_path)
            try:
                self.s3_client.download_file(self.bucket_name, key, download_file_path)
            except Exception as e:
                logger.error("Failed to download key '%s' from bucket '%s'. Error: %s", key, self.bucket_name, e)
                logger.error("Traceback: %s", traceback.format_exc())
                raise e


