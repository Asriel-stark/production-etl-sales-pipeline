import os
import datetime
import traceback
from src.main.utility.logging_config import logger

class UploadToS3:
    """Utility class to upload local directory directories and files to an AWS S3 bucket."""
    
    def __init__(self, s3_client):
        """Initializes the S3 uploader client connection wrapper."""
        self.s3_client = s3_client

    def upload_to_s3(self, s3_directory, s3_bucket, local_file_path):
        """Uploads files under local_file_path recursively to S3 prefix, preserving directory structures."""
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        s3_prefix = f"{s3_directory}/{current_epoch}"
        logger.info("Uploading local directory %s to s3://%s/%s", local_file_path, s3_bucket, s3_prefix)
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    # Ignore spark metadata files
                    if file.startswith(".") or file.startswith("_"):
                        continue
                    
                    full_local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_local_path, local_file_path)
                    s3_key = f"{s3_prefix}/{relative_path.replace(os.sep, '/')}"
                    
                    logger.info("Uploading file %s to S3 key: %s", full_local_path, s3_key)
                    self.s3_client.upload_file(full_local_path, s3_bucket, s3_key)
                    
            return f"Data Successfully uploaded in {s3_directory} data mart"
        except Exception as e:
            logger.error("Error occurred while uploading files under '%s' to S3: %s", local_file_path, e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise e


