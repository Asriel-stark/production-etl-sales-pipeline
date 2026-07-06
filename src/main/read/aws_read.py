import boto3
import traceback
from src.main.utility.logging_config import logger

class S3Reader:
    """Helper client class to interface with S3 storage, facilitating bucket exploration and list queries."""

    def list_files(self, s3_client, bucket_name, folder_path):
        """Lists file paths under specified bucket name and prefix path, ignoring folder objects."""
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
            if 'Contents' in response:
                logger.info("Total files metadata received from S3 bucket '%s/%s': %s", bucket_name, folder_path, response)
                files = [
                    f"s3://{bucket_name}/{obj['Key']}" 
                    for obj in response['Contents'] 
                    if not obj['Key'].endswith('/')
                ]
                return files
            else:
                return []
        except Exception as e:
            logger.error("Failed while listing S3 files in bucket %s at prefix %s. Error: %s", bucket_name, folder_path, e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise e

