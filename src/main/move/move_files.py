import traceback
from src.main.utility.logging_config import logger

def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix, file_name=None):
    """Moves objects in an S3 bucket from a source prefix to a destination prefix.
    If file_name is specified, only objects whose key ends with that filename will be moved.
    """
    logger.info("Executing move_s3_to_s3: source=%s, dest=%s, file_filter=%s", source_prefix, destination_prefix, file_name)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        if file_name is None:
            for obj in response.get('Contents', []):
                source_key = obj['Key']
                destination_key = destination_prefix + source_key[len(source_prefix):]

                logger.info("Copying S3 object: %s to %s", source_key, destination_key)
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': source_key},
                    Key=destination_key
                )
                logger.info("Deleting S3 object: %s", source_key)
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        else:
            for obj in response.get('Contents', []):
                source_key = obj['Key']

                if source_key.endswith(file_name):
                    destination_key = destination_prefix + source_key[len(source_prefix):]

                    logger.info("Copying matching S3 object: %s to %s", source_key, destination_key)
                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': source_key},
                        Key=destination_key
                    )
                    logger.info("Deleting matching S3 object: %s", source_key)
                    s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                    logger.info("Moved S3 file: %s to %s", source_key, destination_key)

        return f"Data Moved successfully from {source_prefix} to {destination_prefix}"
    except Exception as e:
        logger.error("Error occurred while moving files in S3: %s", e)
        logger.error("Traceback: %s", traceback.format_exc())
        raise e

def move_local_to_local():
    """Placeholder for local file move operations."""
    pass

