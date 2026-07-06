import os
import shutil
import traceback
from src.main.utility.logging_config import logger

def delete_local_file(delete_file_path):
    """Deletes files or directories locally. If a directory path is provided, all its contents are deleted recursively."""
    if not os.path.exists(delete_file_path):
        logger.warning("Local path '%s' does not exist. Skipping deletion.", delete_file_path)
        return
        
    try:
        if os.path.isfile(delete_file_path):
            os.remove(delete_file_path)
            logger.info("Deleted file: %s", delete_file_path)
            return

        files_to_delete = [
            os.path.join(delete_file_path, filename) 
            for filename in os.listdir(delete_file_path)
        ]
        for item in files_to_delete:
            if os.path.isfile(item):
                os.remove(item)
                logger.info("Deleted file: %s", item)
            elif os.path.isdir(item):
                shutil.rmtree(item)
                logger.info("Deleted folder and contents: %s", item)
    except Exception as e:
        logger.error("Error occurred while deleting local files at '%s': %s", delete_file_path, e)
        logger.error("Traceback: %s", traceback.format_exc())
        raise e

