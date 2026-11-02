import logging
from datetime import datetime, timezone
import uuid
import sys
from src.utils.read_config_file import *


# Entry point for bronze to silver generic transformation job.
if(__name__=='__main__'):


    #CONSTANTS per batch
    ingestion_ts= datetime.now(timezone.utc) # at module level so all records in batch have same ingestion timestamps.
    my_uuid = str(uuid.uuid4()) # at module level, so all records in bacth have same batch_id

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root_logger.addHandler(handler)

    logger = logging.getLogger("Silver Load Generic Job")


    config_dict=initialize_context_dict(logger)