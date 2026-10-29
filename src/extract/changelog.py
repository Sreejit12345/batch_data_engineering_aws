# Entry point for raw to bronze ingestion job.
# high level - this code is responsible to generate a changelog along with delta column(insert,update,delete) by comparing 
# the [last processed bronze batchId by bronze] vs [the next batchId in raw].

'''
 This sets up 1 spark catalog (glue_catalog). This spark catalog is internally mapped to the default account specific aws glue catalog 
 We already have 3 databases set up inside the default account specific aws glue catalog , with respective locations stored in the database level.
 
 Iceberg's GlueCatalog uses a fallback mechanism (defaultWarehouseLocation) to determine where to store data for new tables:   

It first checks if the target Glue database has an explicitly defined database location (databaseUri).
Because your 3 databases already have locations present, Iceberg will successfully use databaseUri as the default storage location for any new table created within those databases.



create a key named --conf and put the value as - this is done with the help of cloudformation:


spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true

'''

from src.extract.utils.common_functions import *
from src.extract.utils.read_config_file import *
import logging


if(__name__=='__main__'):

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

    logger = logging.getLogger("ChangeLog Job")


    config_dict=initialize_context_dict(logger)

    spark_sess=get_spark_session()

    metadata_tble_existence= does_table_exists(spark_sess,config_dict['metadata']['database_name'],
                                               config_dict['metadata']['raw_bronze_table'])

    if(metadata_tble_existence==False):
        logger.info(f"Metadata table does not exist- creating table raw-bronze-batch-progress-{config_dict['env']}")



