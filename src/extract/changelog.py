# high level - this code is responsible to generate a changelog along with delta column(insert,update,delete) by comparing 
# the [last processed bronze batchId by bronze] vs [the next batchId in raw].

'''
 This sets up 1 spark catalog (glue_catalog). This spark catalog is internally mapped to the default account specific aws glue catalog 
 We already have 3 databases set up inside the default account specific aws glue catalog , with respective locations stored in the database level.
 
 Iceberg's GlueCatalog uses a fallback mechanism (defaultWarehouseLocation) to determine where to store data for new tables:   

It first checks if the target Glue database has an explicitly defined database location (databaseUri).
Because your 3 databases already have locations present, Iceberg will successfully use databaseUri as the default storage location for any new table created within those databases.



create a key named --conf and put the value as - this is done with the help of cloudformation:


spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.sql.defaultCatalog=glue_catalog

'''

from src.extract.utils.common_functions import *
from src.extract.utils.read_config_file import *
import logging
from src.extract.schema.bronze_schema import *
from datetime import datetime, timezone
import uuid
from pyspark.sql import functions as F
import sys

# Entry point for raw to bronze ingestion job.
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

    logger = logging.getLogger("ChangeLog Job")


    config_dict=initialize_context_dict(logger)

    metadata_table_name=f"{config_dict['metadata']['database_name']}.{config_dict['metadata']['raw_bronze_table']}"

    spark_sess=get_spark_session()

    metadata_tble_existence= does_table_exists(spark_sess,config_dict['metadata']['database_name'],
                                               config_dict['metadata']['raw_bronze_table'])


    if(metadata_tble_existence==False):
        logger.info(f"Metadata table does not exist- creating table {metadata_table_name}")

        spark_sess.sql(f"""
            CREATE TABLE IF NOT EXISTS {metadata_table_name} (
            database_name STRING,
            table_name STRING,
            layer STRING,
            batch_ordering_columns STRING,
            ts_for_last_processed_batch_id_by_bronze STRING,
            natural_primary_keys STRING,
            last_batch_id_processed_successfully_by_bronze STRING
        )
        USING iceberg
        """)

        logger.info(f"Metadata table {metadata_table_name} created...")


        logger.info("populating entries for table with null values...")

        spark_sess.sql(f"""
        INSERT INTO {metadata_table_name}
        VALUES
        ('raw-githubanalytics-{config_dict['env']}', 'org', 'raw', 'ingestion_timestamp', NULL, 'org_id', NULL),
        ('raw-githubanalytics-{config_dict['env']}', 'repo', 'raw', 'ingestion_timestamp', NULL, 'repo_id', NULL),
        ('raw-githubanalytics-{config_dict['env']}', 'repo_language', 'raw', 'ingestion_timestamp', NULL, 'repo_id,language', NULL),
        ('raw-githubanalytics-{config_dict['env']}', 'language', 'raw', 'ingestion_timestamp', NULL, 'language_name', NULL),
        ('raw-githubanalytics-{config_dict['env']}', 'commits', 'raw', 'ingestion_timestamp', NULL, 'commit_sha', NULL),
        ('raw-githubanalytics-{config_dict['env']}', 'pull_requests', 'raw', 'ingestion_timestamp', NULL, 'pull_request_id', NULL),
        ('raw-githubanalytics-{config_dict['env']}', 'user', 'raw', 'ingestion_timestamp', NULL, 'user_id', NULL)
        """)

    else:

        logger.info("Skipping metadata table creation- table already exists")


    raw_bronze_table_metadata= spark_sess.read.table(metadata_table_name).collect()

    
    for r in raw_bronze_table_metadata:


        # bronze and raw share same table names , only database name is different.
        bronze_table_full_name=config_dict['bronze']['database_name'] + '.' + r['table_name']
        raw_table_full_name = r['database_name'] + '.' + r['table_name']


        logger.info(f"Raw to Bronze Processing for table : {r['table_name']}")

        spark_schema=BRONZE_SCHEMAS[r['table_name']]

        natural_keys= r['natural_primary_keys']
        batch_order= r['batch_ordering_columns']
        
        logger.info(f"{raw_table_full_name}'s corresponding bronze table name is {bronze_table_full_name}")


        #create bronze tables - with same names as raw tables
        create_partitioned_table_if_not_exists(spark_sess,config_dict['bronze']['database_name'],r['table_name'],
                                               spark_schema,["bronze_ingestion_timestamp"])


        if(r['last_batch_id_processed_successfully_by_bronze'] is None and r['ts_for_last_processed_batch_id_by_bronze'] is None ):
            logger.info(f"No processed batch found for table {raw_table_full_name}")

            # finds the next batch based on batch_order
            # Ideally we dont have parallel writes on ingestion so ingestion_timestamp wont be same.

            # new unprocessed batch
            source_query= f"""

            with first_batch as(
            select min({batch_order}) as first_batch_ts from {raw_table_full_name}
            )
            select r.* from {raw_table_full_name} r cross join first_batch f
            where r.{batch_order}=f.first_batch_ts
            """

            # next batch - empty
            target_query= f"""
            select *
            from {raw_table_full_name}
            where 1=2      
            """
            

        else:
            last_batch_processed=r['last_batch_id_processed_successfully_by_bronze']
            last_batch_processed_ts= r['ts_for_last_processed_batch_id_by_bronze']


            # finds the next batch based on batch_order
            # Ideally we dont have parallel writes on ingestion so ingestion_timestamp wont be same.

            # new unprocessed batch
            source_query= f"""
                 with first_batch as(
                        select min({batch_order}) as first_batch_ts from {raw_table_full_name}
                         where ingestion_timestamp >='{last_batch_processed_ts}'
                                        and batch_id != '{last_batch_processed}'
                        )
                    select r.* from 
                    {raw_table_full_name} r
                    cross join first_batch f
                    where r.{batch_order}=f.first_batch_ts
            """

            # batch already processed by bronze pipeline, but we pick up from raw
            target_query= f"""
            select * from 
                {raw_table_full_name} 
                where ingestion_timestamp ='{last_batch_processed_ts}'
                and batch_id = '{last_batch_processed}'     
            """


        new_batch=spark_sess.sql(source_query)

        if(new_batch.count()==0):
            logger.info("no new batches exist , quiting workflow.....")
            sys.exit()

        processed_batch= spark_sess.sql(target_query)

        final_df= generate_chagelog(new_batch,processed_batch,natural_keys.split(','))

        
        # add bronze audit columns

        final_df= final_df.withColumn("bronze_batch_id",F.lit(my_uuid)).\
        withColumn("bronze_ingestion_timestamp",F.lit(ingestion_ts))


        # lineage columns

        row=new_batch.limit(1).collect()[0]

        try:
        
            final_df.writeTo(f"{bronze_table_full_name}").append()

            logger.info(f"Table {bronze_table_full_name} processed successfully")

            spark_sess.sql(f"""
            UPDATE {metadata_table_name} 
            SET ts_for_last_processed_batch_id_by_bronze ='{row['ingestion_timestamp']}' ,
            last_batch_id_processed_successfully_by_bronze = '{row['batch_id']}'
            WHERE table_name = '{r['table_name']}'
            and database_name = '{r['database_name']}'
                        
            """)

            logger.info(f"Table {bronze_table_full_name} metadata processed successfully")

        except:
            spark_sess.sql(f"""
            DELETE FROM {bronze_table_full_name}
            WHERE bronze_batch_id = '{my_uuid}'
            """)

            spark_sess.sql(f"""
            UPDATE {metadata_table_name}
            SET ts_for_last_processed_batch_id_by_bronze =
                {("NULL" if r['ts_for_last_processed_batch_id_by_bronze'] is None else "'" + str(r['ts_for_last_processed_batch_id_by_bronze']) + "'")},
                last_batch_id_processed_successfully_by_bronze =
                {("NULL" if r['last_batch_id_processed_successfully_by_bronze'] is None else "'" + str(r['last_batch_id_processed_successfully_by_bronze']) + "'")}
            WHERE table_name = '{r['table_name']}'
            AND database_name = '{r['database_name']}'
            """)
            raise
        

           






            




    
    
