# High level - thids job is responsible for doing scd1/2 based on generic loaders
'''
 This sets up 1 spark catalog (glue_catalog). This spark catalog is internally mapped to the default account specific aws glue catalog 
 We already have 3 databases set up inside the default account specific aws glue catalog , with respective locations stored in the database level.
 
 Iceberg's GlueCatalog uses a fallback mechanism (defaultWarehouseLocation) to determine where to store data for new tables:   

It first checks if the target Glue database has an explicitly defined database location (databaseUri).
Because your 3 databases already have locations present, Iceberg will successfully use databaseUri as the default storage location for any new table created within those databases.

Although we have the warehouse set in database level- we need to set a dummy spark warehouse for glue to start:  --conf spark.sql.catalog.glue_catalog.warehouse=s3://idfk/

create a key named --conf and put the value as - this is done with the help of cloudformation:


spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.sql.defaultCatalog=glue_catalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://idfk/

'''



import logging
from datetime import datetime, timezone
import uuid
import sys
from src.utils.read_config_file import *
from src.utils.common_functions import *
from src.transform.schema.silver_schema import *
from src.transform.scd1 import *
from src.transform.scd2 import *

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


    metadata_table_sql_name = f"glue_catalog.`{config_dict['metadata']['database_name']}`.`{config_dict['metadata']['bronze_silver_table']}`"

    spark_sess=get_spark_session()

    metadata_tble_existence= does_table_exists(spark_sess,metadata_table_sql_name)


    if(metadata_tble_existence==False):
        logger.info(f"Metadata table does not exist- creating table {metadata_table_sql_name}")

        spark_sess.sql(f"""
            CREATE TABLE IF NOT EXISTS {metadata_table_sql_name} (
            database_name STRING,
            table_name STRING,
            layer STRING,
            batch_ordering_columns STRING,
            tgt_load_type STRING,
            tgt_table_type STRING,
            ts_for_last_processed_batch_id_by_silver TIMESTAMP,
            natural_primary_keys STRING,
            last_batch_id_processed_successfully_by_silver STRING
        )
        USING iceberg
        """)

        logger.info(f"Metadata table {metadata_table_sql_name} created...")


        logger.info("populating entries for table with null values...")

        spark_sess.sql(f"""
        INSERT INTO {metadata_table_sql_name}
        VALUES
        ('bronze-githubanalytics-{config_dict['env']}', 'org', 'bronze', 'bronze_ingestion_timestamp','scd2','dim', NULL, 'org_id', NULL),
        ('bronze-githubanalytics-{config_dict['env']}', 'repo', 'bronze', 'bronze_ingestion_timestamp','scd2','dim', NULL, 'repo_id', NULL),
        ('bronze-githubanalytics-{config_dict['env']}', 'language', 'bronze', 'bronze_ingestion_timestamp','scd1','dim', NULL, 'language_name', NULL),
        ('bronze-githubanalytics-{config_dict['env']}', 'user', 'bronze', 'bronze_ingestion_timestamp','scd2','dim', NULL, 'user_id', NULL),
        ('bronze-githubanalytics-{config_dict['env']}', 'repo_language', 'bronze','bronze_ingestion_timestamp', 'scd2', 'bridge', NULL, 'repo_id,language', NULL),
        ('bronze-githubanalytics-{config_dict['env']}', 'commits', 'bronze', 'bronze_ingestion_timestamp','scd1','fact', NULL, 'commit_sha', NULL),
        ('bronze-githubanalytics-{config_dict['env']}', 'pull_requests', 'bronze', 'bronze_ingestion_timestamp','scd1','fact', NULL, 'pull_request_id', NULL)
    
        """)

    else:

        logger.info("Skipping metadata table creation- table already exists")



    bronze_silver_table_metadata = spark_sess.read.table(metadata_table_sql_name).collect()

    
    for r in bronze_silver_table_metadata:


        # bronze and silver share same table names , only database name is different.

        silver_table_sql_name = (
            f"glue_catalog.`{config_dict['silver']['database_name']}`.`{r['table_name']}`"
        )
        bronze_table_sql_name = f"glue_catalog.`{r['database_name']}`.`{r['table_name']}`"

        logger.info(f"bronze to silver processing for table : {r['table_name']}")

        spark_schema=SILVER_SCHEMAS[r['table_name']]
        partition=SILVER_PARTITIONS[r['table_name']] #get the corresponding partition columns
        load_type= r['tgt_load_type']
        table_type=r['tgt_table_type']
        pk=r['natural_primary_keys']

        last_batch_processed_ts= r['ts_for_last_processed_batch_id_by_silver']
        last_batch_processed = r['last_batch_id_processed_successfully_by_silver']


        natural_keys= r['natural_primary_keys']
        batch_order= r['batch_ordering_columns']
        
        logger.info(f"{bronze_table_sql_name}'s corresponding silver table name is {silver_table_sql_name}")

        if(partition is None):

            logger.info(f"Creating non partitioned table {silver_table_sql_name} if it does not exist!")

            #create silver tables - with same names as raw tables
            create_non_partitioned_table_if_not_exists(spark_sess,"glue_catalog",config_dict['silver']['database_name'],r['table_name'],
                                                spark_schema)

        else:

              logger.info(f"Creating table {silver_table_sql_name} partitioned on {partition} if it does not exist!")

              create_partitioned_table_if_not_exists(spark_sess,"glue_catalog",config_dict['silver']['database_name'],r['table_name'],
                                                            spark_schema,partition)



        if(r['last_batch_id_processed_successfully_by_silver'] is None and r['ts_for_last_processed_batch_id_by_silver'] is None ):

            logger.info(f"No processed batch found for table {bronze_table_sql_name}")

            source_query= f"""
                        with first_batch as(
                        select min({batch_order}) as first_batch_ts from {bronze_table_sql_name}
                        )
                        select r.* from {bronze_table_sql_name} r cross join first_batch f
                        where r.{batch_order}=f.first_batch_ts
                    """

        else:

            source_query= f"""
                          with first_batch as(
                                 select min({batch_order}) as first_batch_ts from {bronze_table_sql_name}
                                  where bronze_ingestion_timestamp >='{last_batch_processed_ts}'
                                                 and bronze_batch_id != '{last_batch_processed}'
                                 )
                             select r.* from 
                             {bronze_table_sql_name} r
                             cross join first_batch f
                             where r.{batch_order}=f.first_batch_ts
                     """

        source_df= spark_sess.sql(source_query)

        if(source_df.count() ==0):
            logger.info("no new batches exist , proceeding to next table.....")
            continue


        source_df=source_df.withColumn('silver_batch_id',F.lit(my_uuid)).\
        withColumn('silver_ingestion_timestamp',F.lit(ingestion_ts)).\
        withColumn('is_deleted',
                  F.when(F.col('delta_type')==F.lit('delete'),F.lit(True)).otherwise(F.lit(False)))

        row=source_df.limit(1).collect()[0]

        sk_reqd=False

        try:

               if(table_type=='dim'):
                    #generate -99 key for all dimensions
                    generate_default_key_for_dim(spark_sess,"glue_catalog", f"`{config_dict['silver']['database_name']}`",
                                                              f"`{r['table_name']}`",
                                                              f"{r['table_name']}_sk")
                    sk_reqd=True

             
            
               if(load_type=='scd1'): 

                    if(table_type=='fact' and SILVER_FOREIGN_KEYS_REPLACEMENTS[r['table_name']]!=None): #for facts we might need to replace keys
                         
                         source_df=natural_key_replacement_sk(spark_sess,logger,"glue_catalog", f"`{config_dict['silver']['database_name']}`",
                                                              f"{r['table_name']}",
                                                              source_df,SILVER_FOREIGN_KEYS_REPLACEMENTS) 

                         logger.info(f"Keys replaced for table `{r['table_name']}`")   

                    

                    scd1_implementation(spark_sess,logger,source_df,"glue_catalog",
                                    f"`{r['table_name']}`",f"`{config_dict['silver']['database_name']}`",pk.split(','),sk_reqd)

                    
        
                    logger.info(f"Table {silver_table_sql_name} processed successfully by scd1 pattern!")

               elif(load_type=='scd2'):

                    if(r['table_name']=='repo_language'):
                         # for bridge table we use bronze_ingestion_timestamp as scd2 column
                        scd2_implementation(spark_sess,logger,source_df,"glue_catalog",
                                        f"`{r['table_name']}`",f"`{config_dict['silver']['database_name']}`",
                                        pk.split(','),'bronze_ingestion_timestamp')
                    else: # for all others we use updated_at
                          scd2_implementation(spark_sess,logger,source_df,"glue_catalog",
                                                                 f"`{r['table_name']}`",f"`{config_dict['silver']['database_name']}`",
                                                                 pk.split(','),'updated_at')
                         

                    logger.info(f"Table {silver_table_sql_name} processed successfully by scd2 pattern!")

                         

               spark_sess.sql(f"""
                            UPDATE {metadata_table_sql_name} 
                            SET ts_for_last_processed_batch_id_by_silver =
                                CAST('{row['bronze_ingestion_timestamp']}' AS TIMESTAMP),
                            last_batch_id_processed_successfully_by_silver = '{row['bronze_batch_id']}'
                            WHERE table_name = '{r['table_name']}'
                            and database_name = '{r['database_name']}'
                                        
                            """)

               logger.info(f"Table {bronze_table_sql_name} metadata processed successfully")

        except:

            logger.info("Something went wrong in processing table")

            spark_sess.sql(f"""
            UPDATE {metadata_table_sql_name}
            SET ts_for_last_processed_batch_id_by_silver =
                {("NULL" if r['ts_for_last_processed_batch_id_by_silver'] is None else "CAST('" + str(r['ts_for_last_processed_batch_id_by_silver']) + "' AS TIMESTAMP)")},
                last_batch_id_processed_successfully_by_silver =
                {("NULL" if r['last_batch_id_processed_successfully_by_silver'] is None else "'" + str(r['last_batch_id_processed_successfully_by_silver']) + "'")}
            WHERE table_name = '{r['table_name']}'
            AND database_name = '{r['database_name']}'
            """)

            raise




    