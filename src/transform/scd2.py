from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.common_functions import *
from datetime import datetime


def scd2_implementation(spark_session,logger,df,catalog_name,target_table,silver_database,primary_keys,scd2_time_col):

    # Each df that is being read from bronze is always deduplicated on primary_keys- in bronze so there wont be any duplicates

    # scd2_time_col is the time column from upsteam which we do scd2 on

    max_end_timestamp = datetime(2099, 12, 31, 0, 0, 0)
    full_table_name_silver=f"{catalog_name}.{silver_database}.{target_table}"

    logger.info(f"Proceesing to load table {full_table_name_silver} by scd2 pattern..")

    # we do batch wise processing from raw onwards and each batch has strictly 1 natural key comb( since that is unique per batch)

    # add surogate key to incoming df
    max_key=derive_max_surrogate_key(spark_session,catalog_name,silver_database,target_table,f"{target_table.replace('`','')}_sk")
    wnd= Window.orderBy(F.col(primary_keys[0]))
    df=df.withColumn(f"{target_table.replace('`','')}_sk",F.row_number().over(wnd)+ F.lit(max_key))

    silv_cols=spark_session.sql(f"select * from {full_table_name_silver} limit 1").columns

    
    insert_df=df.filter("delta_type=='insert'")
    update_df=df.filter("delta_type=='update'")
    delete_df=df.filter("delta_type=='delete'")

    # Preparing insert df

    insert_df= insert_df.withColumn("valid_from_timestamp",F.col(scd2_time_col)).\
    withColumn("valid_to_timestamp",F.lit(max_end_timestamp)).\
    withColumn("is_active",F.lit(True))

    insert_insert_df=insert_df.select(*silv_cols)


    # Preparing update df 

    update_df.createOrReplaceTempView('src')


    # generate join condition for merge from tgt columns

    full_join_cond=None
    for col in primary_keys:
        d= f"t.{col}=s.{col}"

        if full_join_cond is None:
            full_join_cond=d
        else:
            full_join_cond= full_join_cond + " AND " + d

    update_update_merge_stmt= f"""
    
    MERGE INTO {full_table_name_silver} t
    using src s
    ON {full_join_cond}
    AND t.is_active=True

    WHEN MATCHED THEN 
        UPDATE 
         SET t.is_active = False, t.valid_to_timestamp = s.{scd2_time_col}

    """

    #insert part of update df


    update_insert_df= update_df.withColumn("valid_from_timestamp",F.col(scd2_time_col)).\
    withColumn("valid_to_timestamp",F.lit(max_end_timestamp)).\
    withColumn("is_active",F.lit(True))

    update_insert_df=update_insert_df.select(*silv_cols)


    # Preparing delete df 

    # For delete- we mark the record is_deleted as true and simply expire the active record. We dont insert any new record.


    delete_df.createOrReplaceTempView('src_del') # this is the old that needs to get expired


    delete_update_merge_stmt= f"""
    
    MERGE INTO {full_table_name_silver} t
    using src_del s
    ON {full_join_cond}
    AND t.is_active=True

    WHEN MATCHED THEN 
        UPDATE 
         SET t.is_active = False, t.valid_to_timestamp = s.{scd2_time_col}, t.is_deleted=s.is_deleted
    """

    # Statement execution


    spark_session.sql(update_update_merge_stmt)

    spark_session.sql(delete_update_merge_stmt)


    all_insert=insert_insert_df.unionByName(update_insert_df)

    all_insert.writeTo(full_table_name_silver).append()

