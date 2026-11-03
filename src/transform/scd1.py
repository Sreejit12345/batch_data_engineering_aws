from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.common_functions import *


def scd1_implementation(spark_session,logger,df,catalog_name,target_table,target_database,primary_keys,isskreqd):

    full_table_name=f"{catalog_name}.{target_database}.{target_table}"

    logger.info(f"Applying silver merge for table {full_table_name}")

    # generate join condition for merge from tgt columns
    full_join_cond=None
    for col in primary_keys:
        d= f"t.{col}=s.{col}"

        if full_join_cond is None:
            full_join_cond=d
        else:
            full_join_cond= full_join_cond + " AND " + d

    # generate columsn to update via merge- bronze and silver tables share the same column names apart from audit columns , delta columns and surrgate key
    target_df=spark_session.sql(f"select * from {full_table_name} limit 1")
    raw_audit_cols=['raw_batch_id','raw_ingestion_timestamp']
    reqd_cols=[i for i in target_df.columns 
               if (i not in primary_keys) and (i not in raw_audit_cols) and (i!='delta_type') and (i!=f"{target_table.replace('`','')}_sk")]

    update_exp=None
    for i in reqd_cols:
        x= f"t.{i}=s.{i}"

        if update_exp is None:
            update_exp=x
        else:
            update_exp= update_exp + ' , ' + x

    if(isskreqd is True):
        # add surogate key to incoming df - needed for the insert part
        max_key=derive_max_surrogate_key(spark_session,catalog_name,target_database,target_table,f"{target_table.replace('`','')}_sk")
        wnd= Window.orderBy(F.col(primary_keys[0]))
        df=df.withColumn(f"{target_table.replace('`','')}_sk",F.row_number().over(wnd)+ F.lit(max_key))

    #generate columns to insert by merge 
    insert_stmt=None
    values_stmt=None
    for i in target_df.columns:
        x = f"INSERT( {i}"
        y= f"VALUES (s.{i})"
        if(insert_stmt is None):
            insert_stmt=x
        else:
            insert_stmt= insert_stmt+ f', {i}'

        if(values_stmt is None):
            values_stmt=y
        else:
            values_stmt=values_stmt+  f', s.{i}'
 
    insert_stmt=insert_stmt+')'
    values_stmt=values_stmt+')'



    df.createOrReplaceTempView("source_table")

    spark_session.sql(f"""
    
    MERGE INTO {full_table_name} t   
    USING source_table s         
    ON 
    {full_join_cond}
    
    WHEN MATCHED THEN 
    UPDATE SET {update_exp}
        
    WHEN NOT MATCHED THEN 
    {insert_stmt} {values_stmt}

    """)

 
     