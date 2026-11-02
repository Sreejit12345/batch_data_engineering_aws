def scd1_implementation(spark_session,logger,df,catalog_name,target_table,target_database,primary_keys):

    full_table_name=f"{catalog_name}.{target_database}.{target_table}"

    logger.info(f"Applying silver merge for table {full_table_name}")

    full_join_cond=None
    for col in primary_keys:
        d= f"t.{col}=s.{col}"

        if full_join_cond is None:
            full_join_cond=d
        else:
            full_join_cond= full_join_cond + " AND " + d
        

    df.createOrReplaceTempView("source_table")

    spark_session.sql(f"""
    
    MERGE INTO {full_table_name} t   
    USING source_table s         
    ON 
    {full_join_cond}
    
    WHEN MATCHED THEN 
    UPDATE SET *
        
    WHEN NOT MATCHED THEN 
    INSERT *;
    
    """)
    
     