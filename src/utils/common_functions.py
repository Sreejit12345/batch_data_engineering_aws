from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.extract.schema.bronze_schema import *

def does_table_exists(spark,tbl_name):

    exists = spark.catalog.tableExists(tbl_name)
    return exists

def get_spark_session():
    spark=SparkSession.builder.appName("bronze job").getOrCreate()

    return spark

def derive_max_surrogate_key(spark,catalog,database,table_name,sk_col_name):

    val=spark.sql(f"select coalesce(max({sk_col_name}),0) mx_key from {catalog}.{database}.{table_name} ").collect()[0]

    return val['mx_key']


def deduplicate_on_key(df,keys,order_dict):

    '''
    df - Dataframe to be deduplicated on 
    keys - array of columns which is used as partitionBy. Eg: ['key1','key2']
    order_dict - dict of keys value pairs where value can be asc/desc. used in orderBy   Eg: {'key':'asc', 'key1':'desc'}
    '''

    
    sort_key=[]

    for key,value in order_dict.items():

        if(value.lower()=='asc'):
            n=F.col(key).asc()
        else:
            n=F.col(key).desc()

        sort_key.append(n)


    w=Window.partitionBy(*keys).orderBy(*sort_key)

    df=df.withColumn('rn',F.row_number().over(w)).filter('rn==1').drop('rn')

    return df


def create_partitioned_table_if_not_exists(spark,catalog,database,table_name,schema,partitionBy):

    table_exists=does_table_exists(spark,f"{catalog}.`{database}`.`{table_name}`")

    sql_schema= schema.toDDL()

    if(table_exists==False):
         spark.sql(f"""
            CREATE TABLE {catalog}.`{database}`.`{table_name}`
            (
            {sql_schema}
            )
           USING iceberg
            {partitionBy}
            """)

def create_non_partitioned_table_if_not_exists(spark,catalog,database,table_name,schema):

    table_exists=does_table_exists(spark,f"{catalog}.`{database}`.`{table_name}`")

    sql_schema= schema.toDDL()

    if(table_exists==False):
         spark.sql(f"""
            CREATE TABLE {catalog}.`{database}`.`{table_name}`
            (
            {sql_schema}
            )
           USING iceberg
            """)


def generate_chagelog(new_batch,processed_batch,join_cols):
    # join columns are natural keys and must be present in both dfs . These are used to join and find diff in batches.
    # join columns are also used to deduplicate each df before join

    # build order dict for dedup
    
    sort_d={}
    for i in join_cols:
        sort_d[i]='asc'


    # form alias

    new_batch_rename_list=[]
    new_batch_col_list=[]

    for i in new_batch.columns:
        x=F.col(f"new.{i}").alias(f'new_{i}')
        new_batch_rename_list.append(x)
        new_batch_col_list.append(f'new_{i}')


    processed_batch_rename_list=[]
    processed_batch_col_list=[]

    for i in processed_batch.columns:
        x=F.col(f"processed.{i}").alias(f'processed_{i}')
        processed_batch_rename_list.append(x)
        processed_batch_col_list.append(f'processed_{i}')
    

    new_batch=deduplicate_on_key(new_batch,join_cols,sort_d)
    processed_batch=deduplicate_on_key(processed_batch,join_cols,sort_d)

    join_cond= None
    for i in join_cols:
        c= F.col(f"new.{i}")==F.col(f"processed.{i}")

        if(join_cond is None):
            join_cond=c
        else:
            join_cond = join_cond & c

    jdf=new_batch.alias('new').join(processed_batch.alias('processed'),join_cond,"outer").\
    select(*new_batch_rename_list,*processed_batch_rename_list)

    #natural keys renamed
    #natural keys are non-nullabale so comparing 1 is enough

    natural_keys_new=['new_'+i for i in join_cols]
    natural_keys_processed=['processed_'+i for i in join_cols]


    #  insert detection

    insert_df=jdf.filter(F.col(natural_keys_processed[0]).isNull()).select(*new_batch_col_list)

    insert_df=insert_df.withColumn("delta_type",F.lit("insert"))

    #  delete detection

    delete_df=jdf.filter(F.col(natural_keys_new[0]).isNull()).select(*processed_batch_col_list)

    delete_df=delete_df.withColumn("delta_type",F.lit("delete"))

    # update detection

    update_df=jdf.filter( (F.col(natural_keys_new[0]).isNotNull()) & (F.col(natural_keys_processed[0]).isNotNull()) )


    audit_cols_raw=['new_ingestion_timestamp','new_batch_id','new_source',
                    'processed_ingestion_timestamp','processed_batch_id','processed_source']

    new_cols_wo_audit=[ i for i in new_batch_col_list if i not in audit_cols_raw ]
    processed_cols_wo_audit =  [ i for i in processed_batch_col_list if i not in audit_cols_raw ]

    new_cols_wo_audit_exp=[]
    for i in new_cols_wo_audit:
        k=F.coalesce(F.col(i).cast('string'),F.lit('UNKNOWN'))
        new_cols_wo_audit_exp.append(k)

    processed_cols_wo_audit_exp=[]
    for i in processed_cols_wo_audit:
        k=F.coalesce(F.col(i).cast('string'),F.lit('UNKNOWN'))
        processed_cols_wo_audit_exp.append(k)
    

    update_df= update_df.withColumn('new_col_hash',F.sha2(F.concat_ws(':',*new_cols_wo_audit_exp),256)).\
    withColumn('processed_col_hash',F.sha2(F.concat_ws(':',*processed_cols_wo_audit_exp),256)).\
    filter("new_col_hash!=processed_col_hash").\
    select(*new_batch_col_list)

    update_df=update_df.withColumn("delta_type",F.lit('update'))


    full_df= insert_df.union(update_df).union(delete_df)

    all_cols= full_df.columns
    all_cols.remove('delta_type')

    for i in range(0,len(all_cols)):

        full_df=full_df.withColumnRenamed(all_cols[i],new_batch.columns[i])


    full_df=full_df.withColumnRenamed('ingestion_timestamp','raw_ingestion_timestamp').\
    withColumnRenamed('batch_id','raw_batch_id')
        
    return full_df







