
from pyspark.sql import SparkSession

def does_table_exists(spark,database,tbl_name):

    exists = spark.catalog.tableExists(tbl_name,database)
    return exists

def get_spark_session():

    spark=SparkSession.builder.appName("bronze job").getOrCreate()

    return spark