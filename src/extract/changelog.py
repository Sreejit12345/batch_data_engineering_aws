# high level - this code is responsible to generate a changelog along with delta column(insert,update,delete) by comparing 
# the [last processed bronze batchId by bronze] vs [the next batchId in raw].


from pyspark.sql import SparkSession


#Constants

metadata_schema_bronze = "metadata-githubanalytics-dev"

def get_spark():

    spark=SparkSession.builder.appName("bronze job").getOrCreate()

    return spark

def query_last_processed_batch_bronze():

    return "c8ca1a58-6617-42ce-8176-a38ba251ff35"


