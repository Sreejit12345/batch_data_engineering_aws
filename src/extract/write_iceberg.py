from pyiceberg.catalog import load_catalog
import datetime
from src.extract.schema.schema import ARROW_SCHEMAS
import pyarrow as pa

# database bronze-githubanalytics-dev has already been creasted by cloudformation with location pointing to s3://bronze-githubanalytics-dev/warehouse/
# for local auth the user executing must have aws cli installed and necessary iam permissions .
# In aws glue it is governed by iam role policies

def initialize_catalog():
    glue_catalog=load_catalog(
        name='468094684048',
        **{
            'type': 'glue',
            'client.region': 'ap-south-1',
        }
    )

    return glue_catalog

def get_ingestion_date():
    # returns current date

    today = datetime.date.today()
    formatted_date = str(today)
    return formatted_date


def write_table(table_name,df,logger,database_name):

    logger.info("Looking up schema for table %s", table_name)

    schema= ARROW_SCHEMAS[table_name]
    catalog = initialize_catalog()
    identifier = f"{database_name}.{table_name}"

    catalog.create_table_if_not_exists(
        identifier=identifier,
        schema=schema,
    )
    
    table = catalog.load_table(identifier)

    arrow_table = pa.Table.from_pylist(df, schema=table.schema().as_arrow())

    table.append(arrow_table)
