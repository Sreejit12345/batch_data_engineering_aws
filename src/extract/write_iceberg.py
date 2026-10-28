from pyiceberg.catalog import load_catalog
import datetime
from src.extract.schema.schema import ARROW_SCHEMAS
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform


# database raw-githubanalytics-dev has already been creasted by cloudformation with location pointing to s3://raw-githubanalytics-dev/warehouse/
# for local auth the user executing must have aws cli installed and necessary iam permissions .
# In aws glue it is governed by iam role policies

def initialize_catalog(catalog_name,aws_region):
    glue_catalog=load_catalog(
        name=catalog_name,
        **{
            'type': 'glue',
            'client.region': aws_region,
        }
    )

    return glue_catalog


def write_table(table_name,df,logger,database_name,catalog_name,aws_region):

    # appends df to iceberg table 'table_name'. Also creates 1 if it does not exist

    logger.info("Looking up schema for table %s", table_name)

    schema= ARROW_SCHEMAS[table_name]
    catalog = initialize_catalog(catalog_name,aws_region)
    identifier = f"{database_name}.{table_name}"

    if not catalog.table_exists(identifier):

        logger.info(f" Table {identifier} does not exist! creating new table ")
        catalog.create_table(
            identifier=identifier,
            schema=schema,
        )

        table = catalog.load_table(identifier)

        with table.transaction() as txn:
            with txn.update_spec() as spec_upd:
                spec_upd.add_field("ingestion_timestamp", DayTransform(), "ingestion_day")


    table = catalog.load_table(identifier)
    arrow_table = pa.Table.from_pylist(df, schema=table.schema().as_arrow())

    table.append(arrow_table)

    logger.info(f"loaded table {identifier} with {len(arrow_table)} rows")

