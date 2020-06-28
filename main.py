from pathlib import Path

from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark import SparkContext, SQLContext


SPARK_CONTEXT = SparkContext()
SQL_CONTEXT = SQLContext(SPARK_CONTEXT)


def load_and_concatenate_files():
    data_schema = get_schema()
    data_dir = Path('data')
    all_dfs = {}

    for file_loc in data_dir.iterdir():
        city_name = '_'.join(file_loc.stem.split('_')[:-1])
        df = (
            SQL_CONTEXT
            .read.format('com.databricks.spark.csv')
            .options(header='true', schema=data_schema, inferSchema=False, delimiter=',')
            .load(file_loc)
        )
        all_dfs[city_name] = df
    
    return all_dfs


def get_schema():
    """Create a pyspark schema for the listing data files. Every file has the same column names and
    types. The variable col_items is a dictionary where each key is a column name and its item is the
    column data type. 
    """
    col_items = {
        'id': 'int', 'name': 'str', 'host_id': 'int', 'host_name': 'str',
        'neighbourhood_group': 'str', 'neighbourhood': 'str', 'latitude': 'float',
        'longitude': 'float', 'room_type': 'str', 'price': 'int', 'minimum_nights': 'int',
        'number_of_reviews': 'int', 'last_review': 'date', 'reviews_per_month': 'float',
        'calculated_host_listings_count': 'int', 'availability_365': 'int'
    }
    python_to_pyspark = {
        'str': T.StringType(), 'float': T.FloatType(), 'int': T.IntegerType(), 'date': T.DateType()
    }
    listing_schema = []

    for col_name, col_type in col_info.items():
        listing_schema.append(
            T.StructField(
                name=col_name,
                dataType=python_to_pyspark[col_type],
                nullable=True
            )
        )
 
    listing_schema = T.StructType(listing_schema)

    return listing_schema
