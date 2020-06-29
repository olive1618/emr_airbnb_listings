from pathlib import Path

from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark import SparkContext, SQLContext


SPARK_CONTEXT = SparkContext()
SQL_CONTEXT = SQLContext(SPARK_CONTEXT)


def load_and_concatenate_files():
    """Load the file for the first city to a pyspark dataframe. Then iterate through the list of
    file names, load each to a dataframe, and merge each with the main DF
    """
    data_schema = get_schema()
    data_dir = Path('data')
    file_names = [file_loc for file_loc in data_dir.iterdir()]

    first_file = file_names.pop()
    main_df = (
        SQL_CONTEXT
        .read.format('com.databricks.spark.csv')
        .options(header='true', schema=data_schema, inferSchema=False, delimiter=',')
        .load(str(first_file))
    )
    city_name = '_'.join(first_file.stem.split('_')[:-1])
    main_df = main_df.withColumn('city_name', F.lit(city_name))

    for file_loc in file_names:
        city_name = '_'.join(file_loc.stem.split('_')[:-1])
        df = (
            SQL_CONTEXT
            .read.format('com.databricks.spark.csv')
            .options(header='true', schema=data_schema, inferSchema=False, delimiter=',')
            .load(str(file_loc))
        )
        df = df.withColumn('city_name', F.lit(city_name))
        main_df = main_df.union(df)

    return main_df


def get_schema():
    """Create a pyspark schema for the listing data files. Every file has the same column names and
    types. The variable col_items is a dictionary where each key is a column name and its item is
    the column data type
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

    for col_name, col_type in col_items.items():
        listing_schema.append(
            T.StructField(
                name=col_name,
                dataType=python_to_pyspark[col_type],
                nullable=True
            )
        )

    listing_schema = T.StructType(listing_schema)

    return listing_schema


if __name__ == "__main__":
    data_dfs = load_and_concatenate_files()
