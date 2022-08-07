from itertools import chain
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def keep_and_rename_columns(df: DataFrame, cols: dict) -> DataFrame:
    # TODO: COmmenting keep and rename cols
    df = df.select(list(cols.keys()))
    for og_name, new_name in cols.items():
        if og_name in df.columns:
            df = df.withColumnRenamed(og_name, new_name)
    return df

def non_null(colname: str) -> Column:
    """ Returns the non-null filter for a column

    Args:
        colname (str): column name

    Returns:
        `Column`: Column filter
    """
    return F.col(colname).isNotNull()

def non_negative(colname: str) -> Column:
    """ Returns the non-negative filter for a column

    Args:
        colname (str): column name

    Returns:
        `Column`: Column filter
    """
    return F.col(colname) >= 0

def strictly_positive(colname: str) -> Column:
    """ Returns the strictly positive filter for a column

    Args:
        colname (str): column name

    Returns:
        `Column`: Column filter
    """
    return F.col(colname) > 0

def extract_cdc_week(df: DataFrame, mmwr_weeks_df: DataFrame) -> DataFrame:
    colnames = df.columns
    return df.join(
        mmwr_weeks_df,
        on=['day', 'month', 'year'],
        how='inner'
    ).select(['week_index', 'cdc_week'] + colnames)

def extract_date_columns(df: DataFrame, mmwr_weeks_df:DataFrame, date_col: str) -> DataFrame:
    # TODO: commending on extract_day_month_year
    if dict(df.dtypes)[date_col] == 'timestamp':
        df = df\
            .withColumn('date', F.date_format(F.col(date_col),'MM/dd/yyyy'))
    else:
        df = df\
            .withColumn('date', F.col(date_col))

    colnames = df.columns

    # extract the day, month and year
    df = df\
        .withColumn('month', F.col('date').substr(1,2).cast(IntegerType()))\
        .withColumn('day', F.col('date').substr(4,2).cast(IntegerType()))\
        .withColumn('year', F.col('date').substr(7,4).cast(IntegerType()))

    # extract the cdc week
    df = extract_cdc_week(df, mmwr_weeks_df)

    return df\
        .select(['year', 'month', 'day', 'week_index', 'cdc_week'] + colnames)

def perform_cleaning(df: DataFrame, mmwr_weeks_df: DataFrame,
    keep_cols: dict, cleaning_dict: dict) -> DataFrame:
    # TODO: commenting perform cleanign

    df = keep_and_rename_columns(df, keep_cols)

    for colname, cleaning_functions in cleaning_dict.items():
        for cleaning_f in cleaning_functions:
            df = df.where(cleaning_f(colname))

    df = extract_date_columns(df, mmwr_weeks_df, 'date')

    return df
    
def replace_column_using_dict(df:DataFrame, colname: str, mappings: dict) -> DataFrame:
    # TODO: comment replace_column_using_dict
    # from: https://stackoverflow.com/questions/42980704/pyspark-create-new-column-with-mapping-from-a-dict
    map_expr = F.create_map([F.lit(x) for x in chain(*mappings.items())])
    return df.withColumn(colname, map_expr[F.col(colname)])