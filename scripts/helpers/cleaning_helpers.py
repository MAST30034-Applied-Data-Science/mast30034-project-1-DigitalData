''' Provides functions for cleaning datasets.

Xavier Travers
1178369
'''
from itertools import chain
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def keep_and_rename_columns(df: DataFrame, cols: dict) -> DataFrame:
    """ Extracts columns and renames them.

    Args:
        df (`DataFrame`): The dataset to clean
        cols (dict): The columns to extract and rename

    Returns:
        `DataFrame`: Dataset with renamed selected columns
    """

    # first extract all the columns
    df = df.select(list(cols.keys()))

    # iterate through the columns and remove rows 
    # based on the defined cleaning functions
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

def extract_mmwr_week(df: DataFrame, mmwr_weeks_df: DataFrame) -> DataFrame:
    """ Extracts the mmwr week for each date in a dataset via a join.

    Args:
        df (`DataFrame`): The dataset for which to associate MMWR data.
        mmwr_weeks_df (`DataFrame`): The MMWR week dataset.

    Returns:
        `DataFrame`: The desired dataset with extracted MMWR week data.
    """

    colnames = df.columns

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in colnames:
        borough_col = 'pu_borough'
    elif 'do_borough' in colnames:
        borough_col = 'do_borough'
    elif 'borough' in colnames:
        borough_col = 'borough'
    else: 
        borough_col = 'boro'

    # columns to join to mmwr with 
    join_on = ['day', 'month', 'year']

    if borough_col is not 'boro':
        join_on += [borough_col]

    return df\
        .join(
            mmwr_weeks_df.withColumnRenamed('borough', borough_col),
            on=join_on,
            # this is done so that viral data can be set to 0 on "quiet" weeks
            how='right_outer'
        )\
        .select(
            ['week_ending', 'week_year', 'week_month', 'week_index'] 
            + colnames
        )\
        .fillna(0)

def extract_date_columns(df: DataFrame, mmwr_weeks_df:DataFrame, 
        date_col: str = 'date') -> DataFrame:
    """ Extract the date data from the date column of a dataset
    (these date columns are usually in an `mm/dd/yyyy` string format).
    This also adds MMWR week data for each date.

    Args:
        df (`DataFrame`): The dataset with a `"date"` column
        mmwr_weeks_df (`DataFrame`): The MMWR week dataset.
        date_col (str, optional): The name of the date column.

    Returns:
        `DataFrame`: Dataset with extracted date columns (incl. mmwr weeks)
    """

    # first check what type/format the date column is saved in.
    if dict(df.dtypes)[date_col] == 'timestamp':
        df = df\
            .withColumn('date', F.date_format(F.col(date_col),'MM/dd/yyyy'))
    else:
        df = df\
            .withColumn('date', F.col(date_col))

    # save the column names 
    # (since I don't necessary want all MMWR week data)
    colnames = df.columns

    # extract the day, month and year
    df = df\
        .withColumn('month', F.col('date').substr(1,2).cast(IntegerType()))\
        .withColumn('day', F.col('date').substr(4,2).cast(IntegerType()))\
        .withColumn('year', F.col('date').substr(7,4).cast(IntegerType()))

    # extract the cdc week
    df = extract_mmwr_week(df, mmwr_weeks_df)

    return df\
        .select(
            ['year', 'month', 'day', 'week_ending', 'week_year', 'week_month',
                'week_index'] + colnames
        )

def perform_cleaning(df: DataFrame, mmwr_weeks_df: DataFrame,
        keep_cols: dict, cleaning_dict: dict) -> DataFrame:
    """ A superfunction which performs a general cleaning process for a dataset
    from start to finish. Some data may still require external cleaning.
    The process:
    1. Extracts and renames wanted columns
    2. Performs filtering/cleaning per column
    3. Adds MMWR week data

    Args:
        df (`DataFrame`): The dataset to clean
        mmwr_weeks_df (`DataFrame`): The MMWR week dataset
        keep_cols (dict): The columns to keep and rename
        cleaning_dict (dict): The as well as the filter functions

    Returns:
        `DataFrame`: The cleaned dataset
    """

    # Extracts and renames wanted columns
    df = keep_and_rename_columns(df, keep_cols)

    # Performs filtering/cleaning per column
    for colname, cleaning_functions in cleaning_dict.items():
        for cleaning_f in cleaning_functions:
            df = df.where(cleaning_f(colname))

    # Adds MMWR week data and returns dataset
    return extract_date_columns(df, mmwr_weeks_df, 'date')
    
def replace_column_using_dict(df:DataFrame, colname: str, 
        mappings: dict) -> DataFrame:
    """ Given a mapping dictionary, 
    replaces a column in a dataset with mapped values.

    Args:
        df (`DataFrame`): Dataset with the column
        colname (str): Name of the column
        mappings (dict): Dictionary of values mappings 
        (I recommend a `defaultdict` if you don't know the full set of values)

    Returns:
        `DataFrame`: The dataset with the replaced column
    """
    
    # from: https://stackoverflow.com/questions/42980704/pyspark-create-new-column-with-mapping-from-a-dict
    map_expr = F.create_map([F.lit(x) for x in chain(*mappings.items())])
    return df.withColumn(colname, map_expr[F.col(colname)])


def extract_borough_name(df: DataFrame, zones_df:DataFrame, 
        prefix: str) -> DataFrame:
    """ Applicable primarily to TLC datasets. This joins a dataset to the zones
    and extracts the corresponding pickup and dropoff borough per entry.
    Also filters out data not in the 5 boroughs of NYC.

    Args:
        df (`DataFrame`): The TLC dataset
        zones_df (`DataFrame`): The taxi zones dataset
        prefix (str): The prefix to check for (pickup or dropoff)

    Returns:
        `DataFrame`: The dataset with extracted boroughs
    """

    # save the names (I don't want everything added from the taxi zones data)
    colnames = df.columns

    return df.join( # join with the zones df
        zones_df.select(
            F.col('LocationID').alias(f'{prefix}_location_id'), 
            F.col('borough').alias(f'{prefix}_borough')
        ),
        on = f'{prefix}_location_id',
        how = 'inner' 
    )\
    .select( # only keep the borough column from the zones dataset
        colnames + [f'{prefix}_borough']
    )\
    .where( # only keep locations within the 5 boroughs of NYC
        F.col(f'{prefix}_borough').isin([
            'Manhattan',
            'Brooklyn',
            'Bronx',
            'Staten Island',
            'Queens'
        ])
    )