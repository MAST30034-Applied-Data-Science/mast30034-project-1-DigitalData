''' Provides functions for joining datasets vertically or horizontally.

Xavier Travers
1178369
'''
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import os

def join_by_week_by_borough(tlc_df: DataFrame, viral_df: DataFrame, 
        virus_name: str) -> DataFrame:
    """ Join a TLC dataset with a viral dataset by borough and week.
    I will always be joining the tlc data with 
    the viral data from the previous week 
    (since the previous week assumedly dictates next week's choices).

    Args:
        tlc_df (`DataFrame`): The TLC dataset to join
        viral_df (`DataFrame`): The viral dataset to join
        virus_name (str): The name of the virus (used for prefixes)

    Returns:
        `DataFrame`: The joined dataset
    """

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in tlc_df.columns:
        borough_col = 'pu_borough'
    elif 'do_borough' in tlc_df.columns:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    # join the viral and tlc dataset
    return tlc_df.join(
        viral_df.select(
            *[
                F.col(colname).alias(f'{virus_name}_{colname}')
                for colname in viral_df.columns
            ],
        ),
        on = [
            (F.col(borough_col) == F.col(f'{virus_name}_borough')),
            (F.col('week_index') == (F.col(f'{virus_name}_week_index') + 1))
        ], # I want values rows without viral data to be null/zero valued.
        how = 'leftouter'
    ).fillna(0) # also impute zero values where viral data is null/not there
    # this specifically applies for the flu dataset, which is seasonal.


def read_stacked_tlc_df(spark:SparkSession, 
        location:str = '../../data/raw/tlc/yellow') -> DataFrame:
    """ Read and stack the months of a TLC dataset.

    Args:
        spark (`SparkSession`): Used to read the datasets.
        location (str, optional): The location/type of the dataset. 
            Defaults to '../../data/raw/tlc/yellow'.

    Returns:
        `DataFrame`: The stacked dataset
    """

    stacked_df = None

    # iterate through the downloaded files per taxi type
    for filename in os.listdir(location):

        # read the parquet in
        tlc_df = spark.read.parquet(f'{location}/{filename}')

        # stack the data using union
        if stacked_df == None:
            stacked_df = tlc_df
        else:
            stacked_df = stacked_df.union(tlc_df)

    return stacked_df