from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import os

def join_by_week_by_borough(tlc_df: DataFrame, viral_df: DataFrame, 
        virus_name: str) -> DataFrame:
    # I will always be joining the tlc data with 
    # the viral data from the previous week 
    # (since the previous week assumedly dictates next week's choices)

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
            # F.col('borough').alias(f'{virus_name}_borough'),
            # F.col('week_index').alias(f'{virus_name}_prev_week_index'),
            # F.col('tot_cases').alias(f'{virus_name}_tot_cases'), 
            # F.col('daily_avg_cases').alias(f'{virus_name}_daily_avg_cases')
        ),
        on = [
            (F.col(borough_col) == F.col(f'{virus_name}_borough')),
            (F.col('week_index') == (F.col(f'{virus_name}_week_index') + 1))
        ],
        how = 'leftouter'
    )


def read_stacked_tlc_df(spark:SparkSession, 
        location:str = '../data/raw/tlc/yellow') -> DataFrame:

    stacked_df = None

    # iterate through the downloaded files per taxi type
    for filename in os.listdir(location):

        # read the parquet in
        tlc_df = spark.read.parquet(f'{location}/{filename}')

        if stacked_df == None:
            stacked_df = tlc_df
        else:
            stacked_df = stacked_df.union(tlc_df)

    return stacked_df