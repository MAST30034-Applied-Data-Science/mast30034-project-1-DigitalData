from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import os

def join_by_week_by_borough(tlc_df: DataFrame, viral_df: DataFrame, 
        case_col: str) -> DataFrame:
    # I will always be joining the tlc data with 
    # the viral data from the previous week 
    # (since the previous week generally dictates next week's choices)

    # TODO: commenting on join
    out_df = tlc_df
    out_df = out_df.join(
        viral_df.select(
            F.col(case_col).alias(f'pu_{case_col}'),
            F.col('borough').alias(f'pu_borough'),
            (F.col('week_index')).alias('prev_week_index')
        ),
        on = 'pu_borough'
    ).where(F.col('week_index') == (F.col('prev_week_index') + 1))

    out_df = out_df.join(
        viral_df.select(
            F.col(case_col).alias(f'do_{case_col}'),
            F.col('borough').alias(f'do_borough'),
            (F.col('week_index')).alias('prev_week_index')
        ),
        on = ['do_borough', 'prev_week_index']
    )

    return out_df

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