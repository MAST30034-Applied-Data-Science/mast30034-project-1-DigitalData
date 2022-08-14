# TODO: Commenting on aggregate helpers
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import functions as F
import geopandas as gp


AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'total_per_capita': ('tot_pc_', lambda colname: F.sum(colname) / F.col('population')),
    'total_per_100k': ('tot_p100k_', lambda colname: 100000 * F.sum(colname) / F.col('population')),
    'average': ('avg_', F.avg),
    'daily_average': ('daily_avg_', lambda colname: F.sum(colname) / 7),
    'daily_average_per_capita': ('avg_pc_', lambda colname: F.sum(colname) / (7 * F.col('population'))),
    'daily_average_per_100k': ('avg_p100k_', lambda colname: 100000 * F.sum(colname) / (7 * F.col('population'))),
    'count': ('num_', F.count),
    'count_per_capita': ('num_pc_', lambda colname: F.count(colname) / F.col('population')),
    'count_per_100k': ('num_p100k_', lambda colname: 100000 * F.count(colname) / F.col('population')),
}
# TODO: commenting

def group_and_aggregate(df: DataFrame, pop_df: DataFrame, 
        group_cols: "list[str]", agg_cols: dict) -> DataFrame:

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in group_cols:
        borough_col = 'pu_borough'
    elif 'do_borough' in group_cols:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    df = df.join(
        pop_df.select(
            F.col('week_year'),
            F.col('borough').alias(borough_col),
            F.col('population')
        ),
        on = ['week_year', borough_col],
    )

    grouped_df = df.groupBy(group_cols + ['population'])
    # TODO: commenting
    column_aggregates = []

    for colname, func_types in agg_cols.items():
        for func_type in func_types:
            prefix, func = AGGREGATION_FUNCTIONS[func_type]
            column_aggregates.append(
                func(colname).alias(f'{prefix}{colname}')
            )

    return grouped_df.agg(*column_aggregates)
