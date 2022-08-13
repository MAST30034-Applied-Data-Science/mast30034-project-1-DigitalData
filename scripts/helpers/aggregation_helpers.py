# TODO: Commenting on aggregate helpers
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import functions as F
import geopandas as gp


AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'average': ('avg_', F.avg),
    'daily_average': ('daily_avg_', lambda colname: F.sum(colname) / 7),
    'count': ('num_', F.count)
}
# TODO: commenting

def group_and_aggregate(df: DataFrame, group_cols: "list[str]", agg_cols: dict) -> DataFrame:
    grouped_df = df.groupBy(group_cols)
    # TODO: commenting
    column_aggregates = []

    for colname, func_types in agg_cols.items():
        for func_type in func_types:
            prefix, func = AGGREGATION_FUNCTIONS[func_type]
            column_aggregates.append(
                func(colname).alias(f'{prefix}{colname}')
            )

    return grouped_df.agg(*column_aggregates)
