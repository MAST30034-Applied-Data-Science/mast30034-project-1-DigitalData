# TODO: Commenting on aggregate helpers
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import functions as F
import geopandas as gp


AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'average': ('avg_', F.avg),
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
    # return grouped_df.agg(
    #     AGGREGATION_FUNCTIONS[func_type](1)(colname).alias(AGGREGATION_FUNCTIONS[func_type](0))
    #     for col, func_types in agg_cols.items()
    #     for func_type in func_types
    # )

    print('test')

def extract_borough_name(df: DataFrame, zones_df:DataFrame,
        id_column: str, prefix: str) -> DataFrame:
    # TODO: commenting
    colnames = df.columns
    return df.join(
        zones_df.select(
            F.col('LocationID').alias(f'{prefix}_location_id'), 
            F.col('borough').alias(f'{prefix}_borough')
        ),
        on = f'{prefix}_location_id',
        how = 'inner'
    ).select(colnames + [f'{prefix}_borough'])