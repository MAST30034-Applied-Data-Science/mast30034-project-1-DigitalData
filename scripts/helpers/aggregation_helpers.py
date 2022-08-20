''' Provides a function for aggregating and grouping datasets.

Xavier Travers
1178369
'''
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The list of any aggregation functions that I could possible need
AGGREGATION_FUNCTIONS = {
    'total': ('tot_', F.sum),
    'total_per_capita': ('tot_pc_', 
        lambda colname: F.sum(colname) / F.col('population')),
    'total_per_100k': ('tot_p100k_', 
        lambda colname: 100000 * F.sum(colname) / F.col('population')),
    'average': ('avg_', F.avg),
    'daily_average': ('daily_avg_', 
        lambda colname: F.sum(colname) / 7),
    'daily_average_per_capita': ('avg_pc_', 
        lambda colname: F.sum(colname) / (7 * F.col('population'))),
    'daily_average_per_100k': ('avg_p100k_', 
        lambda colname: 100000 * F.sum(colname) / (7 * F.col('population'))),
    'count': ('num_', F.count),
    'count_per_capita': ('num_pc_', 
        lambda colname: F.count(colname) / F.col('population')),
    'count_per_100k': ('num_p100k_', 
        lambda colname: 100000 * F.count(colname) / F.col('population')),
}

def group_and_aggregate(df: DataFrame, pop_df: DataFrame, 
        group_cols: "list[str]", agg_cols: dict) -> DataFrame:
    """ Group the data and aggregate it using the chosen functions

    Args:
        df (`DataFrame`): The dataset to aggregate. 
        pop_df (`DataFrame`): The dataframe of borough populations per year.
        group_cols (list[str]): The columns to group the dataset by.
        agg_cols (dict): The aggregation functions used.

    Returns:
        `DataFrame`: The aggregated dataframe
    """

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in group_cols:
        borough_col = 'pu_borough'
    elif 'do_borough' in group_cols:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    # add the population data
    df = df.join(
        pop_df.select(
            F.col('week_year'),
            F.col('borough').alias(borough_col),
            F.col('population')
        ),
        on = ['week_year', borough_col],
    )

    # group using the defined columns and population
    grouped_df = df.groupBy(group_cols + ['population'])
    
    # define the list of aggregated columns to add
    column_aggregates = []

    # iterate through the defined aggregations,
    # perform the aggregation and name the columsn accordingly
    for colname, func_types in agg_cols.items():
        for func_type in func_types:
            prefix, func = AGGREGATION_FUNCTIONS[func_type]

            new_colname = colname
            if '*' == colname:
                new_colname = 'trips'

            column_aggregates.append(
                func(colname).alias(f'{prefix}{new_colname}')
            )

    # return the grouped data with aggregations
    return grouped_df.agg(*column_aggregates)
