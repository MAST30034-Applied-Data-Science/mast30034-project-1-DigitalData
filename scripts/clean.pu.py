# %% [markdown]
# ### MAST30034: Applied Data Science Project 1
# ---
# # Preprocessing Part 1: Cleaning The Data
# #### Xavier Travers (1178369)
# 
# Cleaning the datasets of null, inconsistent, or unnecessary values.

# %%
# imports used throughout this notebook
from collections import defaultdict
import sys
from pyspark.sql import functions as F
import pandas as pd

# add homemade helpers
sys.path.insert(1, '../../scripts')
import helpers.cleaning_helpers as ch
import helpers.join_helpers as jh

# path where the data files are stored
DATA_PATH = '../../data'

# %%
from pyspark.sql import SparkSession

# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName('MAST30034 XT Project 1')
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config('spark.sql.repl.eagerEval.enabled', True) 
    .config('spark.sql.parquet.cacheMetadata', 'true')
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# %%
# import the cdc week file to convert all dates to cdc weeks now
mmwr_weeks_df = spark.read.parquet(f'{DATA_PATH}/raw/virals/mmwr_weeks.parquet')

# %%
# import the zones dataset
zones_df = spark.read.csv(f'{DATA_PATH}/raw/tlc_zones/zones.csv',
    header = True)

# %% [markdown]
# ### 1. Join population statistics with zones

# %%
# import the populations data
pop_df_2010_2019 = pd.read_excel(f'{DATA_PATH}/raw/populations/2010_2019.xlsx',
    header = [3], index_col=0).transpose()

pop_df_2010_2019.head(5)

# %%
# extract the new york city boroughs
pop_df_2010_2019 = pop_df_2010_2019[[
    '.Bronx County, New York',
    '.Kings County, New York',
    '.New York County, New York',
    '.Queens County, New York',
    '.Richmond County, New York']]

# rename the columns to their corresponding boroughs
pop_df_2010_2019.columns = [
    'Bronx',
    'Brooklyn',
    'Manhattan',
    'Queens',
    'Staten Island'
]
pop_df_2010_2019.head(5)

# %%
# import the populations data
pop_df_2020_2021 = pd.read_excel(f'{DATA_PATH}/raw/populations/2020_2021.xlsx',
    header = [3], index_col=0).transpose()

pop_df_2020_2021.head(5)

# %%
# extract the new york city boroughs
pop_df_2020_2021 = pop_df_2020_2021[[
    '.Bronx County, New York',
    '.Kings County, New York',
    '.New York County, New York',
    '.Queens County, New York',
    '.Richmond County, New York']]

# rename the columns to their corresponding boroughs
pop_df_2020_2021.columns = [
    'Bronx',
    'Brooklyn',
    'Manhattan',
    'Queens',
    'Staten Island'
]
pop_df_2020_2021.head(5)

# %%
# stack the population data
pop_df = pd.concat(
    [pop_df_2010_2019, pop_df_2020_2021]
)

# only keep 2018-2021 data
pop_df = pop_df.filter(items = [2018, 2019, 2020, 2021], axis = 0)
pop_df.head()

# %%
# rename the index column
pop_df.index.name = 'week_year'
pop_df = pop_df.reset_index()
pop_df.head()

# %%
# verticalize the data (I just want a year column and a borough column)
temp_df = None
for col in pop_df.columns[1:]:
    # add the borough to an extracted borough column
    # using .copy() hides a space-hog warning about not editing pop_df 
    # (which is my intention)
    b_pop_df = pop_df[['week_year', col]].copy()
    b_pop_df.columns = ['week_year', 'population']
    b_pop_df['borough'] = col

    if temp_df is None:
        temp_df = b_pop_df 
    else:
        temp_df = pd.concat([temp_df, b_pop_df])

# set the verticalized data to the population data
pop_df = spark.createDataFrame(temp_df)

# %%
# save the population df
pop_df.write.mode('overwrite').parquet(f'{DATA_PATH}/curated/population_by_borough_by_year')

# %% [markdown]
# ### 2. Cleaning the TLC dataset(s)

# %%
# there are large trip distances that I need to filter out
example_df.sort('trip_distance', ascending = False).limit(5)

# %%
# read in the tlc data
tlc_df = jh.read_stacked_tlc_df(spark)

# %%
# derive extra values which are used to filter out valid trips
SECONDS_TO_HOURS = 1 / (60*60)
tlc_df = tlc_df\
    .withColumn('hours_elapsed', 
        ( # counts how long the trip was in hours
            (F.col("tpep_dropoff_datetime").cast("long")
            - F.col('tpep_pickup_datetime').cast("long")) 
            * SECONDS_TO_HOURS
        )
    )\
    .withColumn('mph', 
        ( # calculate the speed of the trip
            F.col('trip_distance') / F.col('hours_elapsed')
        )
    )

# %%
# https://ypdcrime.com/vt/article30.php?zoom_highlight=fifty+five+miles+per+hour#t1180-a
# As per: https://www.dot.ny.gov/divisions/operating/oom/transportation-systems/repository/TSMI-17-05.pdf
# the NYS maximum speed limit is 65 mph. filter out trips faster than legal.
tlc_df = tlc_df.where(
    (F.col('mph').isNotNull()) &
    (F.col('mph') <= 65)
)

# this legitimately removes any invalid trips 
# (and other null values are culled later)

# %%
# WARNING: this one is time intensive 
# next, filter out trips which do not start  within the 5 boroughs 
tlc_df = ch.extract_borough_name(
    tlc_df.withColumnRenamed('PULocationID', 'pu_location_id'), zones_df,  'pu')

# %%
# names of the tlc datasets to clean 
# (I was originally planning on working on fhvhv and green as well)
TLC_NAMES = ['yellow']

# dictionary to rename all the columns I want to keep
TLC_KEEP_COLUMNS = {
    'tpep_pickup_datetime': 'date',
    # 'passenger_count': 'passengers',
    'trip_distance': 'trip_distance',
    'pu_borough': 'pu_borough',
    # 'DOLocationID': 'do_location_id',
    'hours_elapsed': 'hours_elapsed'
    # #  below only apply to fhvhv
    # 'hvfhs_license_num': 'fhvhv_license',
    # 'pickup_datetime': 'date',
    # 'trip_miles': 'trip_distance',
    # 'shared_request_flag': 'shared'
}

# create a dictionary of the columns to keep and the required filters
TLC_CLEAN_COLUMNS = {
    'pu_location_id': [ch.non_null], 
    # 'do_location_id': [ch.non_null], 
    # 'passengers': [ch.non_null], 
    'trip_distance': [ch.non_null, ch.non_negative], 
    # #  below only apply to fhvhv
    # 'fhvhv_license': [ch.non_null], 
}

# %%
# perform the drawn out cleaning process (function in `scripts/helpers`)
tlc_df = ch.perform_cleaning(tlc_df, mmwr_weeks_df, TLC_KEEP_COLUMNS, 
    TLC_CLEAN_COLUMNS)

# %%
# WARNING: this one is time intensive 
# save the stacked df by month (this will take a while)
# sorting first prevents the partitioning part of the write from crashing
# (form personal experience)
tlc_df = tlc_df.sort('week_year', 'week_month')
tlc_df.write\
    .partitionBy('week_year', 'week_month')\
    .mode('overwrite')\
    .parquet(f'{DATA_PATH}/curated/tlc/cleaned/yellow')

# %% [markdown]
# ### 3. Cleaning the COVID dataset

# %%
# read in the covid dataset
covid_df = spark.read.csv(f'{DATA_PATH}/raw/virals/covid/cases_by_day.csv',
    header = True, inferSchema=True)
covid_df.limit(5)

# %%
# sum the number of incomplete datasets (ensure no incomplete values)
sum(covid_df.select('INCOMPLETE'))

# %%
# dictionary to rename all the columns I want to keep
COVID_KEEP_COLUMNS = {
    'date_of_interest':'date'
}

# create a dictionary of the columns to keep and the required filters
COVID_CLEAN_COLUMNS = defaultdict(lambda: ch.non_negative)

# define the boroughs as they appear in columns of the covid dataset
COVID_BOROUGHS = {
    'BX_':'Bronx',
    'BK_':'Brooklyn',
    'MN_':'Manhattan',
    'QN_':'Queens',
    'SI_':'Staten Island',
}

# define the count values to extract
COVID_COUNTS = {
    'CASE_COUNT': 'cases', 
    # 'DEATH_COUNT': 'deaths', 
    # 'HOSPITALIZED_COUNT': 'hospitalised'
}

# got throuch each borough and count value and add them to the columns to keep
for prefix, new_prefix in COVID_BOROUGHS.items():
    for suffix, new_suffix in COVID_COUNTS.items():
        COVID_KEEP_COLUMNS[f'{prefix}{suffix}'] = f'{new_prefix}{new_suffix}'

# %%
# perform the drawn out cleaning process (function in `scripts/helpers`)
covid_df = ch.perform_cleaning(covid_df, mmwr_weeks_df, COVID_KEEP_COLUMNS, 
    COVID_CLEAN_COLUMNS)

# %%
# date columns to keep after verticalising the covid data
COVID_DATE_COLUMNS = [
    F.col('date'), 
    F.col('week_ending'), 
    F.col('week_year'), 
    F.col('week_month'), 
    F.col('week_index')
]

# verticalise this dataset
# I'd rather just have a 'borough' column for homogeneity of all the data
temp_df = None
for prefix in COVID_BOROUGHS.values():
    # derive the columns for this borough to extract and stack
    borough_columns = []
    for suffix in COVID_COUNTS.values():
        borough_columns.append(F.col(f'{prefix}{suffix}').alias(suffix))

    # extract the counts for this borough and add them to the stacked dataframe
    if temp_df == None:
        temp_df = covid_df.select(COVID_DATE_COLUMNS + borough_columns)\
            .withColumn('borough', F.lit(prefix))
    else:
        temp_df = temp_df\
            .union(
                covid_df.select(COVID_DATE_COLUMNS + borough_columns)\
                    .withColumn('borough', F.lit(prefix))
            )

# set the df to the stacked data
covid_df = temp_df

# %%
# fill the null cases (created from the one-sided outer join for the df) with 0
covid_df = covid_df.fillna(0, 'cases')

# %%
# save the cleaned covid data
covid_df.write.mode('overwrite').parquet(f'{DATA_PATH}/curated/virals/covid/cleaned/cases_by_day')

# %% [markdown]
# ### 4. Cleaning the flu dataset

# %%
# read in the flu dataset
flu_df = spark.read.csv(f'{DATA_PATH}/raw/virals/flu/cases_by_week.csv',
    header=True, inferSchema=True)

# %%
# dictionary to rename all the columns I want to keep
FLU_KEEP_COLUMNS = {
    'Week Ending Date': 'date',
    'Region': 'region',
    'County': 'borough',
    'Disease': 'disease',
    'Count': 'cases',
}

# create a dictionary of the columns to keep and the required filters
FLU_CLEAN_COLUMNS = {
    'date': [],
    'region': [lambda _: F.col('region') == 'NYC'],
    'borough': [],
    'disease': [],
    'cases': [ch.non_negative]
}

# %%
# perform the drawn out cleaning process (function in `scripts/helpers`)
flu_df = ch.perform_cleaning(flu_df, mmwr_weeks_df, FLU_KEEP_COLUMNS, 
    FLU_CLEAN_COLUMNS)

# %%
# map the boroughs to their proper names
# from: https://portal.311.nyc.gov/article/?kanumber=KA-02877
# also from map dict
FLU_COUNTY_TO_BOROUGH = {
    'BRONX': 'Bronx',
    'KINGS': 'Brooklyn',
    'NEW YORK': 'Manhattan',
    'QUEENS': 'Queens',
    'RICHMOND': 'Staten Island'
}

# %%
# apply the mapping to the flu df
flu_df = ch.replace_column_using_dict(flu_df, 'borough', FLU_COUNTY_TO_BOROUGH)

# also remove the regions column (not needed anymore)
columns_without_regions = flu_df.columns[:]
columns_without_regions.remove('region')
flu_df = flu_df.select(columns_without_regions)

# %%
# fill the null cases (created from the one-sided outer join for the df) with 0
flu_df = flu_df.fillna(0, 'cases')
# %%
# save the cleaned flu data
flu_df.write.mode('overwrite').parquet(f'{DATA_PATH}/curated/virals/flu/cleaned/cases_by_week')


