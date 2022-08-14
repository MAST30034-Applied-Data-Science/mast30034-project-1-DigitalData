''' Downloads necessary datasets to `DIR/data/raw`.
Created with reference to the `Python_PreReq_Notebook.ipynb` provided.

Xavier Travers
1178369
'''
import itertools
from urllib.request import urlretrieve
import os
import time

# output directory
out_dir_rel = './data/raw/'

# make required paths if nonexistant
if not os.path.exists(out_dir_rel):
    os.makedirs(out_dir_rel)

# list of urls to dl datasets from
dl_dict = { # the easy ones
    'tlc_zones/zones.csv': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv',
    'tlc_zones/zones.zip': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip',
    'tlc_zones/boroughs.geojson': 'https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON',
    'populations/2010-2019.xlsx': 'https://www2.census.gov/programs-surveys/popest/tables/2010-2019/counties/totals/co-est2019-annres-36.xlsx',
    'populations/2020-2021.xlsx': 'https://www2.census.gov/programs-surveys/popest/tables/2020-2021/counties/totals/co-est2021-pop-36.xlsx',
    'virals/covid/cases-by-day.csv': 'https://raw.githubusercontent.com/nychealth/coronavirus-data/master/trends/data-by-day.csv',
    'virals/flu/cases-by-week.csv': 'https://health.data.ny.gov/api/views/jr8b-6gh6/rows.csv?accessType=DOWNLOAD',
}

# add the tlc data over the defined timeline(s)
URL_TLC_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
TLC_NAMES = ['yellow'] # ,'fhvhv'] # this was taking too long
TLC_DATES = { # the necessary dates for analysis

    # note: the timelines defined in this code have padding. 
    # This is because the final data is grouped by mmwr weeks, 
    # which do not necessarily align within month definitions.
    # (also, inbetween times are used for time-series analysis)

    # for pre-COVID timeline: 52 weeks Mar 2018 - Feb 2019
    2018: range(2,13),
    2019: range(1,13),
    # for pre-COVID timeline: 52 weeks Mar 2018 - Feb 2019
    2020: range(1,13),
    2021: range(1,4),
}

# iterate through tlc types to download
for name in TLC_NAMES:

    # iterate through the years and specific months necessary for analysis
    for year, months in TLC_DATES.items():

        # iterate over months
        for m in months:
            # convert month to proper format 
            month = str(m).zfill(2)

            # add this file to the download dictionary
            dl_dict[
                f'tlc/{name}/{year}-{month}.parquet'
            ] = f'{URL_TLC_TEMPLATE}{name}_tripdata_{year}-{month}.parquet'

# start the time measurement
start = time.time()
start_dl = 0

# count number of downloaded files
num_downloaded = 0

print('''
DOWNLOADS STARTING...
- Files that have already been downloaded will be skipped.
- You might like to go do something else while this runs. This is a slow process (~5-10 mins).
- If you interrupt this script mid-download, I recommend deleting the last parquet file it said it was downloading.
''')

# perform download the files according to the dict
for filename, url in dl_dict.items():
    out_dir = f'{out_dir_rel}{filename}'

    # skip if it's already downloaded
    if os.path.exists(out_dir): continue

    # create the needed dir for this dl file if necessary
    if not os.path.exists(os.path.dirname(out_dir)):
        os.makedirs(os.path.dirname(out_dir))

    # start the time for this download
    start_dl = time.time()

    # download it
    print(f'DOWNLOADING \"{filename}\" FROM \"{url}\"')
    try:
        urlretrieve(url, out_dir)
        print(f'\tSUCCESS! TOOK {(time.time() - start_dl):.2f}s')
    except: # skip if the link is forbidden (example: for fhvhv in 2018)
        print(f'\tDOWNLOAD ERROR! TOOK {(time.time() - start_dl):.2f}s')
        continue

print(f'\nDONE IN {(time.time() - start):.2f}s')
