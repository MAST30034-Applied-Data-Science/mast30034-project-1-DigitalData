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
    'tlc_zones/zones.csv': 'https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD',
    'tlc_zones/boroughs.geojson': 'https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON',
    'covid/cases-by-day.csv': 'https://raw.githubusercontent.com/nychealth/coronavirus-data/master/trends/data-by-day.csv'
}

# add the tlc data over the 6 months nov-2019 -> apr-2020 (when COVID hits)
URL_TLC_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
TLC_NAMES = ['yellow', 'green', 'fhvhv']
TLC_DATES = { # the necessary dates for analysis
    2019: range(6,13),
    2020: range(1,13),
    2021: range(1,7),
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
