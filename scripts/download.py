''' Downloads necessary datasets to `DIR/data/raw`.
Created with reference to the `Python_PreReq_Notebook.ipynb` provided.

Xavier Travers
1178369
'''
from urllib.request import urlretrieve
import os

# for debug prints
DEBUGGING = False

# output directory
out_dir_rel = './data/raw/'

# make it if nonexistant
if not os.path.exists(out_dir_rel):
    os.makedirs(out_dir_rel)


# list of urls to dl datasets from
dl_dict = { # basic defaults
    "tlc/taxi_zones.csv": "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD",
}

# add the tlc data over the 6 months nov-2019 -> apr-2020 (when COVID hits)
URL_TLC_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"



# perform url retrieval and dl
for filename, url in dl_dict:
    out_dir = f"{out_dir_rel}{filename}"
    urlretrieve(url, out_dir)
