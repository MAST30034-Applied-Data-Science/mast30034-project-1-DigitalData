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
out_dir_rel = '../data/raw/'

# make it if nonexistant
if not os.path.exists(out_dir_rel):
    os.makedirs(out_dir_rel)

