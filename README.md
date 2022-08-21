# MAST30034 Project 1 `README.md`
- Name: Xavier Travers
- Student ID: 1178369

<!-- ## Student Instructions
You **must** write up `README.md` for this repository to be eligable for readability marks.

1. Students must keep all Jupyter Notebooks in the `notebooks` directory.
2. Students must keep all `.py` scripts under the `scripts` directory. These can include helper functions and modules with relevant `__init__.py`
3. Students must store all raw data downloaded (using a Python script) in the `data/raw` folder. This will be in the `.gitignore` so **do not upload any raw data files whatsoever**.
4. Students must store all curated / transformed data in the `data/curated` folder. This will be in the `.gitignore` so **do not upload any raw data files whatsoever**. We will be running your code from the `scripts` directory to regenerated these.
5. All plots must be saved in the `plots` directory.
6. Finally, your report `.tex` files must be inside the `report` directory. If you are using overleaf, you can download the `.zip` and extract it into this folder.
7. Add your name and Student ID to the fields above.
8. Add your relevant `requirements.txt` to the root directory. If you are unsure, run `pip3 list --format=freeze > requirements.txt` (or alternative) and copy the output to the repository.
9. You may delete all `.gitkeep` files if you really want to. These were used to ensure empty directories could be pushed to `git`.
10. When you have read this, delete the `Student Instructions` section to clean the readme up.

Remember, we will be reading through and running your code, so it is in _your best interest_ to ensure it is readable and efficient.

## README example
This is an example `README.md` for students to use. **Please change this to your requirements**. -->

****
## Research Goal 
To determine the effects that the virus case rates (COVID-19 and Influenza) have on the frequency and distance of trips among different taxi services in New York.

****
## Timeline
The timeline for the research area is starting January 2020 and ending December 2021 (See [the report](https://github.com/MAST30034-Applied-Data-Science/mast30034-project-1-DigitalData/blob/main/report/main.pdf) for justification).

****
## Pipeline

<!-- ### - The Intended Way
I would love it if this works on your end.
I unfortunately cannot guarrantee it will.
This is quite new and I haven't had that much time to test it.
*Run this bash script from the repository's root directory (__do not__ `cd` into the `scripts` folder).*
```
./pipeline.sh
```
This script should run the whole data pipeline start to finish.
This script should generate all the plots necessary, except for the `.png` files for the maps.
Map images in the report are screenshots of the maps that this script generates.

### - Alternative: The Long Way -->
*Run all the scripts from the repository's root directory (__do not__ `cd` into the `scripts` folder).*

1. `download.py`: Downloads the raw data into the `data/raw` directory. Run with

```
python3 ./scripts/download.py
```
2. `generate_mmwr_weeks.py`: Generates a `data/raw/mmwr_weeks.parquet` which is used for aggregation by week (where the Influenza data is already grouped by CDC/MMWR week). Run with

```
python3 ./scripts/generate_mmwr_weeks.py
```
3. `notebooks/preprocessing/preprocessing_part_1_cleaning.ipynb`: Cleans the dataset (removes rows containing `null` and negative values where necessary).
4. `notebooks/preprocessing/preprocessing_part_2_aggregation.ipynb`: Groups the datasets by MMWR week and pick-up borough.
5. The data analysis notebooks: These can be explored in any order (since they do not change data, only generating plots).
    - `notebooks/data_analysis/data_analysis_distance_distribution.ipynb`: Related to finding the distribution of trip distances.
    - `notebooks/data_analysis/data_analysis_distance_vs_time.ipynb`: Plots the trip distances over time.
    - `notebooks/data_analysis/data_analysis_geospatial_distance_mapping.ipynb`: Maps the average trip radii per borough.
    - `notebooks/data_analysis/data_analysis_viral_cases_vs_time.ipynb`: Plots the viral case rates over time.
    - `notebooks/data_analysis/data_analysis_distance_modelling.ipynb`: Generates the linear models of trip distances.
    - `notebooks/data_analysis/data_analysis_trip_rates_vs_time.ipynb`: Plots the trip rates over time. *This is not used in the report.*

****
## Python Scripts
There are several scripts located in the `scripts` folder.
These have enough commenting to not need a breakdown of each here.

****
## Main Python Modules
These are used throughout the code and should be installed before running.
For a more detailed snapshot of the modules I have installed when running my code,
see the `requirements.txt`.
- `pyspark`
- `pandas`
- `matplotlib`
- `statsmodels`
- `geopandas`
- `folium`
- `numpy`

****

<!-- 2. `preprocess.ipynb`: This notebook details all preprocessing steps and outputs it to the `data/curated` directory.
3. `analysis.ipynb`: This notebook is used to conduct analysis on the curated data.
4. `model.py` and `model_analysis.ipynb`: The script is used to run the model from CLI and the notebook is used for analysing and discussing the model. -->
