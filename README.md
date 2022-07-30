# MAST30034 Project 1 README.md
- Name: Xavier Travers
- Student ID: 1178369

## Student Instructions
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
This is an example `README.md` for students to use. **Please change this to your requirements**.

**Research Goal:** To determine the effect that the COVID pandemic has had on the frequency of use of taxi services in New York.

**Timeline:** The timeline for the research area is starting July 2019 and ending June 2021.

**Pipeline:**
*Run all the scripts from the repository's root directory (do not `cd` into the `scripts` folder).*

1. `download.py`: This downloads the raw data into the `data/raw` directory. 
Run ```python3 ./scripts/download.py```.
2. `preprocessing_part_1_conversion.ipynb`: Converts any `.csv` datasets into the `.parquet` format.
3. `preprocessing_part_2_cleaning.ipynb`: Cleans the dataset (removes rows containing `null` and negative values where necessary).
4. `preprocessing_part_3_aggregation.ipynb`: This groups the datasets by month, year, taxi type, shared-ride status (which is a passenger count for green/yellow taxis and a boolean value for high frequency taxi services).
5. ``
6. ``

<!-- 2. `preprocess.ipynb`: This notebook details all preprocessing steps and outputs it to the `data/curated` directory.
3. `analysis.ipynb`: This notebook is used to conduct analysis on the curated data.
4. `model.py` and `model_analysis.ipynb`: The script is used to run the model from CLI and the notebook is used for analysing and discussing the model. -->
