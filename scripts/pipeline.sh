# You can run the full pipeline easily here.
# Xavier Travers (1178369)
echo "MAKE SURE YOU'RE RUNNING THIS SCRIPT FROM THE REPOSITORY'S ROOT DIRECTORY"
echo "IF THIS SCRIPT FAILS, RUN THE PYTHON SCRIPTS ONE-BY-ONE"
echo

# Run the download script
echo
echo "Running scripts/downloads.py"
python3 ./scripts/download.py

# Run the generating MMWR weeks script
echo
echo "Running scripts/generate_mmwr_weeks.py"
python3 ./scripts/generate_mmwr_weeks.py

# Run the python cleaning script
echo
echo "Running scripts/clean.py"
python3 ./scripts/clean.py

# Run the python aggregation script
echo
echo "Running scripts/aggregate.py"
python3 ./scripts/aggregate.py