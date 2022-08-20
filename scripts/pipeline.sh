# You can run the full pipeline easily here.
# Xavier Travers (1178369)
echo "MAKE SURE YOU'RE RUNNING THIS SCRIPT FROM THE REPOSITORY'S ROOT DIRECTORY"
echo "IF THIS SCRIPT FAILS, RUN THE PYTHON SCRIPTS ONE-BY-ONE"
echo

# Run the download script
echo
echo "Running scripts/downloads.py"
python3 ./scripts/download.py

# Run the generateing MMWR weeks script
echo
echo "Running scripts/generate_mmwr_weeks.py"
python3 ./scripts/generate_mmwr_weeks.py