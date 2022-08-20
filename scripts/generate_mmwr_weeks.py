''' Generates a parquet file containing the defined timeline(s) and calculates
the MMWR weeks with padding 
(in case some MMWR weeks overlap multiple months/years).

Xavier Travers
1178369
'''
from itertools import zip_longest
import os
from statistics import mode
import pandas as pd
import calendar as cd

def date_equal(d1, d2) -> bool:
    """ Determines whether two dates are the same.
    This goes as granular as date (no hours, mins or more accurate).
    """
    return (
        d1.year == d2.year 
        and d1.month == d2.month
        and d1.day == d2.day
    )

def all_dates_equal(dl1, dl2) -> bool:
    """ Checks whether two lists of dates are equal (as granular sa date).
    """
    if(len(dl1) != len(dl2)): return False

    # iterate through the dates in the lists and compare
    for d1,d2 in zip_longest(dl1, dl2):
        if not date_equal(d1, d2):
            return False
    return True

def in_right_year(w: list, y:int) -> bool:
    """ Check whether the majority of a given week is within the given year.

    Args:
        w (list): The week as a list of dates
        y (int): The year to check for
    """

    # count the # of dates in the year
    count = 0
    for d in w:
        if d.year == y:
            count += 1
    
    if count >= 4: return True
    return False

# init the list of mmwr week dictionaries
mmwr_weeks = []

# year of start of visualization window
start_year = 2018
end_year = 2021

# this is done using a python calendar
cal = cd.Calendar(6)

# other helpful variables like relative week index and tracking the last week
week_index = 0
last_week = []

# iterate through the years
for year in range(start_year, end_year + 1):
    year_mmwr_index = 0

    # iterate through the months in the year
    for monthRow in cal.yeardatescalendar(year, width=1):
        month = monthRow[0]

        # iterate through the weeks in each month
        for week in month:

            # skip this week if it has already been added
            if all_dates_equal(last_week, week):
                continue

            # skip this week if its majority is not in this year
            if not in_right_year(week, year):
                continue
            
            # generate the lists and 
            last_week = list(week)
            week_months = []
            week_years = []
            year_mmwr_index += 1
            week_index += 1

            # preemptively extract months and years 
            # (used to assign modal month and year)
            for date in week:
                week_months.append(date.month)
                week_years.append(date.year)

            # add for each day in the week a dictionary row
            # containing potentially necessary information.
            for date in week:
                new_row = {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day,
                    'month_mmwr_index': year_mmwr_index,
                    'week_index': week_index,
                    'us_format': f'{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{date.year}',
                    'week_ending': f'{week[-1].year}-{str(week[-1].month).zfill(2)}-{str(week[-1].day).zfill(2)}',
                    'week_month': mode(week_months),
                    'week_year': mode(week_years)
                }

                mmwr_weeks.append(new_row)
    
# generate a dataframe from the mmwr list of dictionaries
df = pd.DataFrame(mmwr_weeks)

# define the two timelines of analysis ('pre' vs 'post')
df['timeline'] = 0

def set_timeline(t_year: int, t_month: int, timeline: int, duration: int = 12):
    """ Sets a specific timeline given a starting month and month length.

    Args:
        t_year (int): initial year
        t_month (int): initial month
        timeline (int): the index of the timeline
        duration (int, optional): how long it is in months. Defaults to 12.
    """
    for i in range(1, duration + 1):
        df.loc[
            lambda sdf: (sdf['week_year'] == t_year) 
                        & (sdf['week_month'] == t_month), 
            ['timeline']
        ] = timeline

        t_month += 1
        if t_month > 12:
            t_year += 1
            t_month = 1

# set the selected timelines (used for filtering later)
set_timeline(2018, 7, 2, 24)
set_timeline(2020, 7, 1)

# some debug display for feedback
# also shows that the script worked
print('GENERATED MMWR WEEKS TABLE')
print('PRINTING EVERY 50TH ROWS')
print(df.iloc[::50, :].head(df.size))

# define the path
def mmwr_path(folder: str):
    """ Fills out the path needed to save the mmwr weeks
    """
    return f'./data/{folder}/virals'

# if I wanted to add the the data to the curated folder as well, I'd do it here. 
# (I did this originally, so I'll just keep the option open)
for folder in ['raw']:

    # create the needed dir for this dl file if necessary
    if not os.path.exists(os.path.dirname(mmwr_path(folder))):
        os.makedirs(os.path.dirname(mmwr_path(folder)))

    # save them
    print(f'SAVING "{mmwr_path(folder)}/mmwr_weeks.parquet"')
    df.to_parquet(f'{mmwr_path(folder)}/mmwr_weeks.parquet')