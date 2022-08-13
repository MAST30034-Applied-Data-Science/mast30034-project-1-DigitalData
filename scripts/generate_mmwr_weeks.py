''' TODO: Commenting CDC weeks script

Xavier Travers
1178369
'''
from itertools import zip_longest
import os
from statistics import mode
import pandas as pd
import calendar as cd

def date_equal(d1, d2):
    return (
        d1.year == d2.year 
        and d1.month == d2.month
        and d1.day == d2.day
    )

def all_dates_equal(dl1, dl2):
    if(len(dl1) != len(dl2)): return False

    for d1,d2 in zip_longest(dl1, dl2):
        if not date_equal(d1, d2):
            return False
    return True

def not_in_right_year(w, y):
    count = 0
    for d in w:
        if d.year == y:
            count += 1
    
    if count < 4: return True
    return False

cdc_weeks = []
start_year = 2018
end_year = 2021
cal = cd.Calendar(6)
week_index = 0
last_week = []
for year in range(start_year, end_year + 1):
    cdc_index = 0
    for monthRow in cal.yeardatescalendar(year, width=1):
        month = monthRow[0]
        for week in month:
            if all_dates_equal(last_week, week):
                continue
            if not_in_right_year(week, year):
                continue

            last_week = list(week)
            cdc_index += 1
            week_index += 1
            week_months = []
            week_years = []

            for date in week:
                week_months.append(date.month)
                week_years.append(date.year)

            for date in week:
                new_row = {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day,
                    'cdc_week': cdc_index,
                    'week_index': week_index,
                    'us_format': f'{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{date.year}',
                    'week_ending': f'{week[-1].year}-{str(week[-1].month).zfill(2)}-{str(week[-1].day).zfill(2)}',
                    'week_month': mode(week_months),
                    'week_year': mode(week_years)
                }
                cdc_weeks.append(new_row)
    
df = pd.DataFrame(cdc_weeks)

# define the two timelines of analysis ('pre' vs 'post')
df['timeline'] = 'neither'

def set_timeline(t_year: int, t_month: int, timeline: str):
    for i in range(1, 13):
        df.loc[
            lambda sdf: (sdf['week_year'] == t_year) 
                        & (sdf['week_month'] == t_month), 
            ['timeline']
        ] = timeline

        t_month += 1
        if t_month > 12:
            t_year += 1
            t_month = 1

set_timeline(2018, 3, 'pre')
set_timeline(2019, 3, 'keep for graphing')
set_timeline(2020, 3, 'post')

print(df.head(100))

# define the path
def mmwr_path(folder):
    return f'./data/{folder}/virals/'

for folder in ['raw', 'curated']:

    # create the needed dir for this dl file if necessary
    if not os.path.exists(os.path.dirname(mmwr_path(folder))):
        os.makedirs(os.path.dirname(mmwr_path(folder)))

    print(f'SAVING "{mmwr_path(folder)}mmwr_weeks.parquet"')
    df.to_parquet(f'{mmwr_path(folder)}mmwr_weeks.parquet')