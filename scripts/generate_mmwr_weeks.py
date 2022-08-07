''' TODO: Commenting CDC weeks script

Xavier Travers
1178369
'''
from itertools import zip_longest
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
start_year = 2017
end_year = 2022
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
            for date in week:
                new_row = {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day,
                    'cdc_week': cdc_index,
                    'week_index': week_index,
                    'us_format': f'{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{date.year}'
                }
                cdc_weeks.append(new_row)
    
df = pd.DataFrame(cdc_weeks)
print('SAVING "./data/raw/virals/mmwr_weeks.parquet"')
df.to_parquet('./data/raw/virals/mmwr_weeks.parquet')