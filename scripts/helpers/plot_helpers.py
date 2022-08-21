''' Provides functions for plotting data in different formats.

Xavier Travers
1178369
'''
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import geopandas as gpd
import folium
import numpy as np
import statsmodels.api as sm
from statsmodels.base.wrapper import ResultsWrapper
from statsmodels.graphics.api import abline_plot

# preset the plot colours for the boroughs
BOROUGH_COLOURS = {
    'Bronx': 'red',
    'Brooklyn': 'cyan',
    'Manhattan': 'green',
    'Queens': 'orange',
    'Staten Island': 'blue'
}

def histogram(df: pd.DataFrame, x: str, xlabel: str = '', bins: int = 20,
        logx: bool = False):
    """ Plot a histogram to show the general distribution.

    Args:
        df (`pd.DataFrame`): The dataset to use.
        x (str): The column to plot the distribution for.
        xlabel (str, optional): The label to use for the x axis. Defaults to ''.
        bins (int, optional): The number of bins. Defaults to 20.
        logx (bool, optional): Whether to plot on a logarithmic scale. 
            Defaults to False.
    """

    # create the subplot for the histogram
    fig, ax = plt.subplots()

    # apply logx if need
    if logx:
        df[x] = np.log(df[x])

    # plot the histogram on the selected x column
    df[x].hist(ax=ax, bins = bins, density = True)

    # set the x label of the histogram
    if len(xlabel) != 0: 
        ax.set_xlabel(xlabel)
    if logx:
        ax.set_xlabel(f'{ax.get_xlabel()} (Log Scale)')

    # set the y axis label
    ax.set_ylabel('Density')

    # set the histogram's title correctly
    ax.set_title(f'Histogram ({bins} Bins)')

    # show and save the plot
    plt.savefig(
        f'../../plots/histogram-{ax.get_xlabel()}-{bins}-bins.png',
        bbox_inches='tight',
        dpi=300
    )
    plt.show()

def time_series(df: pd.DataFrame, y: str, ylabel: str = '', logy:bool = False):
    """ Plot a time-series line plot to show changes over time.

    Args:
        df (`pd.DataFrame`): The dataset.
        y (str): The value to plot vs time.
        ylabel (str, optional): The label to set for the y axis. Defaults to ''.
        logy (bool, optional): Whether to plot on a logarithmic scale. 
            Defaults to False.
    """

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in df.columns:
        borough_col = 'pu_borough'
    elif 'do_borough' in df.columns:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    # extract the dates
    df['week_ending_date'] = pd.to_datetime(df['week_ending'])

    # create the time-series by group
    fig, ax = plt.subplots()
    grouped_df = df.sort_values('week_ending_date').groupby(borough_col)
    for key, group in grouped_df:

        # apply date formatting
        # from: https://stackoverflow.com/questions/14946371/editing-the-date-formatting-of-x-axis-tick-labels-in-matplotlib
        ax.xaxis.set_major_locator(mdates.YearLocator(base = 1, month = 1, day = 1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        group.plot(ax=ax, x='week_ending_date', y=y, kind='line', label=key,
            color=BOROUGH_COLOURS[key], logy=logy)

    # create a legend outside the plot
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # set the labels as necessary
    ax.set_xlabel('MMWR Weeks (End Dates)')
    if len(ylabel) != 0: 
        ax.set_ylabel(ylabel)
    else:
        ax.set_ylabel(y)
    if logy:
        ax.set_ylabel(f'{ax.get_ylabel()} (Log Scale)')

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    # show and save the plot
    plt.savefig(
        f'../../plots/time-series-{ax.get_ylabel()}-vs-{ax.get_xlabel()}-by-{borough_col}.png',
        bbox_inches='tight',
        dpi=300
    )
    plt.show()

def scatter(df:pd.DataFrame, x:str, y:str, xlabel:str = '', ylabel:str = '', 
        logx:bool = False, logy:bool = False):
    """ Generate a scatter plot of two columns in a dataset.

    Args:
        df (`pd.DataFrame`): The dataset.
        x (str): x column
        y (str): y column
        xlabel (str, optional): Label of x axis. Defaults to `''`.
        ylabel (str, optional): Label of y axis. Defaults to `''`.
        logx (bool, optional): Log scale x? Defaults to `False`.
        logy (bool, optional): Log scale y? Defaults to `False`.
    """

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in df.columns:
        borough_col = 'pu_borough'
    elif 'do_borough' in df.columns:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    # iterate over the groups and plot their values
    fig, ax = plt.subplots()
    grouped_df = df.groupby(borough_col)
    for key, group in grouped_df:
        group.plot.scatter(ax=ax, x=x, y=y, label=key, color=BOROUGH_COLOURS[key],
            logx=logx, logy=logy)

    # move legend out of bounds
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # set the x and y labels as necessary
    if len(xlabel) != 0: 
        ax.set_xlabel(xlabel)
    if logx:
        ax.set_xlabel(f'{ax.get_xlabel()} (Log Scale)')

    if len(ylabel) != 0: 
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_ylabel(f'{ax.get_ylabel()} (Log Scale)')
    
    # show and save the plot
    plt.savefig(f'../../plots/scatter-{ax.get_ylabel()}-vs-{ax.get_xlabel()}-by-{borough_col}.png',
        dpi=300)
    plt.show()

def geospatial_distances_when_max(df: pd.DataFrame, borough_gj: gpd.GeoDataFrame, 
        max_col: str, virus_name: str, legend_name: str) -> folium.Map:
    """ Generate a geospatial map of trip distances at the maximum of a viral case rate.

    Args:
        df (`pd.DataFrame`): The dataset
        borough_gj (`gpd.GeoDataFrame`): The GeoJSON for borough polygons
        max_col (str): The column to find the maximal value for.
        virus_name (str): The name of the virus.
        legend_name (str): What to put in the legend colormap.

    Returns:
        `folium.Map`: The map.
    """

    # Create the folium map
    _map = folium.Map(location=[40.72, -74.20], 
        tiles="CartoDB PositronNoLabels", zoom_start=10)

    def miles_to_meters(miles: float)-> float:
        # from google
        METERS_IN_A_MILE = 1609.34
        return miles * METERS_IN_A_MILE

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in df.columns:
        borough_col = 'pu_borough'
    elif 'do_borough' in df.columns:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    # identify the max week for the max_col
    max_cases_idx = df.groupby([borough_col])[max_col]\
        .transform(max) == df[max_col]

    # only keep values from the max week.
    df = df[max_cases_idx]

    # add the borough outlines
    _map.add_child(folium.Choropleth(
        geo_data=borough_gj,
        name='borough layers',
        fill_opacity=0.75,
        data = df,
        columns=[borough_col, max_col],
        key_on = 'properties.boro_name',
        fill_color='YlOrRd',
        legend_name=legend_name
    ))

    # iterate through boroughs and add the borough names in a readable format.
    for borough, coord in borough_gj[['boro_name', 'centroid']].values:
        _map.add_child(folium.Marker(
                location = coord,
                icon = folium.DivIcon(
                    html = f'''
                    <h4 style=' color: black;
                                right: 50%; top: 0%;
                                position: absolute;
                                transform: translate(50%,-50%);
                                text-align: center;
                                padding: 3px;
                                border-radius: 10px;
                                background-color: rgba(255, 255, 255, 0.65)
                                '>
                    <b>{borough}</b>
                    </h4>
                    '''
                )
            )
        )

    # iterate through boroughs and add the average trip distance/radius
    # for the selected week
    for borough, coord in borough_gj[['boro_name', 'centroid']].values:
        borough_df = df[df[borough_col] == borough]
        _map.add_child(folium.Circle(
                location = coord,
                radius = miles_to_meters(borough_df['avg_trip_distance'].values[0]),
                weight=1.5,
                color='black',
            )
        )

    # save the map and return it (which allows it to be shown from a `.ipynb`)
    _map.save(f'../../plots/map-avg-trip-distance-at-max-{virus_name}-by-{borough_col}.html')
    return _map

def geospatial_average_distance(df: pd.DataFrame, 
        borough_gj: gpd.GeoDataFrame) -> folium.Map:
    """ Generate a geospatial map of average trip distances over the timeline.

    Args:
        df (`pd.DataFrame`): The dataset
        borough_gj (`gpd.GeoDataFrame`): The GeoJSON for borough polygons

    Returns:
        `folium.Map`: The map.
    """

    # Create the folium map
    _map = folium.Map(location=[40.72, -74.20], 
        tiles="CartoDB PositronNoLabels", zoom_start=10)

    def miles_to_meters(miles: float)-> float:
        """ Convert Miles to Meters """
        # This value comes from a google search.
        METERS_IN_A_MILE = 1609.34
        return miles * METERS_IN_A_MILE

    # check which borough column is included
    borough_col = ''
    if 'pu_borough' in df.columns:
        borough_col = 'pu_borough'
    elif 'do_borough' in df.columns:
        borough_col = 'do_borough'
    else:
        borough_col = 'borough'

    # iterate through the boroughs
    for borough, geom, coord in borough_gj[['boro_name', 'geometry', 'centroid']].values:

        # add the borough outlines
        _map.add_child(folium.Choropleth(
            geo_data=geom,
            name='borough layers',
            fill_opacity=0.75,
            fill_color=BOROUGH_COLOURS[borough]
            # fill_color='#feb24c'
        ))

        # add the borough names in a readable format.
        _map.add_child(folium.Marker(
                location = coord,
                icon = folium.DivIcon(
                    html = f'''
                    <h4 style=' color: black;
                                right: 50%; top: 0%;
                                position: absolute;
                                transform: translate(50%,-50%);
                                text-align: center;
                                padding: 5px;
                                border-radius: 10px;
                                background-color: rgba(255, 255, 255, 0.65)'>
                    <b>{borough}</b>
                    </h4>
                    '''
                )
            )
        )

    # iterate through boroughs and add the overall average trip distance/radius
    for borough, coord in borough_gj[['boro_name', 'centroid']].values:
        
        # calculate the borough's overall average trip distance
        total_distance = sum(
            df.loc[df[borough_col] == borough, 'avg_trip_distance'] *
            df.loc[df[borough_col] == borough, 'num_trips']
        )
        num_trips = sum(df.loc[df[borough_col] == borough, 'num_trips'])
        average_distance = total_distance / num_trips

        _map.add_child(folium.Circle(
                location = coord,
                radius = miles_to_meters(average_distance),
                weight=1.5,
                color='black',
            )
        )

    # save the map and return it (which allows it to be shown from a `.ipynb`)
    _map.save(f'../../plots/map-avg-trip-distance-overall-{borough_col}.html')
    return _map


def diagnostic_observed_fitted(df: pd.DataFrame, model: ResultsWrapper, 
    colname: str, label: str):
    """ Generate a plot to compare the fitted and observed values 
    of a linear model/

    Args:
        df (`pd.DataFrame`): Dataset the model is built on
        model (`ResultsWrapper`): The model fit
        colname (str): Name of the column that we're analysing in df
        label (str): Name of the variable as it appears in x and y axes
    """

    # create the plots themselves
    fig, ax = plt.subplots()
    ax.scatter(
        model.predict(), 
        df[colname].iloc[model.fittedvalues.index]
    )
    line_fit = sm.OLS(
        df[colname].iloc[model.fittedvalues.index], 
        sm.add_constant(model.predict(), prepend=True)
    ).fit()
    abline_plot(model_results=line_fit, ax=ax)

    # set labels
    ax.set_title('Model Fit Plot')
    ax.set_ylabel(f'Observed {label}')
    ax.set_xlabel(f'Fitted {label}');

    # show and save the plot
    plt.savefig(
        f'../../plots/diagnostic-{type(model.model).__name__}-{ax.get_ylabel()}-vs-{ax.get_xlabel()}.png',
        bbox_inches='tight',
        dpi=300
    )
    plt.show()