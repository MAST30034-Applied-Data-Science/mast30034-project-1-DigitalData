import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import geopandas as gpd
import folium

ALL_COLOURS = ['red', 'green', 'blue', 'cyan', 'magenta', 'lime', 'orange', 'yellow']

def time_series(df: pd.DataFrame, y: str, grouping: str, ylabel: str = '',
    logy:bool = False):

    # determine colours by grouping
    colours = {}
    modifier_index = 0
    for group in set(df[grouping]):
        colours[group] = ALL_COLOURS[modifier_index]
        modifier_index += 1

    # extract the dates
    df['week_ending_date'] = pd.to_datetime(df['week_ending'])

    # create the time-series by group
    fig, ax = plt.subplots()
    grouped_df = df.sort_values('week_ending_date').groupby(grouping)
    for key, group in grouped_df:

        # apply dates formatting
        # from: https://stackoverflow.com/questions/14946371/editing-the-date-formatting-of-x-axis-tick-labels-in-matplotlib
        ax.xaxis.set_major_locator(mdates.YearLocator(base = 1, month = 1, day = 1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        group.plot(ax=ax, x='week_ending_date', y=y, kind='line', label=key,
            color=colours[key], logy=logy)

    # create a legend outside the plot
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # set the labels as necessary
    ax.set_xlabel('MMWR Weeks (End Dates)')
    if len(ylabel) != 0: 
        ax.set_ylabel(ylabel)
    else:
        ax.set_ylabel(y)
    if logy:
        ax.set_ylabel(f'{ax.get_ylabel()} (log scale)')

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    # show and save the plot
    plt.savefig(
        f'../plots/time-series-{ax.get_ylabel()}-vs-{ax.get_xlabel()}-by-{grouping}.png',
        bbox_inches='tight',
        dpi=300
    )
    plt.show()

def group_plot(df:pd.DataFrame, x:str, y:str, grouping:str = 'week_index', 
    kind:str = 'scatter', filename_prefix:str = '', xlabel:str = '', 
    ylabel:str = '', logx:bool = False, logy:bool = False):
    """Generate and save a plot using panda's groupby feature.

    Args:
        df (`pd.DataFrame`): Dataset to plot.
        x (str): Column name of x-axis.
        y (str): Column name of y-axis.
        grouping (str): Column name of different groups (which are coloured differently). Defaults to 'type'.
        kind (str, optional): Type of plot [`scatter`, `parallel`, `bar`]. Defaults to 'scatter'.
        filename_prefix (str, optional): Used when naming the plot image file. Defaults to ''.
        xlabel (str, optional): Name of x-axis. If empty, then uses `x`.
        ylabel (str, optional): Name of y-axis. If empty, then uses `y`.
        logx (bool, optional): Apply log scale to x-axis. Defaults to False.
        logy (bool, optional): Apply log scale to x-axis. Defaults to False.
    """

    # list of colours/markers I'd like to use
    # there should never be more than some 6 groups anyways (for legibility)
    all_markers = ['.', 'x', '+', '1', '*', 'p', 's']
    colours = {}
    markers = {}
    modifier_index = 0
    
    # fill the colour map
    for group in set(df[grouping]):
        colours[group] = ALL_COLOURS[modifier_index]
        markers[group] = all_markers[modifier_index]
        modifier_index += 1

    # iterate over the groups and plot their values
    fig, ax = plt.subplots()
    grouped_df = df.groupby(grouping)
    for key, group in grouped_df:
        group.plot(ax=ax, kind=kind, x=x, y=y, label=key, 
        color=colours[key], marker=markers[key],
        logx=logx, logy=logy)

    # move legend out of bounds
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # set the x and y labels as necessary
    if len(xlabel) != 0: 
        ax.set_xlabel(xlabel)
    if logx:
        ax.set_xlabel(f'{ax.get_xlabel()} (log scale)')

    if len(ylabel) != 0: 
        ax.set_ylabel(ylabel)
    if logy:
        ax.set_ylabel(f'{ax.get_ylabel()} (log scale)')
    
    # show and save the plot
    plt.savefig(f'../plots/scatter-{filename_prefix}-{ax.get_ylabel()}-vs-{ax.get_xlabel()}-by-{grouping}.png',
        dpi=300)
    plt.show()

def geospatial_distances_when_max(tlc_df: pd.DataFrame, viral_df: pd.DataFrame, 
        borough_gj: gpd.GeoDataFrame, max_col: str, virus_name: str) -> folium.Map:
    #TODO: commenting geospatial_distances_when_max
    _map = folium.Map(location=[40.66, -73.94], tiles="OpenStreetMap", zoom_start=10)

    def miles_to_meters(miles: float)-> float:
        # from google
        METERS_IN_A_MILE = 1609.34
        return miles * METERS_IN_A_MILE

    df = tlc_df.merge(viral_df, 
        left_on = ['pu_borough', 'week_ending'],
        right_on = ['borough', 'week_ending'])

    colour_index = 0
    for borough, geom, coord in borough_gj[['boro_name', 'geometry', 'centroid']].values:

        _map.add_child(folium.Choropleth(
            geo_data=geom,
            name=borough,
            fill_color=ALL_COLOURS[colour_index],
            fill_opacity=0.2,
            legend_name=borough
        ))

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
        
        borough_df = df[df['borough'] == borough]
        borough_df = borough_df.loc[borough_df[max_col].idxmax()]

        _map.add_child(folium.Circle(
                location = coord,
                # tooltip = borough,
                radius = miles_to_meters(borough_df['avg_trip_distance']),
                # fill_color=ph.ALL_COLOURS[colour_index],
                # fill_opacity=1,
                weight=2,
                color=ALL_COLOURS[colour_index],
            )
        )


        colour_index += 1

    _map.save(f'../plots/avg-trip-distance-at-max-{virus_name}.html')
    return _map