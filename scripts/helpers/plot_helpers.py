import pandas as pd
import matplotlib.pyplot as plt

def group_plot(df:pd.DataFrame, x:str, y:str, grouping:str = 'type', kind:str = 'scatter',
    filename_prefix:str = '', xlabel:str = '', ylabel:str = '', logx:bool = False, logy:bool = False):
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
    all_colours = ['red', 'green', 'blue', 'purple', 'orange', 'yellow', 'brown']
    all_markers = ['.', 'x', '+', '1', '*', 'p', 's']
    colours = {}
    markers = {}
    modifier_index = 0
    
    # fill the colour map
    for group in set(df[grouping]):
        colours[group] = all_colours[modifier_index]
        markers[group] = all_markers[modifier_index]
        modifier_index += 1

    # iterate over the groups and plot their values
    fig, ax = plt.subplots()
    grouped_df = df.groupby(grouping)
    for key, group in grouped_df:
        group.plot(ax=ax, kind='scatter', x=x, y=y, label=key, 
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
    plt.savefig(f'../plots/scatter-{filename_prefix}-{ax.get_ylabel()}-vs-{ax.get_xlabel()}-by-{grouping}.png')
    plt.show()