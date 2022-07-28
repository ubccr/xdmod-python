import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import numpy 
from datetime import date

def quality_heatmap(df):
        
    dates = list(df.columns)
    jobs = list(df.index)

    fig, ax = plt.subplots(figsize=(8, 8))
    cmap = plt.get_cmap("RdYlGn").copy()
    cmap.set_bad('gray', 1.)
    im = ax.imshow(df, cmap = cmap, vmin=0, vmax=100)

    ax.set_xticks(numpy.arange(len(dates)))
    ax.set_yticks(numpy.arange(len(jobs)))
                  
    ax.set_xticklabels(dates)
    ax.set_yticklabels(jobs)
    
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
    
    for i in range(len(jobs)):
        for j in range(len(dates)):
            text = ax.text(j, i, df.at[jobs[i], dates[j]], ha="center", va="center", color="w")
    ax.set_title(df.name)
    fig.tight_layout()
    plt.show()
        
def resource_plot(df, resource, past_df = None):
    
    dates = list(df.columns)
    quality = [df.loc[resource][date] for date in dates]
    
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
    '''
    for i,j in zip(dates, quality):
        ax1.scatter(i, j, s=50, c='b', marker="s")
    #ax1.scatter(x[0:],y[0:], s=10, c='r', marker="o", label='second')
    #plt.legend(loc='upper left');
    
    if past_df is not None:
        past_dates = list(past_df.columns)
        past_quality = [past_df.loc[resource][date] for date in past_dates]
        for i,j in zip(dates, past_quality):
            ax1.scatter(i, j, s=50, c='r', marker="o")
    '''
    ax1.scatter(dates, quality, s=80, c='b', marker="s", label='This Week')
    
    if past_df is not None:
        past_dates = list(past_df.columns)
        past_quality = [past_df.loc[resource][date] for date in past_dates]
        ax1.scatter(dates, past_quality, s=40, c='r', marker="o", label='Prev Week')
    
    plt.ylim(top=105)
    
    plt.legend(loc='upper left')
    plt.xlabel('Dates')
    plt.ylabel(df.name)
    plt.title(f'Report for {resource} resource')
    plt.show()
