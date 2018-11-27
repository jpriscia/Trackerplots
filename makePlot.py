import numpy as np
import pandas as pd
from os import listdir
from os.path import isfile, join, basename
from pdb import set_trace
from root_pandas import read_root
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
import numpy as np
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('partition', help='specify partition: TEC, TIB, TID or TOB',  type=str, choices=['TECM','TECP','TIB','TOB'])
options = parser.parse_args()

partName = ''

#are those correct?                                                                                                                                                                                   

if    options.partition == 'TECM': names = ['Mode','Threshold','run'] 
elif  options.partition == 'TIB': names=['Layer','Threshold','run']
elif  options.partition == 'TECP': names=['Mode','Threshold','run']
elif  options.partition == 'TOB': names=['Layer','Threshold','run']



df = pd.read_csv('%srescaled.csv' %(options.partition) ,names=names)


# stilish stuff....

plt.rcParams['text.usetex'] = True
plt.rcParams["mathtext.default"] = 'regular'
plt.rcParams["mathtext.fontset"] = "stix"
plt.rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
#



#fig, ax = plt.subplots(figsize=(20, 10))
fig = plt.figure(figsize=(20, 10), dpi= 80, facecolor='w', edgecolor='k')
ax = fig.add_subplot(111)


#ax.set_ylabel("LLD Threshold Change [mA]")
#ax.set_xlabel("CMS Run Number")
#if options.partition == 'TOB': ax.set_ylim(-0.5, 0.5)
#if options.partition == 'TECM' or options.partition == 'TECP' : ax.set_ylim(-0.2, 0.5)
#ax.yaxis.label.set_size(20)
#ax.xaxis.label.set_size(20)

ax.xaxis.set_major_formatter(
    ticker.FormatStrFormatter("%d")
    )
ax.yaxis.set_major_formatter(
    ticker.FormatStrFormatter("%.1f")
    )


import matplotlib.ticker as plticker
loc = plticker.MultipleLocator(base=0.1) # this locator puts ticks at regular intervals
ax.yaxis.set_minor_locator(loc)
ax.tick_params(axis='both', labelsize=29, which='both')


x_min = 266102
x_max = df['run'].max()
delta_x = x_max-x_min

y_min = -0.5
y_max = 2.0
if options.partition == 'TECM' or options.partition == 'TECP' :
    y_min = -0.2
    y_max = 0.5
delta_y = y_max-y_min


plt.text(
    x_min+(x_max-x_min)*0.01, y_max+0.025*delta_y,
    r'''\textbf{CMS} \textit{Preliminary}''',
fontsize=32
)

x_years=[271036,294645,315242]

txt = plt.text(
    x_min+delta_x*0.01, y_max-delta_y*0.1,
    r'2015',
    fontsize=24,
    horizontalalignment='left'
    )

txt = plt.text(
    x_years[0]+delta_x*0.01, y_max-delta_y*0.1,
    r'2016',
    fontsize=24,
    horizontalalignment='left'
    )

txt = plt.text(
    x_years[1]+delta_x*0.01, y_max-delta_y*0.1,
    r'2017',
    fontsize=24,
    horizontalalignment='left'
    )

txt = plt.text(
    x_years[2]+delta_x*0.01, y_max-delta_y*0.1,
    r'2018',
    fontsize=24,
    horizontalalignment='left'
    )


df = df.loc[df['run'] > x_min]
first_run = df.run.min()

if options.partition == 'TOB' or options.partition == 'TIB':
    layers = set(df.Layer)
    for layer in layers:
        zero = df[(df.run == first_run) & (df.Layer == layer)].Threshold.min()
        df.loc[df.Layer == layer, 'Threshold'] -= zero

    for label, df in df.groupby('Layer'):
        df.plot(x='run', y='Threshold', ax=ax, style='s',label='Layer %d' % label)




if options.partition == 'TECP' or options.partition == 'TECM':
    rings = set(df.Mode)
    for ring in rings:
        zero = df[(df.run == first_run) & (df.Mode == ring)].Threshold.min()
        df.loc[df.Mode == ring, 'Threshold'] -= zero
        
    df = df[df.Threshold < 0.3]
    for label, df in df.groupby('Mode'):
        df.plot(x='run', y='Threshold', ax=ax, style='s',linestyle= '-',label='Mod %d' % label)
        

plt.xticks(fontsize=24, rotation=40)
plt.yticks(fontsize=24, rotation=0)

plt.legend(
    bbox_to_anchor=(0.4, 0.9),
    loc=1,
    numpoints=1,
    ncol=2, #mode="expand", borderaxespad=0.,
    fontsize=20,
    frameon=True,
    )

plt.axvline(x=271036,linestyle='--',color='g')
plt.axvline(x=294645,linestyle='--',color='g')
plt.axvline(x=315242,linestyle='--',color='g')

plt.ylim((y_min, y_max))
plt.xlim((x_min-2000, x_max+2000))

plt.xlabel(
    r'CMS Run Number', fontsize=28,
    horizontalalignment='right', x=1.0,
)

plt.ylabel(
    r'LLD Threshold Change [mA]', fontsize=28,
    horizontalalignment='right',
    y=0.94
)

fig.tight_layout(rect=[0.05, 0.05, 0.95, 0.95])
plt.show()
