import numpy as np
import pandas as pd
from os import listdir
from os.path import isfile, join, basename
from pdb import set_trace
from root_pandas import read_root
import matplotlib.pyplot as plt
import numpy as np
import os
import calendar

def syscall(cmd):
    print 'Executing: %s' % cmd
    retval = os.system(cmd)
    if retval != 0:
        raise RuntimeError('Command failed!')



RESCALED = True
temp = 0 #in degrees 
subdetector = 'TECMinus' # TIB TID TECPlus TECMinus
#run_Numb = 216430
#run_Numb = 306609 #TIB
run_Numb = 0
#open root file with constants                                                                                  
inputCostants = '/opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/Thresholds/basic.root'
df_const = read_root(inputCostants, columns=['dcuHardId','adcGain0','adcOffset0','i10'])
B = 3280.
T0 = 298.1 #it cannot be in Celsius ...
Rt0 = 10000. 

tob  = []
tecp = []
tecm = []
tib  = []

mypath =  '/opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/Thresholds'

string_det = ""
if subdetector == 'TIB' or subdetector == 'TID': string_det = "_TI_"
if subdetector == 'TOB': string_det = "_TO_"
if subdetector == 'TECPlus': string_det = "_TP_"
if subdetector == 'TECMinus': string_det = "_TM_"


if string_det == "": 
    print 'no partition defined!!!!'


files_Thr = [f for f in listdir(mypath) if isfile(join(mypath, f)) and 'Temp' not in f and string_det in f ]
files_Temp = [f for f in listdir(mypath) if isfile(join(mypath, f)) and 'Temp' in f and string_det in f ] 
ret_df = pd.DataFrame()

for file_ in files_Thr:
    run,part =  basename(file_).split('_')[1:3]
    thresholds_array = np.load(join(mypath, file_))
    name = basename(file_).split('.')[0]

    if run_Numb !=0 and run != str(run_Numb):
        print run, str(run_Numb)
        continue

    file_Temp = [f for f in files_Temp  if name in f]
    if len(file_Temp)==0 : 
        print 'no Temperature file'
        continue
 
    if len(file_Temp)>1: 
        raise Exception('There are two temperature files! This shoudn t happen!')
    else: 
        temp_array = np.load(join(mypath,file_Temp[0]))
    

    df_thr = pd.DataFrame(thresholds_array)
    df_temp = pd.DataFrame(temp_array)


    if subdetector == 'TIB': df_thr = df_thr.loc[df_thr['Detector'] == 'TIB']
    if subdetector == 'TID': df_thr = df_thr.loc[df_thr['Detector'] == 'TID']
    #else : df_thr = df_thr.loc[df_thr['Detector'] == 'TEC']

    # let's average if more than one temp value for a given DeviceID


    df_temp = df_temp.dropna(subset=['channel4'])

    df_thr = df_thr.dropna(subset=['Threshold0'])
    df_thr = df_thr.loc[df_thr['IsValid'] == 1]

    if df_thr.shape[0]==0 or df_temp.shape[0]==0:
        print "empty dataframe"
        print df_thr.shape[0] , df_temp.shape[0]
        continue


    start_time_thr = df_thr['start_time'].iloc[0]
    end_time_thr = df_thr['start_time'].iloc[0] + df_thr['delta_time'].iloc[0]
    
    avg_time = int((start_time_thr+end_time_thr)/2.)

   #timestamp detId channe4
   # df_temp =  df_temp.groupby('dcuHardId',as_index=False)['channel4'].mean() #there should be only one per dcuHardId .. that's just to be oversafe

   # set_trace()
    if df_temp['typeGlRun'].iloc[0]=='noCollision':
        print "no collision run!"
        df_temp =  df_temp.groupby('dcuHardId')
        df_temp = df_temp.apply(lambda x: x.channel4.loc[np.abs(x.dcuTimeStamp - avg_time).idxmin()])

    elif df_temp['typeGlRun'].iloc[0]=='collision':
        print "collision run!"
        glRun_end = int(calendar.timegm(df_temp['glRun_start'].iloc[0].timetuple())+df_temp['glRun_delta'].iloc[0])
        df_temp =  df_temp.groupby('dcuHardId') 
        df_temp = df_temp.apply(lambda x: x.channel4.loc[np.abs(x.dcuTimeStamp - glRun_end).idxmin()])


    df_temp_gr = df_temp.reset_index()
    df_temp_gr.columns.values[1] = 'channel4'
    df_temp_gr = df_temp_gr[df_temp_gr.channel4 != 4095]
    df_thr =  df_thr.groupby('dcuHardId',as_index=False)['Threshold0','Layer','Cl','Detid','Mode'].mean() 

    df_all = pd.merge(df_thr, df_temp_gr, on='dcuHardId', how='outer') 
    df_all = pd.merge(df_all,df_const,on='dcuHardId',how='outer')
    df_all = df_all.dropna(subset=['channel4','Threshold0','Layer','Cl','Detid','Mode'])


    #tranform temperature from ADC count to celsisu
    df_all['val']=(np.log((df_all.channel4-df_all.adcOffset0)/(df_all.adcGain0*df_all.i10*Rt0)))
    df_all['Temp_Hyb']=1./((1./T0)+(df_all.val/B))-273.15

    #df_all['val']=(df_all.channel4_avg-df_all.adcOffset0)/(df_all.adcGain0*df_all.i10)
    #df_all['Temp_Hyb']=1./(1./B*np.log(df_all.val/10000.)+1/298.1)-273.1#-273.15

    df_all = df_all.dropna(subset=['Temp_Hyb'])

       
    if RESCALED:

        df_all['rescaled_Thr']=df_all.Threshold0*np.exp(((temp+273.15)-(df_all.Temp_Hyb+273.15))/55.)#etcc... 258.15         
    else: df_all['rescaled_Thr'] = df_all.Threshold0

    #############################
    ########TRACKER MAP##########
    ############################# 


    if run_Numb !=0 and run==str(run_Numb):
        print 'doing tracker map'
        df_forMap = df_all[['Detid','rescaled_Thr']]
        df_forMap.Detid = df_forMap.Detid.astype(int)
        
        df_forMap.to_csv('/opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/inputMap.txt', sep=' ', index=False, header=False)
        syscall('print_TrackerMap /opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/inputMap.txt "Threshold Current" /opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/outputMap_%s_%s_%s.png 4500 False False 0 5'%(str(run_Numb),subdetector,str(RESCALED)))
   
   
    #############################
    ########TEMPERATURE MAP##########
    ############################# 

    if run_Numb !=0 and run==str(run_Numb):
        
        print 'entra'
        df_forMap = df_all[['Detid','Temp_Hyb']] 
        df_forMap.Detid = df_forMap.Detid.astype(int) 
        df_forMap.to_csv('/opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/inputMap.txt', sep=' ', index=False, header=False) 
        syscall('print_TrackerMap /opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/inputMap.txt "Temperature" /opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/temp_zoom_%s_%s_%s.png 4500 False False -20 30'%(str(run_Numb),subdetector,str(RESCALED)))
        exit


    ####################################  
        

    Thr_avg = df_all["rescaled_Thr"].median() 
   
    if part == 'TO':
        grouped =  df_all.groupby('Layer')
        medians = grouped['rescaled_Thr'].median().reset_index()
        medians['run'] = int(run)
        ret_df = pd.concat([ret_df, medians])

    elif part == 'TI':
        grouped = df_all.groupby('Layer')
        medians = grouped['rescaled_Thr'].median().reset_index()
        #medians = grouped['Temp_Hyb'].median().reset_index()
        medians['run'] = int(run)
        ret_df = pd.concat([ret_df, medians])
        #print run, medians

    elif part == 'TP':
        grouped = df_all.groupby('Cl')
        medians = grouped['rescaled_Thr'].median().reset_index()
        medians['run'] = int(run)
        ret_df = pd.concat([ret_df, medians])
        

    elif part == 'TM':
        grouped = df_all.groupby('Cl')
        medians = grouped['rescaled_Thr'].median().reset_index()
        medians['run'] = int(run)
        ret_df = pd.concat([ret_df, medians])
        


        
ret_df.to_csv('%srescaled.csv' %(subdetector), index=False, header=False)

if part == 'TI' or part == 'TO':
    fig, ax = plt.subplots()
    ax.set_ylabel("LLD Threshold Change [mA]")
    ax.set_xlabel("CMS Run Number")
    ax.set_ylim(-0.5, 2.0)

    ret_df = ret_df.loc[ret_df['run'] > 266102]

    first_run = ret_df.run.min()
    layers = set(ret_df.Layer)
    for layer in layers:
        zero = ret_df[(ret_df.run == first_run) & (ret_df.Layer == layer)].rescaled_Thr.min()
        ret_df.loc[ret_df.Layer == layer, 'rescaled_Thr'] -= zero

    #ret_df.groupby('Layer').plot(x='run', y='rescaled_Thr', ax=ax, style='o', legend=True)
    for label, df in ret_df.groupby('Layer'):
        df.plot(x='run', y='rescaled_Thr', ax=ax, style='o',label=label)
    plt.legend(loc='best')
    #plt.legend(['Layer 1','Layer 2', 'Layer 3', 'Layer 4'])
    plt.show()

if part == 'TP' or part == 'TM':
    fig, ax = plt.subplots()
    ax.set_ylabel("LLD Threshold Change [mA]")
    ax.set_xlabel("CMS Run Number")
    ret_df = ret_df.loc[ret_df['run'] > 266102]
    ret_df.groupby('Cl').plot(x='run', y='rescaled_Thr', ax=ax, style = 'o', legend=True)
    plt.show()

#plt.scatter(*zip(*tob))
#plt.show()
