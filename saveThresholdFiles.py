import ROOT
from root_pandas import read_root
import pandas as pd
from pdb import set_trace
import numpy as np
import os
import matplotlib.pyplot as plt
import os,sys,getopt,glob,cx_Oracle,subprocess
import argparse
import os.path
import time,calendar
from datetime import datetime
from datetime import timedelta

#desc DCU or viewdcu
#da ADC a celsius

def syscall(cmd):
    print 'Executing: %s' % cmd
    retval = os.system(cmd)
    if retval != 0:
        raise RuntimeError('Command failed!')

#run partizione per il nome del tree

def printQuery(analysisId, runNumber):
    myQuery = ( "select distinct "
                  "TKF.DETECTOR      Detector,"
                  "TKF.SIDE          Side,"
                  "TKF.LAYER         Layer,"
                  "TKF.CL            Cl,"
                  "TKF.CR            Cr,"
                  "TKF.MOD           Mod,"
                  "TKF.RACK          Rack,"
                  "TKF.DETID         Detid,"
                  "DCU.DCUHARDID           dcuHardId,"
                  "FEC.CRATESLOT           Crate,"
                  "FEC.FECSLOT             Fec,"
                  "RING.RINGSLOT           Ring,"
                  "CCU.CCUADDRESS          Ccu,"
                  "CCU.ARRANGEMENT         CcuArrangement,"
                  "HYBRID.I2CCHANNEL       I2CChannel,"
                  "DEVICE.I2CADDRESS       I2CAddress,"
                  "ROUND((DEVICE.I2CADDRESS-.5)/2)-16 lasChan,"
                 " AOS.DEVICEID DeviceId,"
                 " AOS.FEDID    FedId,"
                 " AOS.FEUNIT   FeUnit,"
                 " AOS.FECHAN   FeChan,"
                 " AOS.FEDAPV   FeApv,"
                 " AOS.GAIN    Gain,"
                 " AOS.BIAS0   Bias0,"
                 " AOS.MEASGAIN0    MeasGain0,"
                 " AOS.ZEROLIGHT0   ZeroLight0,"
                 " AOS.LINKNOISE0  LinkNoise0,"
                " AOS.LINKNOISE1  LinkNoise1,"
                " AOS.LINKNOISE2  LinkNoise2,"
                " AOS.LINKNOISE3  LinkNoise3,"
                 " AOS.LIFTOFF0  LiftOff0,"
                 " AOS.THRESHOLD0  Threshold0,"
                 " AOS.THRESHOLD1  Threshold1,"
                 " AOS.THRESHOLD2  Threshold2,"
                 " AOS.THRESHOLD3  Threshold3,"
                 " AOS.TICKHEIGHT0  TickHeight0,"
                 " AOS.ISVALID  IsValid,"
                 " AOS.BASELINESLOP0  BaselinesSlop0"
                 " from"
                 " ANALYSISOPTOSCAN AOS join"
                 " ANALYSIS on AOS.ANALYSISID = ANALYSIS.ANALYSISID join"
                 " RUN      on RUN.RUNNUMBER  = ANALYSIS.RUNNUMBER  join"
                 " STATEHISTORY on STATEHISTORY.STATEHISTORYID = RUN.STATEHISTORYID join"
                 " DEVICE on AOS.DEVICEID=DEVICE.DEVICEID join"
                 " HYBRID on DEVICE.HYBRIDID=HYBRID.HYBRIDID join"
                 " CCU    on HYBRID.CCUID=CCU.CCUID join"
                 " RING   on CCU.RINGID=RING.RINGID join"
                 " FEC    on RING.FECID=FEC.FECID   join"
                 " DEVICE b on b.HYBRIDID = HYBRID.HYBRIDID          join"
                 " DCU      on b.DEVICEID = DCU.DEVICEID   and"
                 "             DCU.VERSIONMAJORID = STATEHISTORY.FECVERSIONMAJORID and"
                 "             DCU.VERSIONMINORID = STATEHISTORY.FECVERSIONMINORID left outer join"
                 " tk_fibers tkf on DCU.DCUHARDID = tkf.dcuid and "
                 " mod( AOS.FECHAN,3) = mod(fiber,3) "
                 " where "
                 " AOS.ANALYSISID=%s and RUN.RUNNUMBER=%s")

    return myQuery %(analysisId, runNumber)

def writeDF(infos_q3, start_time, delta_time):
   
   df = pd.DataFrame(infos_q3)
   df.columns = ['Detector','Side','Layer','Cl','Cr','Mode','Rack','Detid','dcuHardId','Crate','Fec','Ring','Ccu','CcuArrangement','I2CChannel','I2CAddress','lasChan','DeviceId','FedId','FedUnit','FeChan','FeApv','Gain','Bias0','MeasGain0','ZeroLight0','LinkNoise0','LinkNoise1','LinkNoise2','LinkNoise3','LiftOff0','Threshold0','Threshold1','Threshold2','Threshold3','TickHeight0','IsValid','BaselinesSlop0']

   df['start_time'] = start_time
   df['delta_time'] = delta_time

   rr = df.to_records(index=False)

   rr.dtype.names = [str(i) for i in rr.dtype.names]
   return rr

#DCU-> temperatura
#Threshold -> per device 



parser = argparse.ArgumentParser()
parser.add_argument('partition', help='specify partition: TEC, TIB, TID or TOB',  type=str, choices=['TECM','TECP','TIB','TOB'])
options = parser.parse_args()

partName = ''

#are those correct? 
if    options.partition == 'TECM': partName = 'TM_09-JUN-2009_1'
elif  options.partition == 'TIB': partName = 'TI_27-JAN-2010_2'  
elif  options.partition == 'TECP': partName = 'TP_09-JUN-2009_1'
elif  options.partition == 'TOB': partName = 'TO_30-JUN-2009_1'



#metti la partition name in input to q1

#connect to Oracle
conn_str = os.path.expandvars("$CONFDB")
conn     = cx_Oracle.connect('cms_trk_tkcc/A4npLENFPA4@cms_omds_lb')
e        = conn.cursor()


#gloabal run query
#siamo sicuri vogliamo selezionare la partizione
#e.execute("select RUNNUMBER,STARTTIME,ENDTIME from RUN join PARTITION using(PARTITIONID) where PARTITIONNAME='%s' and (RUNMODE = 18 or RUNMODE = 2)" %partName)
e.execute("select runPar.string_value, a.RUNNUMBER, runPar.TIME ,  a.STARTTIME, a.ENDTIME from RUN a join PARTITION b on a.PARTITIONID=b.PARTITIONID join  CMS_RUNINFO.runsession_parameter runPar ON  runPar.RUNNUMBER=a.RUNNUMBER  where (a.RUNMODE = 18 or a.RUNMODE = 2) and runPar.NAME='CMS.LVL0:TRIGGER_MODE_AT_START' and b.PARTITIONNAME='%s' ORDER BY a.RUNNUMBER" %partName)

infos_GR = e.fetchall()
time_GR=[]
for result in infos_GR:
    if result[3] and result[4]:
        time_GR.append((result[0],result[1],result[2],np.abs(result[4]-result[3]).total_seconds()))
dfGR_time = pd.DataFrame(time_GR,columns=['runType','runNumber','timeGR_start','deltaT'])

#first query
#e.execute("select distinct RUNNUMBER,STARTTIME,ENDTIME,PARTITIONNAME from RUN join(PARTITION) using(PARTITIONID) where RUNMODE=4  and PARTITIONNAME='%s' ORDER BY RUNNUMBER" %partName)
e.execute("select a.RUNNUMBER, runPar.TIME, a.STARTTIME, a.ENDTIME from RUN a join PARTITION b on a.PARTITIONID=b.PARTITIONID join  CMS_RUNINFO.runsession_parameter runPar ON  runPar.RUNNUMBER=a.RUNNUMBER  where ( a.RUNMODE = 4)  and b.PARTITIONNAME='%s'  and runPar.NAME='RunType'  ORDER BY a.RUNNUMBER" %partName)

infos_q1 = e.fetchall()
runs = []
for result in infos_q1:
    #print result[3], result[2]
    runs.append((int(result[0]),result[1],np.abs(result[3]-result[2]).total_seconds()))


#divide global run in collisions from global run not in collision

dfGR_time_coll = dfGR_time.loc[(dfGR_time['runType'].str.contains("collisions")) & (dfGR_time['deltaT']>60*10) ] 
dfGR_time_coll.reset_index(drop=True, inplace=True)
dfGR_time_noColl = dfGR_time.loc[(~dfGR_time['runType'].str.contains("collisions")) & (dfGR_time['deltaT']>60*10)]
dfGR_time_noColl.reset_index(drop=True, inplace=True)

good_runs=[]
for run  in runs:

    #if run[0]==228555:set_trace()
    if run[1]:
        
        delta_start_coll = np.abs(dfGR_time_coll.timeGR_start - run[1]).min()
        delta_start_noColl = np.abs(dfGR_time_noColl.timeGR_start - run[1]).min()

        if delta_start_noColl.total_seconds()<12*3600 :
            print "noCollision"
            dfGR_time_noColl['diff_start'] =np.abs(dfGR_time_noColl.timeGR_start - run[1])

            #save good run, start and end, and closest global run
            good_runs.append((run[0],run[1],run[2],dfGR_time_noColl['timeGR_start'].iloc[dfGR_time_noColl['diff_start'].idxmin()], 'noCollision', dfGR_time_noColl['runNumber'].iloc[dfGR_time_noColl['diff_start'].idxmin()],dfGR_time_noColl['deltaT'].iloc[dfGR_time_noColl['diff_start'].idxmin()]))
           
        elif delta_start_coll.total_seconds()<12*3600 :
            print "collision"
            dfGR_time_coll['diff_start'] =np.abs(dfGR_time_coll.timeGR_start - run[1])
            good_runs.append((run[0], run[1], run[2], dfGR_time_coll['timeGR_start'].iloc[dfGR_time_coll['diff_start'].idxmin()], 'collision', dfGR_time_coll['runNumber'].iloc[dfGR_time_coll['diff_start'].idxmin()],dfGR_time_noColl['deltaT'].iloc[dfGR_time_noColl['diff_start'].idxmin()]))

        else:
            print "nothing for run ", run[0], delta_start_coll, delta_start_noColl
    
        print ""
    

for run in good_runs:
    if run[0]<266102: continue
    
    #second query, for each run
    e.execute("select max(analysisid), ANALYSISTYPE, RUNNUMBER, PARTITIONNAME from analysis a join partition b on a.PARTITIONID = b.PARTITIONID where PARTITIONNAME='%s' and RUNNUMBER=%s group by ANALYSISTYPE,RUNNUMBER,PARTITIONNAME" %(partName, run[0])) 
    info_q2 = e.fetchall()
    if len(info_q2)==0: continue
    analysisid = info_q2[0][0]

    partitionName = info_q2[0][3]
    analysisType = info_q2[0][1]

    
    #take only the optoscan runs and make the third query in case the array doesn't exist yet
    fileName = analysisType+'_'+str(run[0])+'_'+partitionName+'.npy'
    docs_dir=os.path.expanduser('/opt/cmssw/shifter/jessica/CMSSW_10_2_0_pre4/python/Thresholds')

    if str(analysisType) == 'OPTOSCAN':
        if not (os.path.isfile(docs_dir+'/'+fileName)):

            e.execute(printQuery(analysisid,run[0]))
            infos_q3 = e.fetchall()


            arrayToSave = writeDF(infos_q3, int(calendar.timegm(run[1].timetuple())), int(run[2]))

            np.save(os.path.join(docs_dir,fileName),arrayToSave)
            print fileName,' done...'

        #check if the Temperature file for that given run doesn't exixt yet
        fileNameTemp = analysisType+'_'+str(run[0])+'_'+partitionName+'_Temp.npy'
        
    
        if not (os.path.isfile(docs_dir+'/'+fileNameTemp)):
            
            #I need to convert the datetime in unixtime and take temperatures measured within (?)hours from the beginning/end of the run

            if run[4]=='noCollision':
                time_start =  run[3]-timedelta(seconds=30)   #run[1]-timedelta(hours=12)
                time_end =    run[3]+timedelta(seconds=run[6])   #run[2]+timedelta(hours=12)
                print 'no coll ', time_start, time_end
            if run[4] == 'collision':
                time_start =  run[1]-timedelta(hours=12)                                                    
                time_end =    run[1]+timedelta(hours=12) 
                print 'coll ', time_start, time_end
            unixtime_start = calendar.timegm(time_start.timetuple())  
            unixtime_end = calendar.timegm(time_end.timetuple())
            # query to get temperature
            #######
            ###channel 0 -> Silicon Sensor Temperature
            ###channel 1 -> V250
            ###channel 2 -> V125
            ###channel 3 -> Sensor Leakage current
            ###channel 4 -> Hybrid Temperature
            ###channel 5 -> not used
            ###channel 6 -> not used
            ###channel 7 -> DCU temperature
            #######
            print 'making query: ',unixtime_start,unixtime_end

            #e.execute("with pgroup as ( select distinct dcu_id, detid, cable from  CMS_TRK_DCS_CONF.DCUS join cms_trk_TKCC.tk_fibers on dcu_id=tk_fibers.dcuid) Select avg(dcutimestamp), dcuhardid, avg(channel4) from cms_trk_TKCC.dcuchanneldata dd join pgroup on (dd.dcuhardid=pgroup.dcu_id)  where dcutimestamp between %s and %s and channel4 <> 0 group by dcuhardid" %(unixtime_start,unixtime_end))

            e.execute("with pgroup as ( select distinct dcu_id, detid, cable from  CMS_TRK_DCS_CONF.DCUS join cms_trk_TKCC.tk_fibers on dcu_id=tk_fibers.dcuid) Select dcutimestamp, dcuhardid, channel4, channel0 from cms_trk_TKCC.dcuchanneldata dd join pgroup on (dd.dcuhardid=pgroup.dcu_id)  where dcutimestamp between %s and %s and channel4 <> 0" %(unixtime_start,unixtime_end)) 
            
            info_qTemp = e.fetchall()
            if not info_qTemp : 
                print 'file temp vuoto'
                print run
                continue
            #save query output to numpy array
            df_Temp = pd.DataFrame(info_qTemp)
            df_Temp.columns = ['dcuTimeStamp','dcuHardId','channel4','channel0']
            df_Temp['typeGlRun'] = run[4]
            df_Temp['glRun_start'] = run[3]
            df_Temp['glRun_delta'] = run[6]
            rr_Temp = df_Temp.to_records(index=False)
            rr_Temp.dtype.names = [str(i) for i in rr_Temp.dtype.names]

            np.save(os.path.join(docs_dir,fileNameTemp),rr_Temp)
    
            print fileNameTemp, ' done...'
conn.close()
