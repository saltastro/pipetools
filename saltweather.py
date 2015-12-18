################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
# Redistribution and use in source and binary forms, with or without       #
# modification, are permitted provided that the following conditions       #
# are met:                                                                 #
#                                                                          #
#     * Redistributions of source code must retain the above copyright     #
#       notice, this list of conditions and the following disclaimer.      #
#     * Redistributions in binary form must reproduce the above copyright  #
#       notice, this list of conditions and the following disclaimer       #
#       in the documentation and/or other materials provided with the      #
#       distribution.                                                      #
#     * Neither the name of the South African Astronomical Observatory     #
#       (SAAO) nor the names of its contributors may be used to endorse    #
#       or promote products derived from this software without specific    #
#       prior written permission.                                          #
#                                                                          #
# THIS SOFTWARE IS PROVIDED BY THE SAAO ''AS IS'' AND ANY EXPRESS OR       #
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED           #
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE   #
# DISCLAIMED. IN NO EVENT SHALL THE SAAO BE LIABLE FOR ANY                 #
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL       #
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS  #
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)    #
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,      #
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN #
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE          #
# POSSIBILITY OF SUCH DAMAGE.                                              #
############################################################################

#!/usr/bin/env python
"""
SALTWEATHER updates the SDB database with weather information 
from the  ELS database.

+-------------------+------------------+------+-----+---------+-------+
| Field             | Type             | Null | Key | Default | Extra |
+-------------------+------------------+------+-----+---------+-------+
| Weather_Time      | datetime         | NO   | PRI |         |       |
| NightInfo_Id      | int(10) unsigned | YES  | MUL | NULL    |   x   |
| TemperatureInside | float            | YES  |     | NULL    |       |
| Temperature2m     | float            | YES  |     | NULL    |   x   |
| Temperature30m    | float            | YES  |     | NULL    |   x   |
| WindSpeed         | float unsigned   | YES  |     | NULL    |   x   |
| WindDirection     | float unsigned   | YES  |     | NULL    |   x   |
| DewPointInside    | float            | YES  |     | NULL    |       |
| DewPointOutside   | float            | YES  |     | NULL    |   x   |
| AirPressure       | float unsigned   | YES  |     | NULL    |   x   |
| RelativeHumidty   | float unsigned   | YES  |     | NULL    |       |
| Rain              | tinyint(1)       | YES  |     | NULL    |       |
+-------------------+------------------+------+-----+---------+-------+


Author                 Version      Date
-----------------------------------------------
S M Crawford (SAAO)    0.1          14 Aug 2013


UPDATES
------------------------------------------------

"""

# Ensure python 2.5 compatibility
from __future__ import with_statement

import string, datetime, time
import struct
import numpy as np

import saltsafeio as saltio
import saltsafemysql as saltmysql
from salterror import SaltError, SaltIOError

# -----------------------------------------------------------
# core routine

def saltweather(weathertime, timespan, elshost, elsname, elsuser, elspass, 
            sdbhost,sdbname,sdbuser, password):
   print weathertime

   #open the database
   els=saltmysql.connectdb(elshost, elsname, elsuser, elspass)
   sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser, password)  

   #determine the obsdate
   obsdate=weathertime-datetime.timedelta(seconds=43200)
   obsdate='%i%s%s' % (obsdate.year, string.zfill(obsdate.month,2), string.zfill(obsdate.day,2))
   nid=saltmysql.getnightinfoid(sdb, obsdate)

   print nid
   #get the most recent weather data
   airpressure, dewout, extrelhum, temp2m, temp30m, windspeed, winddir, rain= getweatherdata(els, weathertime, timespan)
   dewin, relhum=getinsidedata(els, weathertime, timespan)
   tempin=getacdata(els, weathertime, timespan)

   #upload that to the sdb
   upcmd="Weather_Time='%s', NightInfo_Id=%i" % (weathertime, nid)

   if tempin is not None: upcmd+=',TemperatureInside=%4.2f' % tempin
   if temp2m is not None: upcmd+=',Temperature2m=%4.2f' % temp2m
   if temp30m is not None: upcmd+=',Temperature30m=%4.2f' % temp30m
   if windspeed is not None: upcmd+=',WindSpeed=%5.2f' % windspeed
   if winddir is not None: upcmd+=',WindDirection=%5.2f' % winddir
   if dewin  is not None: upcmd+=',DewPointInside=%3.2f' % dewin 
   if dewout is not None: upcmd+=',DewPointOutside=%3.2f' % dewout
   if airpressure is not None: upcmd+=',AirPressure=%4.2f' % airpressure
   if relhum is not None: upcmd+=',RelativeHumidty=%4.2f' % relhum
   if extrelhum is not None: upcmd+=',ExternalRelativeHumidity=%4.2f' % extrelhum
   if rain   is not None: upcmd+=',Rain=%i' % rain  

   saltmysql.insert(sdb, upcmd, 'Weather')
   print upcmd
 
   return 

def getacdata(els, weathertime, timespan):
   """Determien the AC temperature
  
      weathertime should be SAST
   """
   #mktime converts weather time into seconds from UNIX epoch
   #7200 converts to UT
   #2082852000 converts from Unix Epoch to Labview Epoch of 1 Jan 1904
   etime=time.mktime(weathertime.timetuple())+2082852000-7200
   stime=etime-timespan
   #now extact weather information from the els
   sel_cmd='AVG(timestamp), AVG(actual_a_c_temperature)'
   tab_cmd='bms_status'
   log_cmd="timestamp>'%s' and timestamp<'%s'" % (stime, etime)
   wea_rec=saltmysql.select(els, sel_cmd, tab_cmd, log_cmd)
   if len(wea_rec)<1:  return None
   return wea_rec[0][1]

def getinsidedata(els, weathertime, timespan):
   """Creates the inside data from the els database 

      Weathertime should be in SAST
 
   """
   #mktime converts weather time into seconds from UNIX epoch
   #7200 converts to UT
   #2082852000 converts from Unix Epoch to Labview Epoch of 1 Jan 1904
   etime=time.mktime(weathertime.timetuple())+2082852000-7200
   stime=etime-timespan
   #now extact weather information from the els
   sel_cmd='AVG(timestamp), AVG(dew_point), AVG(rel_humidity)'
   tab_cmd='bms_internal_conditions'
   log_cmd="timestamp>'%s' and timestamp<'%s'" % (stime, etime)
   wea_rec=saltmysql.select(els, sel_cmd, tab_cmd, log_cmd)
   if len(wea_rec)<1:  return None, None
   return wea_rec[0][1], wea_rec[0][2]
   
   
   



def getweatherdata(els, weathertime, timespan):
   """Creates the weather table from the data in the els
   """
   
   etime=time.mktime(weathertime.timetuple())+2082852000-7200
   stime=etime-timespan
   #now extact weather information from the els
   sel_cmd='timestamp, air_pressure, dewpoint, rel_humidity, wind_mag_30m, wind_dir_30m, wind_mag_10m, wind_dir_10m, temperatures, rain_detected'
   tab_cmd='bms_external_conditions'
   log_cmd="timestamp>'%s' and timestamp<'%s'" % (stime, etime)
   wea_rec=saltmysql.select(els, sel_cmd, tab_cmd, log_cmd)
   if len(wea_rec)<1:  return 8*[None]

   time_list=[]
   air_arr=np.zeros(len(wea_rec))
   dew_arr=np.zeros(len(wea_rec))
   hum_arr=np.zeros(len(wea_rec))
   w30_arr=np.zeros(len(wea_rec))
   w30d_arr=np.zeros(len(wea_rec))
   w10_arr=np.zeros(len(wea_rec))
   w10d_arr=np.zeros(len(wea_rec))
   rain_list=[]
   t02_arr=np.zeros(len(wea_rec))
   t05_arr=np.zeros(len(wea_rec))
   t10_arr=np.zeros(len(wea_rec))
   t15_arr=np.zeros(len(wea_rec))
   t20_arr=np.zeros(len(wea_rec))
   t25_arr=np.zeros(len(wea_rec))
   t30_arr=np.zeros(len(wea_rec)) 


   for i in range(len(wea_rec)):
       time_list.append(str(wea_rec[i][0]))
       air_arr[i]=wea_rec[i][1]
       dew_arr[i]=wea_rec[i][2]
       hum_arr[i]=wea_rec[i][3]
       w30_arr[i]=wea_rec[i][4]
       w30d_arr[i]=wea_rec[i][5]
       w10_arr[i]=wea_rec[i][6]
       w10d_arr[i]=wea_rec[i][7]
       rain_list.append(wea_rec[i][9])
       t_arr=converttemperature(wea_rec[i][8])
       t02_arr[i]=t_arr[0]       
       t05_arr[i]=t_arr[1]       
       t10_arr[i]=t_arr[2]       
       t15_arr[i]=t_arr[3]       
       t20_arr[i]=t_arr[4]       
       t25_arr[i]=t_arr[5]       
       t30_arr[i]=t_arr[6]       
   #average the wind direction by taking the arctan of the average of the sin and cos
   wdir=np.degrees(np.arctan2(np.sin(np.radians(w30d_arr)).mean(),np.cos(np.radians(w30d_arr)).mean()))
   if wdir<0: wdir+=360

   #determine if there was any rain during the period
   rain=0
   if 'T' in rain_list: rain=1

   return air_arr.mean(), dew_arr.mean(), hum_arr.mean(), t02_arr.mean(), t30_arr.mean(), w30_arr.mean(), wdir, rain



def converttemperature(tstruct, nelements=7):
    t_arr=np.zeros(nelements)
    for i in range(nelements):
        t_arr[i]=float(struct.unpack('>d', tstruct[4+8*i:4+8*(i+1)])[0])
    return t_arr

