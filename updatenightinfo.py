################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

#!/usr/bin/env python
"""
updatenightinfo will create or update the nightinfo entry with the 
correct times for sunrise/set, twilight, and moon rise/set

Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1          25 Apr 2012 

"""
# Ensure python 2.5 compatibility
from __future__ import with_statement


import os, time, glob, string, datetime
import ephem

from pyraf import iraf
from iraf import pysalt
import salttime
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging
from salterror import SaltError


debug=True

# -----------------------------------------------------------
# core routine

def updatenightinfo(obsdate, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', logfile='saltlog.log', verbose=True):
    """Update the nightinfo table with current information about the
       night

    """

    with logging(logfile,debug) as log:

       #connect the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)

       #get the nightinfo_id
       try:
           night_id=saltmysql.getnightinfoid(sdb, obsdate)
       except SaltError:
           night_id=None
           
       #get the information for the night
       time_list=get_nightdetails(obsdate)

       #create the insert command
       obsdate=str(obsdate)
       inst_cmd="Date='%s-%s-%s'," % (obsdate[0:4], obsdate[4:6], obsdate[6:8])
       inst_cmd+="SunSet='%s', SunRise='%s', MoonSet='%s', MoonRise='%s', EveningTwilightEnd='%s', MorningTwilightStart='%s'" % \
               (time_list[0], time_list[1], time_list[2], time_list[3], time_list[4], time_list[5])
       inst_cmd+=",MoonPhase_Percent=%i" % (round(float(time_list[6])))

       if night_id: 
          saltmysql.update(sdb, inst_cmd, 'NightInfo', 'NightInfo_Id=%i' % night_id)
          msg="Updating information for Night_Id=%i\n" % night_id
       else:
          saltmysql.insert(sdb, inst_cmd, 'NightInfo')
          msg="Inserting  information for Night_Id=%i\n" % night_id
          

       #log the call
       log.message(msg+inst_cmd, with_stdout=verbose)
      


def sutherland(date=None):
    """Create an ephem.Observer object with the settings for Sutherland
    """
    Suth=ephem.Observer()
    Suth.lat='-32.3794444'
    Suth.lon='20.81069500'
    Suth.elevation=1798.0
    Suth.pressure=0
    if date:
       Suth.date=date
    return Suth

def get_nightdetails(obsdate=None):
    """Get all the night details for a given date 
    """

    #Set the date to noon on the given day
    if obsdate:
       obsdate=str(obsdate)
       date='%s/%s/%s 12:00:01' % (obsdate[0:4], obsdate[4:6], obsdate[6:8])

    #set up the obseratory
    Suth=sutherland(date)

    #now get the sunrise and sunset
    #Following the definition of USNO we set the value
    #for the horizon at -34'
    Suth.horizon='-00:34'
    sunset=ephem.localtime(Suth.next_setting(ephem.Sun()))
    sunrise=ephem.localtime(Suth.next_rising(ephem.Sun()))

    #get the moon rise and moonset
    moonset=ephem.localtime(Suth.next_setting(ephem.Moon()))
    moonrise=ephem.localtime(Suth.next_rising(ephem.Moon()))

    #calculate the moon phase
    m=ephem.Moon()
    m.compute(Suth)
    #calculate the moonphase at midnight
    if date:
       m.compute('%s/%s/%s 23:59:01' % (obsdate[0:4], obsdate[4:6], obsdate[6:8]))
    moonphase=100*m.moon_phase
    print moonphase

    #get the start/end of astronomical twilight
    Suth.horizon='-18'
    twistart=ephem.localtime(Suth.next_setting(ephem.Sun(), use_center=True))
    twiend=ephem.localtime(Suth.next_rising(ephem.Sun(), use_center=True))


    return [sunset, sunrise, moonset, moonrise, twistart, twiend, moonphase]
    #return [str(x).replace('/','-') for x  in [sunset, sunrise, moonset, moonrise, twistart, twiend, moonphase]]
       

if __name__=='__main__':
   import sys, getpass
   obsdate=sys.argv[1]
   passwd=getpass.getpass()
   print get_nightdetails(obsdate)
   updatenightinfo(obsdate, sdbhost='sdb.salt', sdbname='sdb', \
              sdbuser='pipeline', password=passwd, logfile='saltlog.log', verbose=True)
