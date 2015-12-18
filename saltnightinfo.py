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
SALTNIGHTINFO updates the night information from the sommi log.

Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1          20 Oct 2011 

"""
# Ensure python 2.5 compatibility
from __future__ import with_statement


import os, time, glob, string, datetime
import salttime
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging
from salterror import SaltError


debug=True

# -----------------------------------------------------------
# core routine

def saltnightinfo(obsdate, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', clobber=False, logfile='saltlog.log', verbose=True):
    """Update the nightinfo table from the SOMMI log

    """

    with logging(logfile,debug) as log:

       #connect the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)

       #get the nightinfo_id
       night_id=saltmysql.getnightinfoid(sdb, obsdate)
       print night_id
       
       #get the start and end of twilight
       nightstart=saltmysql.select(sdb, 'EveningTwilightEnd', 'NightInfo', 'NightInfo_Id=%i' % night_id)[0][0]
       nightend=saltmysql.select(sdb, 'MorningTwilightStart', 'NightInfo', 'NightInfo_Id=%i' % night_id)[0][0]
       print nightstart
       print nightend
       print (nightend-nightstart).seconds

       #download the SOMMI log from the night
       try:
           sommi_log=saltmysql.select(sdb, 'SONightLog', 'NightLogs', 'NightInfo_Id=%i' % night_id)[0][0]
       except:
           raise SaltError('Unable to read SOMMI log in the database for %s' % obsdate)

       #set up the time for the night

       try:
           ntime=(nightend-nightstart).seconds
       except:
           raise SaltError('Unable to read night length from database for %s' % obsdate)

       #parse the sommi log
       slog=sommi_log.split('\n')
       stime=0
       for i in range(len(slog)):
           if slog[i].count('Science Time:'): stime=extractinformation(slog[i])
           if slog[i].count('Engineering Time:') and not slog[i].count('Non-observing Engineering Time'): etime=extractinformation(slog[i])
           if slog[i].count('Time lost to Weather:'): wtime=extractinformation(slog[i])
           if slog[i].count('Time lost to Tech. Problems:'): ttime=extractinformation(slog[i])
           if slog[i].count('Non-observing Engineering Time:'): ltime=extractinformation(slog[i])
       print etime
       tot_time=stime+etime+wtime+ttime
       print night_id, ntime, stime, etime, wtime, ttime, ltime, tot_time


       #insert the information into the database
       print tot_time, ntime
       if abs(tot_time-ntime) < 900:
           message='Updating NightInfo Table with the following Times:\n'
           message+='Science Time=%i\n' % stime
           message+='Engineeringh=%i\n' % etime
           message+='Time lost to Weather=%i\n' % wtime
           message+='Time lost to Tech. Problems=%i\n' % ttime
           message+='Non-observing Engineering Time=%i\n' % ltime
           log.message(message)
 
           insert_state='ScienceTime=%i, EngineeringTime=%i, TimeLostToWeather=%i, TimeLostToProblems=%i, NonObservingEngineeringTime=%i'  % (stime, etime, wtime,ttime, ltime)
           table_state='NightInfo'
           logic_state='NightInfo_Id=%i' % night_id
           saltmysql.update(sdb, insert_state, table_state, logic_state)
       else:
          message='The total time for the night is not equal to the length of the night\n'
          message+='Night Length=%i\n--------------------\n' % ntime
          message+='Science Time=%i\n' % stime
          message+='Engineeringh=%i\n' % etime
          message+='Time lost to Weather=%i\n' % wtime
          message+='Time lost to Tech. Problems=%i\n' % ttime
          message+='Non-observing Engineering Time=%i\n' % ltime
          message+='Total time for the Night=%i\n' % tot_time
          log.message(message)

       
def extractinformation(line):
   '''Find the number of seconds used in the time string'''
   stime=0
   lines=line.split(':')
   try:
       tstr=lines[1].replace('h', ' ')
       tstr=tstr.replace('m', ' ')
       tstr=tstr.split()
       if len(tstr)==2:
           stime=int(tstr[0])*3600+int(tstr[1])*60
       elif len(tstr)==1:
           stime=int(tstr[0])*3600
       print len(tstr), stime
       
   except Exception, e:
       print e
       stime=0
   print line.strip(), stime
   return stime


