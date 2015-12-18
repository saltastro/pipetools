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
SALTNIGHTSTATS creates a webpage to display stats from the observations
of a given night 

Author                 Version      Date
-----------------------------------------------
S M Crawford (SAAO)    0.3          03 Mar 2013


UPDATES
------------------------------------------------

"""

# Ensure python 2.5 compatibility
from __future__ import with_statement

import string, datetime
import struct
import pyfits
import numpy as np

from pyraf import iraf

import saltsafeio as saltio
import saltsafemysql as saltmysql
from salterror import SaltError, SaltIOError

from saltelsdata import weathertable, seeingtable, converttemperature, guidertable

from saltsafelog import logging

import pylab as pl
from matplotlib import dates

debug=True

# -----------------------------------------------------------
# core routine

def saltnightstats(obsdate, outfile, elshost, elsname, elsuser, elspass, 
            sdbhost,sdbname,sdbuser, password, clobber,logfile,verbose):

   # set up

   proposers = []
   propids = []
   pids = []
   
   with logging(logfile,debug) as log:

       #open the database
       els=saltmysql.connectdb(elshost, elsname, elsuser, elspass)
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser, password)  

       #create the values for the entire night
       obsdate=str(obsdate)

       #create the night stats for the 
       makenightstats(els, sdb, outfile, obsdate, clobber=clobber)

  
def makenightstats(els, sdb, outfile, obsdate, clobber=False):
   """Retrieve data for a given observation during an observation date for 
      a proposal
   """
   fout=saltio.openascii(outfile, 'w')
 

   headerstr="""<html>
<head><title>SALT Night Report for %s</title></head>
<body bgcolor="white" text="black" link="blue" vlink="blue">

<center><h1>
SALT Night Report for %s<br>
</h1></center>
"""  % (obsdate, obsdate)
   fout.write(headerstr)


   #set up the Observing Statistics
   fout.write('<h2> A. Observing Information </h2>\n')
   nid=saltmysql.getnightinfoid(sdb, obsdate)
   print nid
   #get duty information
   selcmd='sa.Surname,  so.Surname,  ct.surname' 
   tabcmd='NightInfo join SaltOperator as so on (SO1_Id=SO_Id) join Investigator as sa on (SA_Id=sa.Investigator_Id) join Investigator as ct on (CTDuty_Id=ct.Investigator_Id)'
   record=saltmysql.select(sdb, selcmd, tabcmd, 'NightInfo_Id=%i' % nid)
   try:
      sa,so,ct=record[0]
      dutystr='SA: %s <br>\nSO: %s <br> \nCT: %s <br>\n<br>' % (sa, so,ct)
   except:
      dutystr='SA: %s <br>\n SO: %s <br> \n CT: %s <br>\n<br>' % ('', '', '')
   fout.write(dutystr)

   #get night time information
   selcmd='SunSet, SunRise, MoonSet, MoonRise, MoonPhase_Percent, EveningTwilightEnd, MorningTwilightStart' 
   record=saltmysql.select(sdb, selcmd, 'NightInfo', 'NightInfo_Id=%i' % nid)[0]
   statlist=['Sun Set', 'Sun Rise', 'Moon Set', 'Moon Rise', 'Moon Phase', 'Evening Twilight', 'Morning Twilight'] 

   statstr='<table border=1><tr><th colspan=2>Nighttime Statistics </th></tr>\n'
   for s,r in zip (statlist, record):
       statstr+='<tr><td>%20s</td><td> %s </td></tr>\n' % (s, r)
   statstr+='</table><br>\n'
   fout.write(statstr)
   mintime=record[0]-datetime.timedelta(seconds=1*3600)
   maxtime=record[1]+datetime.timedelta(seconds=1*3600)
   
   obsstatlist=['Science Time', 'Engineering Time', 'Lost to Weather', 'Lost to Problems']
   selcmd='ScienceTime, EngineeringTime, TimeLostToWeather, TimeLostToProblems'
   record=saltmysql.select(sdb, selcmd, 'NightInfo', 'NightInfo_Id=%i' % nid)[0]
   obsstatstr='<table border=1><tr><th colspan=2>Observing Statistics </th></tr>\n'
   for s,r in zip (obsstatlist, record):
       obsstatstr+='<tr><td>%20s</td><td> %s </td></tr>\n' % (s, r)
   obsstatstr+='</table><br>\n'
   fout.write(obsstatstr)

   #Set up the Envirnmental Statistics
   fout.write('<h2> B. Environmental Statistics </h2>\n')

   #create the tables
   print mintime, maxtime
   #guihdu=guidertable(els, mintime, maxtime)

   #temperature plot
   fout.write('<table>')
   weahdu=weathertable(els, mintime, maxtime)
   tempplot=temperatureplot(weahdu, mintime, maxtime, obsdate)
   fout.write('<tr><td><img width=700 height=200 src=%s></td></tr>\n' % os.path.basename(tempplot))
   windfile=windplot(weahdu, mintime, maxtime, obsdate)
   fout.write('<tr><td><img width=700 height=200 src=%s></td></tr>\n' % os.path.basename(windfile))

   #seeing plot
   seeplot=makeseeingplot(sdb, mintime, maxtime, obsdate)
   fout.write('<tr><td><img width=700 height=200 src=%s><br></td></tr>\n' % os.path.basename(seeplot))

   fout.write('</table>\n')


   #Set up the Pipeline Statistics
   fout.write('<h2> C. Pipeline Statistics </h2>\n')

   fout.write('<table>\n')
   record=saltmysql.select(sdb, 'PipelineStatus, RawSize, ReducedSize, PipelineRunTime', 'PipelineStatistics join PipelineStatus using (PipelineStatus_Id)', 'NightInfo_Id=%i' % nid)[0]
   print record
   print record[1]/3600.0
   pipelinestatus='Completed'
   fout.write('<tr><td>Pipeline Status:</td><td>%s</td></tr>' % record[0])
   fout.write('<tr><td>Raw Data:</td><td>%3.2f Gb</td></tr>' % (record[1]/1e9))
   fout.write('<tr><td>Reduced Data:</td><td>%3.2f Gb</td></tr>' % (record[2]/1e9))
   fout.write('<tr><td>Run Time:</td><td>%3.2f min</td></tr>' % (record[3]/60.0))
   fout.write('</table>\n')

   #Set up the Proposal/Block Statistics
   fout.write('<h2> D. Proposal Statistics </h2>\n')
   selcmd='Proposal_Code, Block_Name, Accepted, ObsTime, BlockRejectedReason_Id, BlockVisit_Id' 
   tabcmd='Block join BlockVisit as bv using (Block_Id) join Proposal using (Proposal_Id) join ProposalCode using (ProposalCode_Id)'
   logcmd='NightInfo_Id=%i' % nid
   record=saltmysql.select(sdb, selcmd, tabcmd, logcmd)
   print record
   fout.write('<table border=1>\n')
   fout.write('<tr><th>Proposal</th><th>Block</th><th>Obs Time</th><th>Accepted?</th><th>Rejected Reason</th></tr>\n')
   for r in record:
       if r[2]>=1: 
          accept='Yes'
          reason=''
       else:
          accept='No'
          print r
          reason=saltmysql.select(sdb, 'RejectedReason', 'BlockRejectedReason', 'BlockRejectedReason_Id=%i' % int(r[4]))[0][0]
       bstr='<tr><td><a href="https://www.salt.ac.za/wm/proposal/%s/">%s</a></td><td>%s</td><td>%3.2f</td><td>%s</td><td>%s</td></tr>\n' % (r[0], r[0], r[1], float(r[3])/3600.0, accept, reason)
       fout.write(bstr)
   fout.write('</table>\n')

   #Set up the Data Quality Statistics
   fout.write('<h2> E. Data Quality </h2>\n')


   fout.write('</body> \n</hmtl>')
   fout.close()

def temperatureplot(weahdu, mintime, maxtime, obsdate):
   """Make the temperature plot"""
   tempplot=os.path.dirname(outfile)+'/temp_%s.png' % obsdate

   t_charr=weahdu.data['TimeStamp']
   t_list=[]

   for i in range(len(t_charr)):
       t_list.append(datetime.datetime.strptime(t_charr[i], '%Y-%m-%d %H:%M:%S'))

   
   pl.figure(figsize=(8, 2))
   ax=pl.axes([0.1,0.05,0.85,0.90])
   ax.plot(t_list, weahdu.data['Temperature  5m'], ls='-', color='#0000FF')
   ax.plot(t_list, weahdu.data['Temperature 15m'], ls='-', color='#00FF00')
   ax.plot(t_list, weahdu.data['Temperature 30m'], ls='-', color='#FF0000')
   ax.set_ylabel('Temperature(C)')
   ax.set_xticklabels([])
   ax.set_xlim([mintime, maxtime])


   #pl.xticks(rotation='vertical')
   pl.savefig(tempplot)

   return tempplot 

# -----------------------------------------------------------
   
def windplot(weahdu, mintime, maxtime, obsdate):
   windfile=os.path.dirname(outfile)+'/wind_%s.png' % obsdate

   t_charr=weahdu.data['TimeStamp']
   t_list=[]

   for i in range(len(t_charr)):
       t_list.append(datetime.datetime.strptime(t_charr[i], '%Y-%m-%d %H:%M:%S'))
   pl.figure(figsize=(8, 2))
   ay=pl.axes([0.1,0.05,0.85,0.90])
   ay.plot(t_list, weahdu.data['Wind 30m'], ls='-', color='#FF0000')
   ay.set_ylabel('Wind Speed', color='#FF0000')
   ay.set_xlim([mintime, maxtime])
   ay.set_xticklabels([])
   for tl in ay.get_yticklabels(): tl.set_color('#FF0000')
   ay2 = ay.twinx()
   ay2.plot(t_list, weahdu.data['Wind 30m Direction'], ls='-', color='#0000FF')
   ay2.set_ylabel('Wind Direction', color='#0000FF')
   ay2.set_ylim([1,359])
   ay2.set_xticklabels([])
   ay.set_xlim([mintime, maxtime])
   for tl in ay2.get_yticklabels(): tl.set_color('#0000FF')

   hfmt = dates.DateFormatter('%H:%M')
   #ax.xaxis.set_major_formatter(hfmt)
   #ax.set_xlabel('Time (Hours)')

   #pl.xticks(rotation='vertical')
   pl.savefig(windfile)

   return windfile 

def makeseeingplot(sdb, mintime, maxtime, obsdate):
   """Make the seeing plot"""
   seehdu=seeingtable(sdb, mintime-datetime.timedelta(seconds=2*3600), maxtime)
   seeplot=os.path.dirname(outfile)+'/see_%s.png' % obsdate

   t_charr=seehdu.data['TimeStamp']
   t_list=[]

   for i in range(len(t_charr)):
       t_list.append(datetime.datetime.strptime(t_charr[i], '%Y-%m-%d %H:%M:%S')+datetime.timedelta(seconds=2*3600))
   
   pl.figure(figsize=(8, 2))
   ax=pl.axes([0.1,0.15,0.85,0.80])
   ax.plot(t_list, seehdu.data['DIMM'], ls='', marker='o', ms=5)
   ax.set_xlim([mintime, maxtime])
   #hfmt = dates.DateFormatter('%H:%M')
   #ax.xaxis.set_major_formatter(hfmt)
   #pl.xlabel('Time (Hours)')
   ax.set_xticklabels([])
   pl.ylabel('Seeing (")')
   #pl.xticks(rotation='vertical')
   pl.savefig(seeplot)

   return seeplot 

# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltnightstats.par")
t = iraf.IrafTaskFactory(taskname="saltnightstats",value=parfile,function=saltnightstats, pkgname='pipetools')
