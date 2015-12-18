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
SALTSLEWSTATS calculaes the slew statistics for each 
night

Author                 Version      Date
-----------------------------------------------
S M Crawford (SAAO)    0.3          07 Aug 2012


UPDATES
------------------------------------------------

"""

# Ensure python 2.5 compatibility
from __future__ import with_statement

import string, datetime
import struct
import pyfits
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import pylab as plt

from pyraf import iraf

import saltsafeio as saltio
import saltsafemysql as saltmysql
from salterror import SaltError, SaltIOError
from salttime import getnextdate

from saltsafelog import logging

debug=True


# -----------------------------------------------------------
# core routine

def saltslewstats(startdate, enddate, elshost, elsname, elsuser, elspass, 
            sdbhost,sdbname,sdbuser, password, clobber,logfile,verbose):

   # set up

   proposers = []
   propids = []
   pids = []
   
   with logging(logfile,debug) as log:

       #open the database
       els=saltmysql.connectdb(elshost, elsname, elsuser, elspass)
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser, password)  

       #loop through each night and determine the statistics for the slew times
       #for each night
       obsdate=int(startdate)
       slew_list=[]
       while obsdate < int(enddate):
           night_id=saltmysql.getnightinfoid(sdb, obsdate)

           start,end=saltmysql.select(sdb,'EveningTwilightEnd,MorningTwilightStart','NightInfo', 'NightInfo_Id=%i' % night_id)[0]
           
           tslew, nslew=slewtime(els, start, end)
           if nslew>0: 
              print obsdate, night_id, start, end, nslew, tslew/nslew
              slew_list.append([start, nslew, tslew])
           obsdate=int(getnextdate(obsdate))

       slew_arr=np.array(slew_list)
 
       days=np.zeros(len(slew_arr))
       for i in range(len(slew_arr)):
           days[i]=(slew_arr[i,0]-slew_arr[0,0]).days
       coef=np.polyfit(days, slew_arr[:,2]/slew_arr[:,1], 2)
       
       ave_date=[]
       ave_values=[]
       ave_nslews=[]
       nstep=10
       for i in np.arange(15,len(slew_arr), 2*nstep):
           ave_date.append(slew_arr[i,0])
           i1=i-nstep
           i2=min(i+nstep, len(slew_arr))
           #ave_values.append(np.median(slew_arr[i1:i2,2]/slew_arr[i1:i2,1]))
           ave_nslews.append(np.median(slew_arr[i1:i2,1]))
           ave_values.append(np.median(slew_arr[i1:i2,2])) 
           print ave_date[-1], ave_values[-1], ave_nslews[-1]

       ave_values=np.array(ave_values)
       ave_nslews=np.array(ave_nslews)
     
       mean_slew=ave_nslews.mean()
       mean_slew=11
       for i in range(len(ave_date)):
             #value is an attempt to correct for the average number of slews
             value=(ave_values[i]+30*(mean_slew-ave_nslews[i]))/mean_slew
             print ave_date[i], '%i %i %i' % (ave_values[i]/ave_nslews[i], ave_nslews[i], value)
 
       plt.figure()
       plt.plot(slew_arr[:,0], slew_arr[:,2]/slew_arr[:,1], ls='', marker='o')
       #plt.plot(slew_arr[:,0], slew_arr[:,1], ls='', marker='o')
       #plt.plot(slew_arr[:,0], slew_arr[:,2], ls='', marker='o')
       
       plt.plot(ave_date, ave_values/ave_nslews)
       #plt.plot(days, np.polyval(coef, days))
       plt.ylabel('Slew Time(s)')
       plt.xlabel('Date')
       plt.show()


           
           
def slewtime(els, start, end):
    """Process tcs information to determine the number of slews
       during the night and how much time was spend slewing
    """
    tcs_list=tcsstatus(els, start, end)

    totslew=0
    nslew=0
    for i in range(len(tcs_list)):
        d,s=tcs_list[i]
        if s.count('SLEW'):
           try:
               dn,sn=tcs_list[i+1]
           except IndexError:
               dn=end
               sn='Twilight'
           nslew+=1
           totslew+=(dn-d).seconds
           #print totslew, (dn-d).seconds
           #totslew=max(totslew, (dn-d).seconds)
           #print s, d, dn, nslew, totslew
    return totslew, nslew
  

def tcsstatus(els, start, end):
    """Determine the time spent slewing between two observations

       return a list of start of slew time, end of slew time for the times between start and end
    """
    select='_timestamp_, tcs_mode' 
    table='tcs_status__timestamp'
    logic="_timestamp_> '%s' and _timestamp_< '%s'" % (start, end)
    record=saltmysql.select(els, select, table, logic)
    return record

