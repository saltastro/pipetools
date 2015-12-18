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
SALTELSERROR compiles a list of errors that occurred during a
given night

Author                 Version      Date
-----------------------------------------------
S M Crawford (SAAO)    0.3          18 Apr 2013


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

from saltsafelog import logging
debug=True

error_list=[ ' internal_dewpoint           ', ' external_dewpoint           ', ' external_relative_humidity  ', ' external_wind_speed         ', ' rain                        ', ' igloo_temps                 ', ' fine_igloo_setpoint         ', ' interlock_panel             ', ' sun                         ', ' estop                       ', ' fire                        ', ' generator_output            ', ' instrument_air              ', ' normal_air                  ', ' chamber_lights              ', ' mirror_travel_range         ', ' dome_shutter_local_operation', ' dome_shutter_locked_out     ', ' generator_fault             ', ' bms_init                    ', ' dome_init                   ', ' pmas_init                   ', ' payload_init                ', ' scam_init                   ', ' structure_init              ', ' tracker_init                ', ' ccas_obscure                ', ' ccas_louvres                ', ' track_louvres               ', ' ccas_dome                   ', ' weather_data                ', ' sommi_comms                 ', ' sammi_comms                 ', ' els_comms                   ', ' eds_comms                   ', ' glycol_tank_temp            ', ' standby_with_shutter_open   ', 'automatic_cooling_not_active ']

# -----------------------------------------------------------
# core routine

def saltelserror(obsdate, elshost, elsname, elsuser, elspass, 
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
       nid=saltmysql.getnightinfoid(sdb, obsdate)
       stime, etime=saltmysql.select(sdb, 'EveningTwilightEnd,MorningTwilightStart', 'NightInfo', 'NightInfo_Id=%i' % nid)[0]
       print stime, etime

       errors=gettcserrors(els, stime, etime)
       if not errors: return None

       for e in errors:
           i=e.index('T')
           print i, error_list[i], e[-1]
       
       print len(errors)

def gettcserrors(els, mintime, maxtime):
   """Retrieve data for a given observation during an observation date for 
      a proposal
   """
   #now extact weather information from the els
   sel_cmd='*'
   tab_cmd='tcs_system_errors__timestamp'
   log_cmd="_timestamp_>'%s' and _timestamp_<'%s'" % (mintime, maxtime)
   tcs_rec=saltmysql.select(els, sel_cmd, tab_cmd, log_cmd)

   return tcs_rec

def seeingtable(sdb, mintime, maxtime):
   #extract the seeing data from the sdb
   sel_cmd='DateTime, Mass, Dimm'
   tab_cmd='MassDimm'
   log_cmd="DateTime>'%s' and DateTime<'%s'" % (mintime, maxtime)
   see_rec=saltmysql.select(sdb, sel_cmd, tab_cmd, log_cmd)
   if len(see_rec)<2:  return None

   stime_list=[]
   mass_arr=np.zeros(len(see_rec))
   dimm_arr=np.zeros(len(see_rec))
   for i in range(len(see_rec)):
       stime_list.append(see_rec[i][0])
       mass_arr[i]=see_rec[i][1]
       dimm_arr[i]=see_rec[i][2]
 
   seecol=[]
   seecol.append(pyfits.Column(name='Timestamp', format='20A', array=stime_list))
   seecol.append(pyfits.Column(name='MASS', format='F', array=mass_arr ))
   seecol.append(pyfits.Column(name='DIMM', format='F', array=dimm_arr ))

   seetab= saltio.fitscolumns(seecol)
   seehdu= pyfits.new_table(seetab)
   seehdu.name='Seeing'
   return seehdu

def guidertable(els, mintime, maxtime):
   """Extract the guider data from the els"""
   #extract the guider data from the els
   sel_cmd='_timestamp_, guidance_available, ee50, mag50'
   tab_cmd='tpc_guidance_status__timestamp '
   log_cmd="_timestamp_>'%s' and _timestamp_<'%s'" % (mintime, maxtime)
   gui_rec=saltmysql.select(els, sel_cmd, tab_cmd, log_cmd)
   if len(gui_rec)<2:  return None


   gtime_list=[]
   ee50_arr=np.zeros(len(gui_rec))
   mag50_arr=np.zeros(len(gui_rec))
   avail_list=[]
   for i in range(len(gui_rec)):
       gtime_list.append(gui_rec[i][0])
       ee50_arr[i]=gui_rec[i][2]
       mag50_arr[i]=gui_rec[i][3]
       avail_list.append(gui_rec[i][1])
   avail_arr=(np.array(avail_list)=='T')

   #write the results to a fits table
   guicol=[]
   guicol.append(pyfits.Column(name='Timestamp', format='20A', array=gtime_list))
   guicol.append(pyfits.Column(name='Available', format='L', array=avail_arr ))
   guicol.append(pyfits.Column(name='EE50', format='F', array=ee50_arr ))
   guicol.append(pyfits.Column(name='mag50', format='F', array=mag50_arr ))

   guitab= saltio.fitscolumns(guicol)
   guihdu= pyfits.new_table(guitab)
   guihdu.name='Guider'
   return guihdu


def converttemperature(tstruct, nelements=7):
    t_arr=np.zeros(nelements)
    for i in range(nelements):
        t_arr[i]=float(struct.unpack('>d', tstruct[4+8*i:4+8*(i+1)])[0])
    return t_arr

# -----------------------------------------------------------
# main code

