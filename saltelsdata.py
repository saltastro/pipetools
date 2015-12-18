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
SALTELSDATA downloads the data from the ELS datadata and correlates
it for an observing date or a proposal or set of observations during
a night

Author                 Version      Date
-----------------------------------------------
S M Crawford (SAAO)    0.3          07 Aug 2012


UPDATES
------------------------------------------------

"""

# Ensure python 2.5 compatibility
from __future__ import with_statement

import os, string, datetime
import struct
import pyfits
import numpy as np

from pyraf import iraf

import saltsafeio as saltio
import saltsafemysql as saltmysql
from salterror import SaltError, SaltIOError

from saltsafelog import logging
debug=True

# -----------------------------------------------------------
# core routine

def saltelsdata(propcode, obsdate, elshost, elsname, elsuser, elspass, 
            sdbhost,sdbname,sdbuser, password, clobber,logfile,verbose):

   # set up

   proposers = []
   propids = []
   pids = []
   
   with logging(logfile,debug) as log:

       # are the arguments defined
       if propcode.strip().upper()=='ALL':
          pids=saltmysql.getproposalcodes(str(obsdate), sdbhost,sdbname,sdbuser, password)
       else:
          pids = saltio.argunpack('propcode',propcode)

       #open the database
       els=saltmysql.connectelsview(elshost, elsname, elsuser, elspass)
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser, password)  

       #create the values for the entire night
  
       #loop through the proposals
       for pid in pids:
           outfile='%s_%s_elsdata.fits' % (pid, obsdate)
           if clobber and os.path.isfile(outfile):
               saltio.delete(outfile)
  

           mintime, maxtime=determinetime(sdb, pid, obsdate)

           message='Extracting ELS data for %s from %s to %s' % (pid, mintime, maxtime)
           log.message(message, with_stdout=verbose)

           getelsdata(els, sdb, outfile, mintime, maxtime)

def determinetime(sdb, pid, obsdate):
   #for a proposal, retrieve the maximum and minimum  time
   sel_cmd='FileName, UTStart, ExposureTime, NExposures'
   tab_cmd='FileData join ProposalCode using (ProposalCode_Id)'
   log_cmd="FileName like '%"+str(obsdate)+"%' and Proposal_Code='"+pid+"'"
   record=saltmysql.select(sdb, sel_cmd, tab_cmd, log_cmd)

   if not record:
       raise SaltError('%s does not have any data on %s' % (pid, obsdate))

   print pid, record
   maxtime=record[0][1]
   mintime=record[0][1]
   for r in record:
       maxtime=max(maxtime, r[1]+datetime.timedelta(seconds=r[2]*r[3]))
       mintime=min(mintime, r[1])
   
   #correct the time from UTC to SAST
   mintime+=datetime.timedelta(seconds=2*3600.0)
   maxtime+=datetime.timedelta(seconds=2*3600.0)
   return mintime, maxtime
       

def getelsdata(els, sdb, outfile, mintime, maxtime, clobber=False):
   """Retrieve data for a given observation during an observation date for 
      a proposal
   """
   #create the tables
   weahdu=weathertable(els, mintime, maxtime)
   seehdu=seeingtable(sdb, mintime, maxtime)
   guihdu=guidertable(els, mintime, maxtime)

   #put all the data together
   table_list=[pyfits.PrimaryHDU()]
   for hdu in [weahdu, seehdu, guihdu]:
       if hdu is not None:
           table_list.append(hdu)

   #creat the output data
   hdulist = pyfits.HDUList(table_list)
   hdulist.writeto(outfile)
       

def weathertable(els, mintime, maxtime):
   """Creates the weather table from the data in the els
   """
   #now extact weather information from the els
   sel_cmd='_timestamp_, air_pressure, dewpoint, rel_humidity, wind_mag_30m, wind_dir_30m, wind_mag_10m, wind_dir_10m, temperatures, rain_detected'
   tab_cmd='bms_external_conditions__timestamp'
   log_cmd="_timestamp_>'%s' and _timestamp_<'%s'" % (mintime, maxtime)
   wea_rec=saltmysql.select(els, sel_cmd, tab_cmd, log_cmd)
   if len(wea_rec)<2:  return None

   time_list=[]
   air_arr=np.zeros(len(wea_rec))
   dew_arr=np.zeros(len(wea_rec))
   hum_arr=np.zeros(len(wea_rec))
   w30_arr=np.zeros(len(wea_rec))
   w30d_arr=np.zeros(len(wea_rec))
   w10_arr=np.zeros(len(wea_rec))
   w10d_arr=np.zeros(len(wea_rec))
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
       t_arr=converttemperature(wea_rec[i][8])
       t02_arr[i]=t_arr[0]       
       t05_arr[i]=t_arr[1]       
       t10_arr[i]=t_arr[2]       
       t15_arr[i]=t_arr[3]       
       t20_arr[i]=t_arr[4]       
       t25_arr[i]=t_arr[5]       
       t30_arr[i]=t_arr[6]       
   weacol=[]
   weacol.append(pyfits.Column(name='TimeStamp', format='20A', array=time_list ))
   weacol.append(pyfits.Column(name='Air Pressure', format='F', array=air_arr ))
   weacol.append(pyfits.Column(name='Dew Point', format='F', array=dew_arr ))
   weacol.append(pyfits.Column(name='Humidy', format='F', array=hum_arr ))
   weacol.append(pyfits.Column(name='Wind 30m', format='F', array=w30_arr ))
   weacol.append(pyfits.Column(name='Wind 30m Direction', format='F', array=w30d_arr ))
   weacol.append(pyfits.Column(name='Wind 10m', format='F', array=w10_arr ))
   weacol.append(pyfits.Column(name='Wind 10m Direction', format='F', array=w10d_arr ))
   weacol.append(pyfits.Column(name='Temperature  2m', format='F', array=t02_arr ))
   weacol.append(pyfits.Column(name='Temperature  5m', format='F', array=t05_arr ))
   weacol.append(pyfits.Column(name='Temperature 10m', format='F', array=t10_arr ))
   weacol.append(pyfits.Column(name='Temperature 15m', format='F', array=t15_arr ))
   weacol.append(pyfits.Column(name='Temperature 20m', format='F', array=t20_arr ))
   weacol.append(pyfits.Column(name='Temperature 25m', format='F', array=t25_arr ))
   weacol.append(pyfits.Column(name='Temperature 30m', format='F', array=t30_arr ))

   weatab= saltio.fitscolumns(weacol)
   weahdu = pyfits.new_table(weatab)
   weahdu.name='Weather'
   return weahdu

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

parfile = iraf.osfn("pipetools$saltelsdata.par")
t = iraf.IrafTaskFactory(taskname="saltelsdata",value=parfile,function=saltelsdata, pkgname='pipetools')
