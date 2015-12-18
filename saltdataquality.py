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

# Author                 Version      Date
# -----------------------------------------------
# S. M. Crawford (SAAO)    1.0        19 Oct 2007

# saltdataquality measures the data quality for a given night 
# and generates a number of statistics about the quality of the 
# night
# 

#Updates
#6 Apr 2011   Updated to use the new io tools

# Ensure python 2.5 compatibility
from __future__ import with_statement


from pyraf import iraf
import saltprint, salttime, saltstring
import tarfile, glob, os, ftplib

import saltsafeio as saltio
import saltsafemysql as saltmysql

from saltsafelog import logging
from salterror import SaltError
from salttime import getnextdate
from saltquery import makelogic 

from reducespst import reducespst


debug=True


# -----------------------------------------------------------
# core routine

def saltdataquality(obsdate, sdbhost = "sdb.saao", sdbname = "sdb", sdbuser = "", 
                    password = '', clobber=False, logfile='salt.log',verbose=True):
   """For a given SALT data.  Move the data to /salt/data after it has been run by the pipeline"""
   #make a dataquality directory
   dqdir='dq%s' % obsdate
   if os.path.isdir(dqdir) and clobber:
      saltio.deletedir(dqdir)
   saltio.createdir(dqdir)
   saltio.changedir(dqdir)

   with logging(logfile,debug) as log:

       #check the entries
       saltio.argdefined('obsdate',str(obsdate))

       #open up the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)


       #run for each instrument
       for instrume in ['rss', 'scam']:
           log.message('Measuring Data Quality for %s observations' % instrume.upper())
           dataquality(str(obsdate), sdb, instrume, clobber, logfile, verbose)

def dataquality(obsdate, sdb, instrume, clobber=True, logfile=None, verbose=True):
   """Run the data quality measurement for each instrument
   """
   if instrume=='rss':
      instrumetable='FitsHeaderRss'
   elif instrume=='scam':
      instrumetable='FitsHeaderSalticam'
 
   #Look for any biases from the night
   sel_cmd='FileName'
   tab_cmd='FileData join FitsHeaderImage using (FileData_Id) join %s using (FileData_Id)' % instrumetable
   logic_cmd=makelogic("CCDTYPE='BIAS'", obsdate, getnextdate(obsdate))
   record=saltmysql.select(sdb, sel_cmd, tab_cmd, logic_cmd)
   print record



   #Look for any flatfields from the night
   sel_cmd='FileName'
   tab_cmd='FileData join FitsHeaderImage using (FileData_Id) join %s using (FileData_Id)' % instrumetable
   logic_cmd=makelogic("CCDTYPE='FLAT'", obsdate, getnextdate(obsdate))
   record=saltmysql.select(sdb, sel_cmd, tab_cmd, logic_cmd)
   print record

   #Look for any Cal_SPST stars
   sel_cmd='FileName'
   tab_cmd='FileData join ProposalCode using (ProposalCode_Id) join FitsHeaderImage using (FileData_Id) join %s using (FileData_Id)' % instrumetable
   logic_cmd=makelogic("CCDTYPE='OBJECT' and Proposal_Code='CAL_SPST'", obsdate, getnextdate(obsdate))
   record=saltmysql.select(sdb, sel_cmd, tab_cmd, logic_cmd)
   print record

   #reduce each of the spectrophotometric standards
   if len(record):
       for infile in record:
           profile='/salt/data/%s/%s/%s/product/mbxgp%s' % (obsdate[0:4], obsdate[4:8], instrume, infile[0])
           reducespst(profile, obsdate, clobber=clobber, logfile=logfile, verbose=verbose)



# -----------------------------------------------------------
# main code

#parfile = iraf.osfn("pipetools$saltarchive.par")
#t = iraf.IrafTaskFactory(taskname="saltarchive",value=parfile,function=saltarchive, pkgname='pipetools')
