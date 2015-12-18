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
SALTCALIBRATIONS searches the SALT Science Database for calibration data associated with a certain proposal.  Once it finds the data, it can also copy the data to the user directory of that proposal


Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1         20  FEB 2012

"""
# Ensure python 2.5 compatibility
from __future__ import with_statement


import os, time, glob, string, shutil
from pyraf import iraf
import salttime
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging
from salterror import SaltError

from findcal import create_caldict


debug=True

# -----------------------------------------------------------
# core routine

def saltcalibrations(propcode, outfile=None, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', clobber=False, logfile='saltlog.log', verbose=True):
    """Seach the salt database for FITS files

    """

    with logging(logfile,debug) as log:

       #check the outfiles
       if not saltio.checkfornone(outfile):
          outfile=None
          
       #check that the output file can be deleted
       if outfile:
           saltio.overwrite(outfile, clobber)
           fout=open(oufile, 'w')

       #connect to the database
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser,password)

       #determine the associated 
       username=saltmysql.getpiptusername(sdb, propcode)
       userdir='/salt/ftparea/%s/spst' % username
       if not os.path.exists(userdir):
          saltio.createdir(userdir)

       log.message("Will copy data to %s" % userdir)

       #Find all the data assocated with a proposal 
       cmd_select='d.FileName,d.FileData_Id,  CCDTYPE, d.DETMODE, d.OBSMODE, CCDSUM, GAINSET, ROSPEED, FILTER, GRATING, GRTILT, CAMANG, MASKID'
       cmd_table=''' FileData as d 
  left join FitsHeaderImage using (FileData_Id) 
  left join FitsHeaderRss using (FileData_Id) 
  left join ProposalCode using (ProposalCode_Id)
'''
       cmd_logic='Proposal_Code="%s" and CCDTYPE="OBJECT" and d.OBSMODE="SPECTROSCOPY"' % (propcode)

       record=saltmysql.select(sdb, cmd_select, cmd_table, cmd_logic)

       #loop through all the results and return only the Set of identical results
       caldict=create_caldict(record)

       #prepare for writing out the results
       outstr=''
       if outfile:
          fout.write(outstr+'\n')
       else:
          print outstr

       #now find all the cal_spst that have the same settings
       cmd_select='d.FileName,d.FileData_Id,  CCDTYPE, d.DETMODE, d.OBSMODE, CCDSUM, GAINSET, ROSPEED, FILTER, GRATING, GRTILT, CAMANG, MASKID'
       cmd_table=''' FileData as d
  left join FitsHeaderImage using (FileData_Id) 
  left join FitsHeaderRss using (FileData_Id) 
  left join ProposalCode using (ProposalCode_Id)
'''
       for r in caldict:
           cmd_logic="CCDSUM='%s' and GRATING='%s' and GRTILT='%s' and CAMANG='%s' and Proposal_Code='CAL_SPST'" % (caldict[r][3], caldict[r][7], caldict[r][8], caldict[r][9])
           #cmd_logic="CCDSUM='%s' and GRATING='%s' and AR_STA='%s' " % (caldict[r][3], caldict[r][7], caldict[r][9])
           log.message(cmd_logic, with_header=False)
           record=saltmysql.select(sdb, cmd_select, cmd_table, cmd_logic)
       #print record

           #write out hte results
           for r in record:
               outstr=' '.join(['%s' % x for x in r]) 
               if outfile:
                   fout.write(outstr+'\n')
               else:
                   log.message(outstr, with_header=False)

               #copy to the user directory
               cfile=makefilename(r[0])
               shutil.copy(cfile, userdir)

       #close outfile
       if outfile: fout.close()

def makefilename(filename):
    obsdate=filename[1:9]
    return '/salt/data/%s/%s/rss/product/mbxgp%s' % (obsdate[0:4], obsdate[4:8], filename)

def makelogic(logic, startdate, enddate):

   #set up the logic for different dates
   date_logic=''
   if startdate: 
      d,m,y=salttime.breakdate(startdate)
      y=str(y)
      m=string.zfill(m, 2)
      d=string.zfill(d, 2)
      date_logic += "UTStart > '%s-%s-%s 12:00:01'" % (y,m,d)
   if startdate and enddate:
      date_logic += " and "
   if enddate:
      edate=salttime.getnextdate(enddate)
      d,m,y=salttime.breakdate(str(edate))
      y=str(y)
      m=string.zfill(m, 2)
      d=string.zfill(d, 2)
      date_logic += "UTStart < '%s-%s-%s 11:59:59'"  % (y,m,d)

   if logic and date_logic:
      logic = '('+logic+')' + ' and ' + date_logic
   else:
      logic
   return logic
       
# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltcalibrations.par")
t = iraf.IrafTaskFactory(taskname="saltcalibrations",value=parfile,function=saltcalibrations, pkgname='pipetools')
