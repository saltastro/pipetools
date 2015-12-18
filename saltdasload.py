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

# Author                 Version      Date         Comment
# -----------------------------------------------------------------------
# S M Crawford (SAAO)    0.3          10 Dec 2008

# saltsdbloadfits adds or updates a fitsdata record in the science database
#
# Limitations
# --Has to be in the directory to work properly

# Ensure python 2.5 compatibility
from __future__ import with_statement


import os, time, glob, string
from pyraf import iraf
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging
from salterror import SaltError
from salttime import dec2sex

from datetime import date


debug=True

# -----------------------------------------------------------
# core routine

def saltdasload(obsdate, sdbhost, sdbname, sdbuser, password, dashost, dasname, dasuser, daspassword, logfile, verbose):
    """Upload data from the science database to the SALTDAS database

    """

    with logging(logfile,debug) as log:

       #open the science database
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser,password)

       #open saltdas
       das=saltmysql.connectdb(dashost,dasname,dasuser,daspassword)
       select='FileData_Id, FileName, Proposal_Code'
       table='FileData join ProposalCode using (ProposalCode_Id)'
       logic="FileName like '%"+str(obsdate)+"%'"
       record=saltmysql.select(sdb, select, table, logic)

       for r in record[:]:
           if not isproprietary(r[0], sdb):
              print r
              loaddata(r[0], r[1], sdb, das)
       return

def loaddata(fid, filename, sdb, das):
   """Load the data from the sdb to the das database for file with filedata_id given by fid"""
   print fid

   #load the data in filed data
   select="FileData_Id, UTStart, Target_Name, ExposureTime, FileName, INSTRUME, OBSMODE, DETMODE, Proposal_Code, Filter, TelRa, TelDec, FileName, FileSize"
   table="FileData join ProposalCode using (ProposalCode_Id)"
   if filename.startswith('S'):
       table += "join FitsHeaderSalticam using (FileData_Id)"
   elif filename.startswith('P'):
       table += "join FitsHeaderRss using (FileData_Id)"
   logic="FileData_ID=%i" %  fid
   record = saltmysql.select(sdb, select, table, logic)
   print '%s' % record[0][1]
   record=record[0]
   insert="filedata_id=%i, UTSTART='%s', object='%s', exposuretime=%f, filename='%s', instrume='%s', obsmode='%s', detmode='%s', proposal_id='%s', filter='%s'" % \
          (record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9])
   insert+=",ra='%s'" % (dec2sex(record[10]/15.0))
   insert+=",decl='%s'" % (dec2sex(record[11]))
   insert+=",filepath='%s', filesize=%f" % (makepath(filename), record[13])
   print insert
   #ra='%s', decl='%s'" % record[0]
   #filepath='%s', filesize=%f


   #load the data in fisheaderimage

   #load the data in 

def makepath(filename):
    return filename


def isproprietary(fid, sdb):
    """Determine if a data set is proprietary"""

    select='FileData_Id, FileName, Proposal_Code, ReleaseDate'
    table='FileData join ProposalCode using (ProposalCode_Id) join Proposal using (ProposalCode_Id)'
    logic="FileData_Id='%i' and current=1" % fid
    record=saltmysql.select(sdb, select, table, logic)

    #if no proposal exists, it isn't proprietary
    if not record: return False

    #if the release date is less than today, it isn't propietary
    releasedate=record[0][3]
    if releasedate<date.today(): return False

    #default is it is proprietary
    return True
  


# -----------------------------------------------------------
# main code

#parfile = iraf.osfn("pipetools$saltsdbloadfits.par")
#t = iraf.IrafTaskFactory(taskname="saltsdbloadfits",value=parfile,function=saltsdbloadfits, pkgname='pipetools')
