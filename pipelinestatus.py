################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

#!/usr/bin/env python
"""
pipeline status updates the database with the current pipeline 
status 

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

def pipelinestatus(obsdate, status, message=None, rawsize=None, reducedsize=None, 
              runtime=None, emailsent=None, sdbhost='sdb.saao', sdbname='sdb',  sdbuser='', password='', 
              logfile='saltlog.log', verbose=True):
    """Update the PipelineStatistics table with the current information
       about the pipeline

    """

    with logging(logfile,debug) as log:

       #connect the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)

       #get the nightinfo_id
       night_id=saltmysql.getnightinfoid(sdb, obsdate)

       #get the status_id for the given status
       status_id=getstatusid(sdb, status)
           
       #create the insert command
       obsdate=str(obsdate)
       inst_cmd="NightInfo_Id=%s, PipelineStatus_Id=%i" % (night_id, status_id)
       if status_id>10:
          inst_cmd+=',ErrorMessage="%s"' % message
       if rawsize is not None: 
          inst_cmd+=",RawSize=%f" % rawsize
       if reducedsize is not None: 
          inst_cmd+=",ReducedSize=%f" % rawsize
       if runtime is not None:
          inst_cmd+=",PipelineRunTime=%i" % runtime
       if emailsent is not None:
          inst_cmd+=",EmailSent=%i" % emailsent 
       print inst_cmd
     
       #insert or update the pipeline
       if checktable(sdb, night_id): 
          saltmysql.update(sdb, inst_cmd, 'PipelineStatistics', 'NightInfo_Id=%i' % night_id)
          msg="Updating information for Night_Id=%i\n" % night_id
       else:
          saltmysql.insert(sdb, inst_cmd, 'PipelineStatistics')
          msg="Inserting  information for Night_Id=%i\n" % night_id
          

       #log the call
       log.message(msg+inst_cmd, with_stdout=verbose)
      
def checktable(sdb, night_id):
    record=saltmysql.select(sdb, 'NightInfo_Id', 'PipelineStatistics', 'NightInfo_Id=%i' % night_id)
    if record: return True
    return False

def getstatusid(sdb, status):
    record=saltmysql.select(sdb, 'PipelineStatus_Id', 'PipelineStatus', "PipelineStatus='%s'" % status.strip())
    if record:   return record[0][0]
    raise SaltError('%s is not a valid Pipeline Status')


if __name__=='__main__':
   import sys, getpass
   obsdate=sys.argv[1]
   passwd=getpass.getpass()
   pipelinestatus(obsdate, status='transfered', message=None, rawsize=1089, reducedsize=None, sdbhost='sdb.salt', sdbname='sdb', \
              sdbuser='pipeline', password=passwd, logfile='saltlog.log', verbose=True)
