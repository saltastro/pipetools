################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

#!/usr/bin/env python
"""
ERROREMAIL sends an email message with the error information 
for the given obsdate

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

def erroremail(obsdate, server='', username='', password='', sender='', recipient='', bcc='',
              sdbhost='sdb.salt', sdbname='sdb', sdbuser='', sdbpass='', logfile='saltlog.log', verbose=True):
    """Update the PipelineStatistics table with the current information
       about the pipeline

    """

    with logging(logfile,debug) as log:

       #connect the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, sdbpass)

       #get the nightinfo_id
       night_id=saltmysql.getnightinfoid(sdb, obsdate)

       #create the message

       try:
          record=saltmysql.select(sdb, 'PipelineStatus, ErrorMessage', 'PipelineStatistics join PipelineStatus using (PipelineStatus_Id)', 'NightInfo_Id=%i' % night_id)
          status, error_message=record[0]
       except Exception, e:
          raise SaltError('Unable to download information: %s' % e)

       #set up the subject and message
       subject='Pipeline Error on %s' % obsdate
       
       message="Failure is simply the opportunity to begin again, this time more intelligently.--Henry Ford\n\n"  
       message+="%s" %  error_message
       log.message(message, with_stdout=verbose)

       #send the email
       saltio.email(server,username,password,sender,recipient,bcc, subject,message)

