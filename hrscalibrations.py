################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.    See LICENSE file for more details                #
#                                                                          #
############################################################################

#!/usr/bin/env python
"""
HRSCALIBRATIONS will set a link to all HRS calibration data for users
to be able to download.  It will also produce a log of the files
that were observed.


Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1         29  APR 2014

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

def hrscalibrations(obsdate, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', clobber=False, logfile='saltlog.log', verbose=True):
    """Seach the salt database for FITS files

    """

    with logging(logfile,debug) as log:

       #make obsdate a string if needed
       obsdate = str(obsdate)
       log.message('Sorting HRS calibration data')

       #connect to the database
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser,password)

       #first select propcodes for all HRS data from that day and check for any CAL data
       table='FileData join ProposalCode using (ProposalCode_Id)' 
       logic="FileName like 'H" + obsdate + "%'  or FileName like 'R" + obsdate + "%'"
       records = saltmysql.select(sdb, 'FileName, Proposal_Code', table, logic)

       #exit if no data was taken for HRS
       if len(records)==0: return

       #Loop through each of the files and create the directories if needed
       image_dict={}
       for filename, propid in records: 
           if  propid.count('CAL'):

               #check for directory and create structure
               caldir = '/salt/HRS_Cals/%s/' % propid
               if not os.path.isdir(caldir): os.mkdir(caldir)
               yeardir = '%s%s/' % (caldir, obsdate[0:4])
               if not os.path.isdir(yeardir): os.mkdir(yeardir)
               daydir = '%s%s/%s/' % (caldir, obsdate[0:4], obsdate[4:8])
               if not os.path.isdir(daydir): os.mkdir(daydir)
               rawdir = '%s%s/%s/raw/' % (caldir, obsdate[0:4], obsdate[4:8])
               if not os.path.isdir(rawdir): os.mkdir(rawdir)
               prodir = '%s%s/%s/product/' % (caldir, obsdate[0:4], obsdate[4:8])
               if not os.path.isdir(prodir): os.mkdir(prodir)

               #create the symlinks to the files
               infile = '/salt/data/%s/%s/hrs/raw/%s' % (obsdate[0:4], obsdate[4:8], filename)
               link = '%s%s' % (rawdir,filename)
               saltio.symlink(infile,link,clobber)

               infile = '/salt/data/%s/%s/hrs/product/mbgph%s' % (obsdate[0:4], obsdate[4:8], filename)
               link = '%smbgph%s' % (prodir,filename)
               saltio.symlink(infile,link,clobber)
               
               log.message('Copied %s to the HRS_CAL/%s directory' % (filename, propid), with_header=False, with_stdout=verbose)

               #create log of files
               image_info = get_image_info(sdb,filename)
               try:
                   image_dict[propid].append([image_info])
               except:
                   image_dict[propid] = [[image_info]]
  
       #create log of each file--currently not enough information
       #in database to do this
       nightlog='/salt/logs/sanightlogs/%s.log' % obsdate
       for k in image_dict: 
           nightlink = '/salt/HRS_Cals/%s/%s/%s/%s.log' % (k,obsdate[0:4], obsdate[4:8], obsdate)
           saltio.symlink(nightlog, nightlink, clobber)
           fout = open('/salt/HRS_Cals/%s/%s/%s/%s.log' % (k,obsdate[0:4], obsdate[4:8],k ), 'w')
           for imlist in image_dict[k]:
              for info in imlist:
                  for f in info:
                      fout.write('%20s ' % str(f))
              fout.write('\n')
           fout.close()

def get_image_info(sdb, filename):
    """Get information about the image from the database"""
    select = 'fd.FileName, hdr.PROPID, hdr.CCDTYPE, hdr.OBSMODE, hdr.DETMODE, hdr.OBJECT, hdr.TELRA, hdr.TELDEC, hdr.DATE_OBS, hdr.TIME_OBS '
    table = 'FileData as fd join FitsHeaderImage as hdr using (FileData_Id)' # join FitsHeaderHrs as hrs using (FileData_Id)'
    logic = 'fd.FileName like "%s"' % filename
    results = saltmysql.select(sdb, select, table, logic)
 
    return results[0]

# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$hrscalibrations.par")
t = iraf.IrafTaskFactory(taskname="hrscalibrations",value=parfile,function=hrscalibrations, pkgname='pipetools')
