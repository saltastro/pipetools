################################# LICENSE ##################################
# Copyright (c) 2015, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

#!/usr/bin/env python

"""
HRSADVANCE performs advanced data reduction of the SALT HRS data.  It also
updates the database with statistical information about the data

Author                 Version      Date
-----------------------------------------------
S M Crawford (SAAO)    0.1          09 Jun 2013

Updates
------------------------------------------------

Todo
------------------------------------------------

"""

from __future__ import with_statement

import sys,glob, os, shutil, time
import numpy as np

import datetime as dt


import ccdproc
from ccdproc import CCDData
from ccdproc import ImageFileCollection

from pyhrs.hrsprocess import *


debug=True

#biasheader_list=['INSTRUME', 'DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED', 'NWINDOW']
#flatheader_list=['INSTRUME', 'DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED', 'FILTER', 'GRATING', 'GR-ANGLE', 'AR-ANGLE', 'NWINDOW']

def get_obsdate(fname):
    """Return the observation date"""
    fname = os.path.basename(fname)
    return fname[1:9]

def find_bias(obsdate, prefix, bias_dir='/salt/HRS_Cals/CAL_BIAS/', nlim=180):
    """Search starting with the obsdate and moving backword in time, 
       for a bias frame
    """
    date = dt.datetime.strptime('{} 12:00:00'.format(obsdate), '%Y%m%d %H:%M:%S')
    for i in range(nlim):
        obsdate = date.strftime('%Y%m%d')
        bias_file = '{0}{1}/{2}/product/{3}BIAS_{4}.fits'.format(bias_dir, obsdate[0:4], obsdate[4:8], prefix, obsdate)
        if os.path.isfile(bias_file): return bias_file
        date = date - dt.timedelta(seconds=24*3600.0)
    return None
    

def hrsbias(rawpath, outpath, link=False, mem_limit=1e9, clobber=True):
   """hrsbias processes the HRS red and blue bias frames in a directory

   Parameters
   ----------
   rawpath: string
      Path to raw data

   outpath: string
      Path to output result

   link: boolean
      Add symbolic link to HRS_CALS directory

   clobber: boolean
      Overwrite existing files

   
   """
   if not os.path.isdir(rawpath): return 

   image_list = ImageFileCollection(rawpath)
   if len(image_list.files)==0: return

   #make output directory
   if not os.path.isdir(outpath): os.mkdir(outpath)
   
   
   obsdate=get_obsdate(image_list.summary['file'][0])
 

   #process the red bias frames
   matches = (image_list.summary['obstype'] == 'Bias') * (image_list.summary['detnam'] == 'HRDET')
   rbias_list = []
   for fname in image_list.summary['file'][matches]:
        ccd = red_process(rawpath+fname)
        rbias_list.append(ccd)
   if rbias_list:
        if os.path.isfile("{0}/RBIAS_{1}.fits".format(outpath, obsdate)) and clobber: 
            os.remove("{0}/RBIAS_{1}.fits".format(outpath, obsdate))
        rbias = ccdproc.combine(rbias_list, method='median', output_file="{0}/RBIAS_{1}.fits".format(outpath, obsdate), mem_limit=mem_limit)
        del rbias_list

   #process the red bias frames
   matches = (image_list.summary['obstype'] == 'Bias') * (image_list.summary['detnam'] == 'HBDET')
   hbias_list = []
   for fname in image_list.summary['file'][matches]:
        ccd = blue_process(rawpath+fname)
        hbias_list.append(ccd)
   if hbias_list:
        if os.path.isfile("{0}/HBIAS_{1}.fits".format(outpath, obsdate)) and clobber: 
            os.remove("{0}/HBIAS_{1}.fits".format(outpath, obsdate))
        hbias = ccdproc.combine(hbias_list, method='median', output_file="{0}/HBIAS_{1}.fits".format(outpath, obsdate), mem_limit=mem_limit)
        del hbias_list


   #provide the link to the bias frame
   if link:
       ldir = '/salt/HRS_Cals/CAL_BIAS/{0}/{1}/'.format(obsdate[0:4], obsdate[4:8])
       if not os.path.isdir(ldir): os.mkdir(ldir)
       ldir = '/salt/HRS_Cals/CAL_BIAS/{0}/{1}/product'.format(obsdate[0:4], obsdate[4:8])
       if not os.path.isdir(ldir): os.mkdir(ldir)

       infile="{0}/RBIAS_{1}.fits".format(outpath, obsdate)
       link='/salt/HRS_Cals/CAL_BIAS/{0}/{1}/product/RBIAS_{2}.fits'.format(obsdate[0:4], obsdate[4:8], obsdate)
       if os.path.isfile(link) and clobber: os.remove(link)
       os.symlink(infile, link)
       infile="{0}/HBIAS_{1}.fits".format(outpath, obsdate)
       link='/salt/HRS_Cals/CAL_BIAS/{0}/{1}/product/HBIAS_{2}.fits'.format(obsdate[0:4], obsdate[4:8], obsdate)
       if os.path.isfile(link) and clobber: os.remove(link)
       os.symlink(infile, link)


def hrsflat(rawpath, outpath, detname, obsmode, master_bias=None, link=False, clobber=True):
   """hrsflat processes the HRS flatfields.  It will process for a given detector and a mode

   Parameters
   ----------
   rawpath: string
      Path to raw data

   outpath: string
      Path to output result

   link: boolean
      Add symbolic link to HRS_CALS directory

   clobber: boolean
      Overwrite existing files

   
   """
   if not os.path.isdir(rawpath): return

   image_list = ImageFileCollection(rawpath)
   if len(image_list.files)==0: return

   #make output directory
   if not os.path.isdir(outpath): os.mkdir(outpath)

   #get the observing date
   obsdate=get_obsdate(image_list.summary['file'][0])

   #setup the instrument prefix
  
   if detname=='HRDET':
      prefix='R'
      process = red_process
   elif detname=='HBDET':
      prefix='H'
      process = blue_process
   else:
      raise ValueError('detname must be a valid HRS Detector name')

   #process the red bias frames
   matches = (image_list.summary['obstype'] == 'Flat field') * (image_list.summary['detnam'] == detname) * (image_list.summary['obsmode'] == obsmode)
   flat_list = []
   for fname in image_list.summary['file'][matches]:
        ccd = process(rawpath+fname, masterbias=master_bias)
        flat_list.append(ccd)
   if flat_list:
        outfile = "{0}/{2}FLAT_{1}_{3}.fits".format(outpath, obsdate, prefix, obsmode.replace(' ', '_'))
        if os.path.isfile(outfile) and clobber:  os.remove(outfile)
        flat = ccdproc.combine(flat_list, method='median', output_file=outfile)

        if link:
            link='/salt/HRS_Cals/CAL_FLAT/{0}/{1}/product/{2}'.format(obsdate[0:4], obsdate[4:8], os.path.basename(outfile))
            if os.path.isfile(link) and clobber: os.remove(link)
            os.symlink(outfile, link)
