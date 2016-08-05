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

from astropy.io import fits
from astropy import stats
from astropy import modeling as mod
from scipy import ndimage as nd
from pyhrs import normalize_image, create_orderframe, clean_flatimage

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



def dq_ccd_insert(filename, sdb):
    """Insert CCD information into the database 

    Parameters
    ----------
    filename: str
       Raw file name 

    sdb: sdb_user.mysql
       Connection to the sdb database
    """
    logic="FileName='%s'" % os.path.basename(filename)
    FileData_Id = sdb.select('FileData_Id','FileData',logic)[0][0]
    i = 0
    record=sdb.select('FileData_Id', 'PipelineDataQuality_CCD', 'FileData_Id=%i and Extension=%i' % (FileData_Id, i))
    update=False
    if record: update=True

    #lets measureme the statistics in a 200x200 box in each image
    struct = CCDData.read(filename, unit='adu')
    my,mx=struct.data.shape
    dx1=int(mx*0.5)
    dx2=min(mx,dx1+200)
    dy1=int(my*0.5)
    dy2=min(my,dy1+200)
    mean,med,sig=stats.sigma_clipped_stats(struct.data[dy1:dy2,dx1:dx2], sigma=5, iters=5)
    omean=None
    orms=None

    ins_cmd=''
    if omean is not None: ins_cmd='OverscanMean=%s,' % omean
    if orms is not None:  ins_cmd+='OverscanRms=%s,' % orms
    if mean is not None:  ins_cmd+='BkgdMean=%f,' % mean
    if sig is not None:   ins_cmd+='BkgdRms=%f' % sig
    if update:
       ins_cmd=ins_cmd.rstrip(',')
       sdb.update(ins_cmd, 'PipelineDataQuality_CCD', 'FileData_Id=%i and Extension=%i' % (FileData_Id, i))
    else:
       ins_cmd+=',FileData_Id=%i, Extension=%i' % (FileData_Id, i)
       sdb.insert(ins_cmd, 'PipelineDataQuality_CCD')

def dq_order_insert(filename, sdb):
    """Insert order information into the database 

    Parameters
    ----------
    filename: str
       Raw file name 

    sdb: sdb_user.mysql
       Connection to the sdb database
    """
    hdu = fits.open(filename)
    data = hdu[0].data
   
    # get the minimum and maximum order
    min_order = int(data[data>0].min())
    max_order = int(data[data>0].max())

    #the name must of a specific format
    bname = os.path.basename(filename)
    sname = bname.split('.')[0].split('_')
    obsdate = sname[1]
    mode = sname[2] + " " + sname[3]
    night_id = sdb.select('NightInfo_Id', 'NightInfo', "Date='{}-{}-{}'".format(obsdate[0:4], obsdate[4:6], obsdate[6:8]))[0][0]
    mode_id = sdb.select('HrsMode_Id', 'HrsMode', "ExposureMode = '{}'".format(mode))[0][0]
    

    ys, xs =data.shape
    xc = int(xs/2.0)  
    arr = data[:, xc]
    for i in range(min_order, max_order+1):
        mask = (arr == i)
        indices = np.where(mask)[0]
        y_lower = indices.min()
        y_upper = indices.max()
        print filename, i, y_lower, y_upper
        log_cmd = "Filename = '{}' and HrsOrder =  {}".format(bname, i)
        record = sdb.select('y_upper', 'DQ_HrsOrder', log_cmd)
        ins_cmd = "x_reference={},y_lower={},y_upper={}".format(xc, y_lower, y_upper)
        if record:
           pass #print 'update'
           sdb.update(ins_cmd, 'DQ_HrsOrder', log_cmd)
        else:
           ins_cmd = "Filename='{}', HrsOrder={},HrsMode_Id={},NightInfor_Id={},".format(bname, i, mode_id, night_id)  + ins_cmd
           #print 'insert'
           sdb.insert(ins_cmd, 'DQ_HrsOrder')
        

def hrsbias(rawpath, outpath, link=False, mem_limit=1e9, sdb=None, clobber=True):
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
        if sdb is not None: dq_ccd_insert(rawpath + fname, sdb)

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
        if sdb is not None: dq_ccd_insert(rawpath + fname, sdb)

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


def hrsflat(rawpath, outpath, detname, obsmode, master_bias=None, f_limit=1000, first_order=53, 
            y_start=30, y_limit=3920, smooth_length=20, smooth_fraction=0.4, filter_size=151,
            link=False, sdb=None, clobber=True):
   """hrsflat processes the HRS flatfields.  It will process for a given detector and a mode

   Parameters
   ----------
   rawpath: string
      Path to raw data

   outpath: string
      Path to output result

   link: boolean
      Add symbolic link to HRS_CALS directory

   sdb: sdb_user.mysql
      SDB object to upload data quality

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
        if sdb is not None: dq_ccd_insert(rawpath + fname, sdb)

   if flat_list:
        outfile = "{0}/{2}FLAT_{1}_{3}.fits".format(outpath, obsdate, prefix, obsmode.replace(' ', '_'))
        if os.path.isfile(outfile) and clobber:  os.remove(outfile)
        flat = ccdproc.combine(flat_list, method='median', output_file=outfile)

        norm = clean_flatimage(flat.data, filter_size=filter_size, flux_limit=0.3,
                block_size=100, percentile_low=30, median_size=5)
        hdu = fits.PrimaryHDU(norm)
        hdu.writeto(prefix+'norm.fits', clobber=True)

        norm[norm>0]=1
        ys, xs = norm.shape
        xc = int(xs/2.0)
        if detname=='HRDET':
             xc = 1947 #int(xs/2.0)
             ndata = norm[:,xc]
             detect_kern = ndata[1:100]
             #these remove light that has bleed at the edges and may need adjusting
             norm[:,:20]=0
             norm[:,4040:]=0
        elif detname=='HBDET':
             ndata = norm[:,xc]
             detect_kern = ndata[32:110]

        frame = create_orderframe(norm, first_order, xc, detect_kern, smooth_length=smooth_length, 
                      smooth_fraction=smooth_fraction, y_start=y_start, y_limit=y_limit)
        order_file = "{0}/{2}ORDER_{1}_{3}.fits".format(outpath, obsdate, prefix, obsmode.replace(' ', '_'))
        hdu = fits.PrimaryHDU(frame)
        hdu.writeto(prefix+'order.fits', clobber=True)
        hdu.writeto(order_file, clobber=True)

        if link:
            link='/salt/HRS_Cals/CAL_FLAT/{0}/{1}/product/{2}'.format(obsdate[0:4], obsdate[4:8], os.path.basename(outfile))
            if os.path.isfile(link) and clobber: os.remove(link)
            os.symlink(outfile, link)
            olink='/salt/HRS_Cals/CAL_FLAT/{0}/{1}/product/{2}'.format(obsdate[0:4], obsdate[4:8], os.path.basename(order_file))
            if os.path.isfile(olink) and clobber: os.remove(olink)
            os.symlink(order_file, olink)
