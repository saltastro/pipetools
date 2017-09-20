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
import pickle
import numpy as np

import logging

import datetime as dt

from astropy.io import fits
from astropy import stats
from astropy import modeling as mod
from astropy import units as u
from scipy import ndimage as nd
from pyhrs import normalize_image, create_orderframe, clean_flatimage

import ccdproc
from ccdproc import CCDData
from ccdproc import ImageFileCollection

from pyhrs.hrsprocess import *
from pyhrs import mode_setup_information, HRSOrder, collapse_array
from pyhrs import normalize_spectra, stitch_spectra, resample, calculate_velocity, convert_data
from pyhrs import extract_order, write_spdict

from specreduce import WavelengthSolution
from specreduce import match_probability, ws_match_lines

from astroscrappy import detect_cosmics


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


def get_hrs_calibration_frame(obsdate, prefix, cal_type='BIAS', mode=None, cal_dir='/salt/HRS_Cals/', nlim=180):
    """Search starting with the obsdate and moving backword in time, 
       for a calibration frame

    Parameters
    ----------
    obsdate: str
       Observing date in YYYYMMDD
   
    prefix: str 
       Prefix for the instrument
   
    cal_type: str 
       Calibration type for the calibration frame: 'BIAS', 'FLAT', 'ORDER', 'ARC'

    mode: str or None
       If None, then no mode is added (appropriate for BIAS frames).  Other it should be the
       observing mode used

    cal_dir: str
       Directory to calibration data

    nlim: int
       Number of days to go back to check for observations

    Returns
    -------
    cal_file: string
       Name and path to appropriate calibration file

    """
    start_date = dt.datetime.strptime('{} 12:00:00'.format(obsdate), '%Y%m%d %H:%M:%S')
    if mode is not None:
         mode = '_'+ mode
    else:
         mode = ''

    cfile = 'hrs/product/{prefix}{cal_type}_{year}{mmdd}{mode}.fits'.format(
               cal_type=cal_type, year=obsdate[0:4], mmdd=obsdate[4:8], prefix=prefix, mode=mode.replace(' ', '_'))
    if os.path.isfile(cfile): return cfile

    def get_name(start_date, i, cal_dir, cal_type, prefix, mode):
        date = start_date + dt.timedelta(seconds=i*24*3600.0)
        obsdate = date.strftime('%Y%m%d')
        if cal_type == 'ORDER': 
            cals = 'FLAT'
        else: 
            cals = cal_type
        cfile = '{cal_dir}CAL_{cals}/{year}/{mmdd}/product/{prefix}{cal_type}_{year}{mmdd}{mode}.fits'.format(
           cal_dir=cal_dir, cal_type=cal_type, year=obsdate[0:4], mmdd=obsdate[4:8], prefix=prefix, mode=mode, cals = cals)
        if os.path.isfile(cfile): return cfile
        return None 

    for i in range(nlim):
        #look into the future
        cfile = get_name(start_date, i,  cal_dir, cal_type, prefix, mode)
        if cfile: return cfile
        #look into the past
        cfile = get_name(start_date, -i,  cal_dir, cal_type, prefix, mode)
        if cfile: return cfile
  
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
    logging.info('Uploading HRS Order Measurements for {}'.format(filename))
    for i in range(min_order, max_order+1):
        mask = (arr == i)
        indices = np.where(mask)[0]
        y_lower = indices.min()
        y_upper = indices.max()
        log_cmd = "Filename = '{}' and HrsOrder =  {}".format(bname, i)
        record = sdb.select('y_upper', 'DQ_HrsOrder', log_cmd)
        ins_cmd = "x_reference={},y_lower={},y_upper={}".format(xc, y_lower, y_upper)
        if record:
           sdb.update(ins_cmd, 'DQ_HrsOrder', log_cmd)
        else:
           ins_cmd = "Filename='{}', HrsOrder={},HrsMode_Id={},NightInfo_Id={},".format(bname, i, mode_id, night_id)  + ins_cmd
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
           if os.path.islink(link) and clobber: os.remove(link)
           os.symlink(infile, link)
           infile="{0}/HBIAS_{1}.fits".format(outpath, obsdate)
           link='/salt/HRS_Cals/CAL_BIAS/{0}/{1}/product/HBIAS_{2}.fits'.format(obsdate[0:4], obsdate[4:8], obsdate)
           if os.path.islink(link) and clobber: os.remove(link)
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
      rdnoise=6.81*u.electron
   elif detname=='HBDET':
      prefix='H'
      process = blue_process
      rdnoise=7.11*u.electron
   else:
      raise ValueError('detname must be a valid HRS Detector name')

   #process the flat  frames
   matches = (image_list.summary['obstype'] == 'Flat field') * (image_list.summary['detnam'] == detname) * (image_list.summary['obsmode'] == obsmode)
   flat_list = []
   for fname in image_list.summary['file'][matches]:
        logging.info('Processing flat image {}'.format(fname))
        ccd = process(rawpath+fname, masterbias=master_bias, error=True, rdnoise=rdnoise)
        flat_list.append(ccd)
        if sdb is not None: dq_ccd_insert(rawpath + fname, sdb)

   if flat_list:
        outfile = "{0}/{2}FLAT_{1}_{3}.fits".format(outpath, obsdate, prefix, obsmode.replace(' ', '_'))
        logging.info('Created master flat {}'.format(os.path.basename(outfile)))
        if os.path.isfile(outfile) and clobber:  os.remove(outfile)
        flat = ccdproc.combine(flat_list, method='median', output_file=outfile)

        norm = clean_flatimage(flat.data, filter_size=filter_size, flux_limit=0.3,
                block_size=100, percentile_low=30, median_size=5)

        norm[norm>0]=1
        if detname=='HRDET':
             xc = 1947 #int(xs/2.0)
             detect_kern = norm[1:100, xc]
             #these remove light that has bleed at the edges and may need adjusting
             norm[:,:20]=0
             norm[:,4040:]=0
        elif detname=='HBDET':
             ys, xs = norm.shape
             xc = int(xs/2.0)
             detect_kern = norm[32:110, xc]

        frame = create_orderframe(norm, first_order, xc, detect_kern, smooth_length=smooth_length, 
                      smooth_fraction=smooth_fraction, y_start=y_start, y_limit=y_limit)
        order_file = "{0}/{2}ORDER_{1}_{3}.fits".format(outpath, obsdate, prefix, obsmode.replace(' ', '_'))
        logging.info('Created order frame {}'.format(os.path.basename(order_file)))
        hdu = fits.PrimaryHDU(frame)
        hdu.writeto(order_file, clobber=True)
        if sdb: dq_order_insert(order_file, sdb)

        if link:
            link='/salt/HRS_Cals/CAL_FLAT/{0}/{1}/product/{2}'.format(obsdate[0:4], obsdate[4:8], os.path.basename(outfile))
            if os.path.islink(link) and clobber: os.remove(link)
            print(outfile)
            print(link)
            os.symlink(outfile, link)
            olink='/salt/HRS_Cals/CAL_FLAT/{0}/{1}/product/{2}'.format(obsdate[0:4], obsdate[4:8], os.path.basename(order_file))
            if os.path.islink(olink) and clobber: os.remove(olink)
            os.symlink(order_file, olink)

def hrsarc(rawpath, outpath, detname, obsmode, master_bias=None, master_flat=None, master_order=None,
           sol_dir=None,  link=False, sdb=None, clobber=True):
   """hrsarc processes the HRS Arc files.

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

   #process the arc frames
   matches = (image_list.summary['obstype'] == 'Arc') * (image_list.summary['detnam'] == detname) * (image_list.summary['obsmode'] == obsmode ) * (image_list.summary['propid'] == 'CAL_ARC')
   for fname in image_list.summary['file'][matches]: 
        logging.info('Processing arc in {}'.format(fname))
        ccd  = process(rawpath+fname, masterbias=master_bias)
        if sdb is not None: dq_ccd_insert(rawpath + fname, sdb)

        #flat field the frame
        ccd=flatfield_science(ccd, master_flat, master_order, median_filter_size=None, interp=True)

        #write out frame
        outfile = "{0}/p{1}".format(outpath, fname)
        ccd.write(outfile, clobber=True)

        hrs_arc_process(ccd, master_order, sol_dir, outpath, sdb, filename=fname)

        if link:
            #link the individual file
            for targ_ext in ['sky', 'obj']:
                db_file = outpath + 'dbp' + fname.replace('.fits', '_{}.pkl'.format(targ_ext))
                link='/salt/HRS_Cals/CAL_ARC/{0}/{1}/product/{2}'.format(obsdate[0:4], obsdate[4:8], os.path.basename(db_file))
                if os.path.islink(link) and clobber: os.remove(link)
                os.symlink(db_file, link)

def extract_arc(arc, order_frame, n_order, soldir, target=True, flux_limit=100):
    shift_dict, ws = pickle.load(open(soldir+'sol_%i.pkl' % n_order))
    hrs = HRSOrder(n_order)
    hrs.set_order_from_array(order_frame.data)
    hrs.set_flux_from_array(arc.data, flux_unit=arc.unit)
    hrs.set_target(target)

    data, coef = hrs.create_box(hrs.flux, interp=True)

    #pickle.dump(data, open('box_%s.pkl' % n_order, 'w'))
    xarr = np.arange(len(data[0]))
    warr = ws(xarr)
    flux = np.zeros_like(xarr)
    i_reference = int(data.shape[0]/2.0)
    flux, shift_dict = collapse_array(data, i_reference=i_reference)
    return xarr, warr, flux, ws, shift_dict

def dq_arc(sdb, ws, n_order, filename, obsmode='HIGH RESOLUTION', fiber='obj'):
    """Upload the arc data to to the database

    Parameters
    ----------
    sdb: mysql.sdb
       Link tothe science database

    ws: WavelengthSolution.WavelengthSolution
       Wavelength solution containing the x and wavelength values 

    n_order: int
       HRS order that was analyzed

    filename: str
       Filename for the code

    obsmode:  str
       Observation mode

    fiber: str
       'obj' or 'sky' fiber being uploaded

    
    """

    # setup up the object logic
    if fiber == 'obj':
       Object=1
    else:
       Object=0

    # get the filedata ID
    logic="FileName='{}'".format(os.path.basename(filename))
    FileData_Id = sdb.select('FileData_Id','FileData',logic)[0][0]

    # get the default frames for a given mode
    if os.path.basename(filename).startswith('H'):
        default_arcfile_dict = {'HIGH RESOLUTION':457144, 'MEDIUM RESOLUTION': 457146, 'LOW RESOLUTION': 457148}
    elif os.path.basename(filename).startswith('R'):
        default_arcfile_dict = {'HIGH RESOLUTION':457145, 'MEDIUM RESOLUTION': 457147, 'LOW RESOLUTION': 457149}

    # get the x, w values for the nominal frame
    try:
       results = sdb.select('X, wavelength', 'DQ_HrsArc', 'FileData_Id={} and HrsOrder={} and Object={}'.format(default_arcfile_dict[obsmode], n_order, Object))
       x=np.array(results)[:,0].astype(float)
       w=np.array(results)[:,1].astype(float)
    except:
       x = None
       w = None

    for i in range(len(ws.x)):
        if abs(ws.wavelength[i]-ws(ws.x[i])) > 0.05: continue
        try:
           j =  np.where(abs(w-ws.wavelength[i]) < 0.01)
           dx = ws.x[i]-x[j][0]
        except:
           dx = -99.99
        ins_cmd = 'FileData_Id={fid}, HrsOrder={n_order}, x={x}, wavelength={wavelength}, DeltaX={dx}, Object={Object} ON DUPLICATE KEY UPDATE  wavelength={wavelength}, DeltaX={dx}, Object={Object};'.format(fid=FileData_Id, n_order=n_order, x=ws.x[i], wavelength=ws.wavelength[i], dx=dx, Object=Object)
        sdb.insert(ins_cmd, 'DQ_HrsArc')



def hrs_arc_process(arc, master_order, soldir, outpath, sdb=None, link=False, filename=None):
    """process an hrs arc
    """
    arm, xpos, target, res, w_c, y1, y2 = mode_setup_information(arc.header)

    n_min = master_order.data[master_order.data>0].min()
    n_max = master_order.data.max()
    ws_dict = {}
    for target_fiber in [True, False]:
        if target_fiber:
            if target=='upper':
               targ_ext = 'obj'
            else:
               targ_ext = 'sky'
        else:
            if target=='lower':
               targ_ext = 'obj'
            else:
               targ_ext = 'sky'
       
        for n_order in np.arange(n_min, n_max):
            if not os.path.isfile(soldir+'sol_%i.pkl' % n_order): continue
            if (master_order.data==n_order).sum()==0: continue
            try:
                x, w, f, ws, sh = extract_arc(arc, master_order, int(n_order), soldir, target=target_fiber)
                m_arr = ws_match_lines(x, f, ws, dw=1.0, kernal_size=3)
                m, prob = match_probability(m_arr[:,1], m_arr[:,2],
                                m_init=mod.models.Polynomial1D(1),
                                fitter=mod.fitting.LinearLSQFitter(),
                                tol=0.02, n_iter=5)
            except Exception, e:
                logging.warning(str(e))
                continue
            ws = WavelengthSolution.WavelengthSolution(m_arr[:,0][prob>0.1],
                                               m_arr[:,2][prob>0.1],
                                               ws.model)
            if sdb: dq_arc(sdb, ws, n_order, filename)
            ws.fit()
            ws_dict[n_order] = (ws, sh)
        db_file = outpath + 'dbp' + filename.replace('.fits', '_{}.pkl'.format(targ_ext))
        pickle.dump(ws_dict, open(db_file, 'wb'))

def get_arc(obsdate, prefix, mode=None, cal_dir='/salt/HRS_Cals/', nlim=180, sdb=None):
    """Find Arc solution closest in date
    """
    start_date = dt.datetime.strptime('{} 12:00:00'.format(obsdate), '%Y%m%d %H:%M:%S')

    def get_name(start_date, i, cal_dir, prefix, mode):
        date = start_date + dt.timedelta(seconds=i*24*3600.0)
        obsdate = date.strftime('%Y%m%d')
        cdir = '{cal_dir}CAL_ARC/{year}/{mmdd}/product/'.format(
           cal_dir=cal_dir, year=obsdate[0:4], mmdd=obsdate[4:8], prefix=prefix, mode=mode)

        tab_cmd = 'FileData join ProposalCode using (ProposalCode_Id) join FitsHeaderImage using (FileData_Id)'
        log_cmd = "FileName like '{prefix}{obsdate}%' and FileData.OBSMODE='{obsmode}' and Proposal_Code = 'CAL_ARC'" .format(prefix=prefix, obsmode=mode, obsdate=obsdate)

        record = sdb.select('FileName', tab_cmd, log_cmd)
        if record == (): return None
        cfile = cdir + 'dbp'+record[0][0].replace('.fits', '_obj.pkl')
        if os.path.isfile('hrs/product/{}'.format(os.path.basename(cfile))): return 'hrs/product/{}'.format(os.path.basename(cfile))
        if os.path.isfile(cfile): return cfile

        return None


    for i in range(nlim):
        #look into the past
        cfile = get_name(start_date, i,  cal_dir, prefix, mode)
        if cfile: return cfile
        #look into the future
        cfile = get_name(start_date, -i,  cal_dir, prefix, mode)
        if cfile: return cfile

    return None


def run_hrsflat(obsdate, rawpath, outpath, sdb=None, nlim=180, link=True):
    """Run the flat fields"""

    # process the red flat fields
    prefix = 'R'
    mccd =  get_hrs_calibration_frame(obsdate, prefix, 'BIAS',  mode=None, cal_dir='/salt/HRS_Cals/', nlim=nlim)
    logging.info('Using {} for the Master Bias frame'.format(mccd))
    masterbias = CCDData.read(mccd, units=u.adu)
    for obsmode in ['HIGH STABILITY', 'LOW RESOLUTION', 'MEDIUM RESOLUTION', 'HIGH RESOLUTION']:
        hrsflat(rawpath, outpath, detname='HRDET', obsmode=obsmode,  master_bias=masterbias,
                first_order=53, y_start=4, y_limit=3920, smooth_length=20, smooth_fraction=0.4, filter_size=151,
                clobber=True, sdb=sdb, link=link)

    #process blue flat fields
    prefix = 'H'
    mccd =  get_hrs_calibration_frame(obsdate, prefix, 'BIAS',  mode=None, cal_dir='/salt/HRS_Cals/', nlim=nlim)
    logging.info('Using {} for the Master Bias frame'.format(mccd))
    masterbias = CCDData.read(mccd, units=u.adu)
    for obsmode in ['HIGH STABILITY', 'LOW RESOLUTION', 'MEDIUM RESOLUTION', 'HIGH RESOLUTION']:
        filter_size = 101
        if obsmode == 'LOW RESOLUTION': filter_size = 131
        try:
           hrsflat(rawpath, outpath, detname='HBDET', obsmode=obsmode,  master_bias=masterbias,
                first_order=84, y_start=30, y_limit=3884, smooth_length=20, smooth_fraction=0.4, filter_size=filter_size,
                clobber=True, sdb=sdb, link=link)
        except ValueError:
           hrsflat(rawpath, outpath, detname='HBDET', obsmode=obsmode,  master_bias=masterbias,
                first_order=84, y_start=30, y_limit=3884, smooth_length=20, smooth_fraction=0.4, filter_size=101,
                clobber=True, sdb=sdb, link=link)

def run_hrsarcs(obsdate, rawpath, outpath,  nlim=180, sdb=None, link=True):
    """Run HRS arc frames"""

    mode_dict={}
    mode_dict['LOW RESOLUTION']='lr'
    mode_dict['MEDIUM RESOLUTION']='mr'
    mode_dict['HIGH RESOLUTION'] = 'hr'
    mode_dict['HIGH STABILITY'] = 'hs'

    #process arc frames
    for prefix in ['H', 'R']:
       if prefix == 'R': detname='HRDET'
       if prefix == 'H': detname='HBDET'

       mccd =  get_hrs_calibration_frame(obsdate, prefix, 'BIAS',  mode=None, cal_dir='/salt/HRS_Cals/', nlim=nlim)
       logging.info('Using {} for the Master Bias frame'.format(mccd))
       masterbias = CCDData.read(mccd)

       for obsmode in ['LOW RESOLUTION', 'MEDIUM RESOLUTION', 'HIGH RESOLUTION']: #high stability

           mccd =  get_hrs_calibration_frame(obsdate, prefix, 'FLAT', mode=obsmode.replace(' ', '_'), cal_dir='/salt/HRS_Cals/', nlim=nlim)
           logging.info('Using {} for the {} flat frame'.format(mccd, obsmode.lower()))
           masterflat = CCDData.read(mccd)

           mccd =  get_hrs_calibration_frame(obsdate, prefix, 'ORDER', mode=obsmode.replace(' ', '_'), cal_dir='/salt/HRS_Cals/', nlim=nlim)
           logging.info('Using {} for the {} order frame'.format(mccd, obsmode.lower()))
           masterorder = CCDData.read(mccd, unit=u.electron)

           hrsarc(rawpath, outpath, detname=detname, obsmode=obsmode,  master_bias=masterbias,
                  master_flat=masterflat, master_order=masterorder, sol_dir='/home/sa/smc/hrs/{}/'.format(mode_dict[obsmode]),
                  sdb=sdb, link=link, clobber=True)



def run_science(obsdate, rawpath, outpath, sdb=None, link=True, symdir='./', nlim=180, mfs=11):
        #process bias frames

        #process arc frames
        for prefix in ['H', 'R']:
           if prefix == 'R': detname='HRDET'
           if prefix == 'H': detname='HBDET'
           mccd = 'hrs/product/{prefix}{cal_type}_{year}{mmdd}.fits'.format(
                  cal_type="BIAS", year=obsdate[0:4], mmdd=obsdate[4:8], prefix=prefix)
           if not os.path.isfile(mccd):
               mccd =  get_hrs_calibration_frame(obsdate, prefix, 'BIAS',  mode=None, cal_dir='/salt/HRS_Cals/', nlim=nlim)
           logging.info('Using {} for a bias file'.format(mccd))
           masterbias = CCDData.read(mccd)
           for obsmode in ['MEDIUM RESOLUTION', 'HIGH RESOLUTION', 'LOW RESOLUTION']:#,'HIGH STABILITY']:
               mccd = 'hrs/product/{prefix}{cal_type}_{year}{mmdd}_{mode}.fits'.format(
                      cal_type="FLAT", year=obsdate[0:4], mmdd=obsdate[4:8], prefix=prefix, mode=obsmode.replace(' ', '_'))
               if not os.path.isfile(mccd):
                   mccd =  get_hrs_calibration_frame(obsdate, prefix, 'FLAT', mode=obsmode.replace(' ', '_'), cal_dir='/salt/HRS_Cals/', nlim=nlim)
                   if mccd is None:
                       logging.info('No {} flat for {} in {}'.format(prefix, obsdate, obsmode))
                       continue
               logging.info('Using {} for a {} flat file'.format(mccd, obsmode.lower()))
               masterflat = CCDData.read(mccd)
               mccd = 'hrs/product/{prefix}{cal_type}_{year}{mmdd}_{mode}.fits'.format(
                      cal_type="ORDER", year=obsdate[0:4], mmdd=obsdate[4:8], prefix=prefix, mode=obsmode.replace(' ', '_'))
               if not os.path.isfile(mccd):
                   mccd =  get_hrs_calibration_frame(obsdate, prefix, 'ORDER', mode=obsmode.replace(' ', '_'), cal_dir='/salt/HRS_Cals/', nlim=nlim)
               logging.info('Using {} for an {} order file'.format(mccd, obsmode.lower()))
               masterorder = CCDData.read(mccd, unit=u.electron)
               marc = get_arc(obsdate, prefix, mode=obsmode, cal_dir='/salt/HRS_Cals/', nlim=180, sdb=sdb)
               logging.info('Using {} for an {} object arc file'.format(marc, obsmode.lower()))
               arc_dict={}
               obj_dict = pickle.load(open(marc, 'r'))
               arc_dict['obj'] =  obj_dict
               sky_dict = pickle.load(open(marc.replace('obj', 'sky'), 'r'))
               arc_dict['sky'] =  sky_dict
               #if prefix=='H': masterbias=None
               if int(obsdate) < 20150901: masterbias=None
               if int(obsdate) < 20161107 and prefix=='H': masterbias=None
               hrsscience(rawpath, outpath, detname=detname, obsmode=obsmode,  master_bias=masterbias,
                  master_flat=masterflat, master_order=masterorder, arc_dict = arc_dict, median_filter_size=mfs,
                  sdb=sdb, symdir=symdir, link=link, clobber=True)

    

def hrsscience(rawpath, outpath, detname, obsmode, master_bias=None, master_flat=None, 
               master_order=None, median_filter_size=11, 
               arc_dict=None,  sdb=None, symdir='./', link=False, clobber=True):
   """hrsscience processes the HRS science files.

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

   link: boolean
      Link data to their proposals

   clobber: boolean
      Overwrite existing files

   
   """
   print(os.getcwd())
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
      rdnoise = 6.81 * u.electron
   elif detname=='HBDET':
      prefix='H'
      process = blue_process
      rdnoise=7.11*u.electron
   else:
      raise ValueError('detname must be a valid HRS Detector name')
   print(prefix)

   if master_bias  is None:
      overscan_correct=True
   else:  
      overscan_correct=False

   #process the arc frames
   matches = (image_list.summary['obstype'] == 'Science') * (image_list.summary['detnam'] == detname) * (image_list.summary['obsmode'] == obsmode ) 
   for fname in image_list.summary['file'][matches]: 
        logging.info('Reducing {}'.format(fname))
        ccd = process(rawpath+fname, masterbias=master_bias, oscan_correct=overscan_correct, error=True, rdnoise=rdnoise)
        if sdb is not None: dq_ccd_insert(rawpath + fname, sdb)

        #cosmic ray clean the data
        crmask, cleanarr = detect_cosmics(ccd.data, inmask=None, sigclip=4.5, sigfrac=0.3,
                   objlim=5.0, gain=1.0, readnoise=6.5,
                   satlevel=65536.0, pssl=0.0, niter=4,
                   sepmed=True, cleantype='meanmask', fsmode='median',
                   psfmodel='gauss', psffwhm=2.5, psfsize=7,
                   psfk=None, psfbeta=4.765, verbose=False)
        ccd.data = cleanarr
        if ccd.mask == None:
           ccd.mask = crmask
        else:
           ccd.mask = ccd.mask * crmask

        #flat field the frame
        ccd=flatfield_science(ccd, master_flat, master_order, median_filter_size=median_filter_size, interp=True)

        #write out frame
        outfile = "{0}/p{1}".format(outpath, fname)
        ccd.write(outfile, clobber=True)

        hrs_science_process(ccd, master_order, arc_dict, outpath, p_order=7, sdb=sdb, filename=fname, interp=True)

        if link:
            if ccd.header['PROPID'] == 'JUNK': continue
            logging.info("Copying {} to {}".format(fname, ccd.header['PROPID']))
            pdir='{0}/{1}/product'.format(symdir, ccd.header['PROPID'])
            if symdir=='./': 
                  propath = "../../hrs/product/"
            else:
                  propath=outpath
            outfile = "{1}/p{0}".format(fname, propath)

            link = "{0}/p{1}".format(pdir, fname)
            if os.path.islink(link) and clobber: os.remove(link)
            os.symlink(outfile, link)

            if not os.path.isfile(pdir+'/README'): add_hrs_README(pdir)

            for file_prefix in ['p', 'sp']:
                for targ_ext in ['obj', 'sky']:
                   if file_prefix=='sp' and targ_ext=='sky': continue

                   sname = fname.replace('.fits', '_{}.fits'.format(targ_ext))
                  
                   sfile = "{0}/{2}{1}".format(propath, sname, file_prefix)
                   link = "{0}/{2}{1}".format(pdir, sname, file_prefix)
                   if os.path.islink(link) and clobber: os.remove(link)
                   os.symlink(sfile, link)

        
def hrs_science_process(ccd, master_order, arc_dict, outpath, p_order=7, interp=False, sdb=None, filename=None):
    """process an hrs science frame

    Parameters
    ----------
    p_order: int
       Order of polynomical for normalization
    """
    arm, xpos, target, res, w_c, y1, y2 = mode_setup_information(ccd.header)

    n_min = master_order.data[master_order.data>0].min()
    n_max = master_order.data.max()
    

    for targ_ext in ['sky', 'obj']:
        sp_dict = {}
        if target == 'upper' and targ_ext=='obj':  fiber = True
        if target == 'upper' and targ_ext=='sky':  fiber = False
        if target == 'lower' and targ_ext=='obj':  fiber = False 
        if target == 'lower' and targ_ext=='sky':  fiber = True 
        logging.info('Extracting {} spectra using the {} fiber in {}'.format(targ_ext, 'upper' if fiber else 'lower', filename))
        for n_order in arc_dict[targ_ext]:
            if (master_order.data==n_order).sum()==0: continue
            try:
               ws, shift = arc_dict[targ_ext][n_order]
               w, f, e, fs  = extract_order(ccd, master_order, int(n_order), ws, shift, y1=y1, y2=y2, order=None, target=fiber, interp=interp)
               sp_dict[n_order] = [w,f, e, fs]
            except:
               pass
       
        if filename is not None:
            outfile = outpath + 'p' +filename.replace('.fits', '_{}.fits'.format(targ_ext))
            write_spdict(outfile, sp_dict, header=ccd.header)

        # preform sky subtraction
        if targ_ext == 'sky':
            sky_dict = sp_dict.copy()
            continue
        elif ccd.header['I2STAGE'] == 'Nothing In Beam':
            for n_order in sp_dict:
                w, f, e,s  = sp_dict[n_order]
                sw, sf, se, ss = sky_dict[n_order]
                f = f - np.interp(w, sw, sf)
                e = (e**2 + np.interp(w, sw, se)**2)**0.5
                s = s - np.interp(w, sw, ss)
                sp_dict[n_order] = [w, f, e, s]

         
        #normalize the frame
        #continuum = mod.models.Chebyshev1D(p_order)
        #fitter=mod.fitting.LinearLSQFitter()
        #try:
            #pass #nsp_dict = normalize_spectra(sp_dict, model=continuum, fitter=fitter)
        #except:
            #logging.info('Failed normalizing {} because {}'.format(filename, str(e)))
            #continue

        #if filename is not None:
            #outfile = outpath + 'np' +filename.replace('.fits', '_{}.fits'.format(targ_ext))
            #write_spdict(outfile, nsp_dict, header=ccd.header)
   
        #stitch the frame together
        n_orders = np.array(sp_dict.keys(),dtype=int)
        n_min = n_orders.min()
        n_max = n_orders.max()

        if arm == 'H': 
           center_order=103
           trim = 20
           median_clean=0
        elif arm=='R':
           center_order =  72
           trim = 50
           median_clean = 0

        if ccd.header['OBSMODE'] in ['HIGH RESOLUTION', 'HIGH STABILITY']:
           resolution = 65000
           dr = 2
        elif ccd.header['OBSMODE'] in ['MEDIUM RESOLUTION']:
           resolution = 35000
           dr = 2
        elif ccd.header['OBSMODE'] in ['LOW RESOLUTION']:
           resolution = 15000
           dr = 4

        logging.info('Stitching {}'.format(filename))
        try: 
            wave, flux, err, sarr = stitch_spectra(sp_dict, center_order=center_order, trim=trim)
        except Exception, e:
            logging.info('Failed stitching {} because {}'.format(filename, str(e)))
            return 


        logging.info('Resampling fluxes in {}'.format(filename))
        try:
            swave, sarr, serr = resample(1.0*wave, sarr, abs(sarr)**0.5, R=resolution, dr=dr, median_clean=median_clean)
            wave, flux, err = resample(wave, flux, err, R=resolution, dr=dr, median_clean=median_clean)
            mask = (wave>4000)*(flux>0)
            err = err[mask]
            flux = flux[mask]
            wave = wave[mask]
            sarr = np.interp(wave, swave, sarr)
        except Exception, e:
            logging.info('Failed to resample {} because {}'.format(filename, str(e)))
            continue

        try:
            logging.info('Applying heliocentric correction to {}'.format(filename))
            vhelio = calculate_velocity(ccd.header)
            ccd.header['VHEL'] = (vhelio.value, 'Helocentric radial velocity (km/s)')
            wave = convert_data(wave, vhelio)
        except Exception, e:
            logging.info('Failed to apply heliocentric correction to {} because {}'.format(filename, str(e)))
            continue
 

        if filename is not None:
            outfile = outpath + 'sp' + filename.replace('.fits', '_{}.fits'.format(targ_ext))
            tmp_dict={}
            tmp_dict[0] = [wave, flux, err, sarr]
            write_spdict(outfile, tmp_dict, header=ccd.header)


def add_hrs_README(pdir):
    """Add a README describing the HRS fies

    pdir: str
       Directory to add README
    """
    readme_str="""
HRS REDUCED FILES

The following files have been reducing using the pyhrs pipeline:
p*.fits -- reduced 2D data (bias, flatfielded, gain-corrected, CR cleaned)
p*_obj.fits or p*_sky.fits -- 1D extracted files
sp*obj.fits -- stitched spectra corrected for heliocentric velocities and vacuum wavelengths

Older depreciated file types:
np*_obj.fits or np*_sky.fits -- normalized data
snp*_obj.fits or snp*_sky.fits -- stitched data 

The 1D extracted files have the format wavelength, flux, error, and order. 

For more descriptions of the processing, please see:
http://pyhrs.readthedocs.io/en/latest/

If you use processed files in this directory, please cite:
Crawford, S.~M.\ 2015, Astrophysics Source Code Library, ascl:1511.005 
http://adsabs.harvard.edu/abs/2015ascl.soft11005C
Craig, M.~W., Crawford, S.~M., Deil, C., et al.\ 2015, Astrophysics Source Code Library, ascl:1510.007 
http://adsabs.harvard.edu/abs/2015ascl.soft10007C

If you use the m* files in this directory, please cite:
Crawford, S.~M., Still, M., Schellart, P., et al.\ 2010, Proc SPIE, 7737, 773725 
http://adsabs.harvard.edu/abs/2010SPIE.7737E..25C

Files starting with an m* are a result of the old pipeline and are included
only for historical reasons and may be depreciated at some point
in the future.

If you have downloaded the data from the web manager, there are additional
files that may be available to you that have been run through A. Kniazev's
MIDAS pipeline.   

The following files were reduced using MIDAS pipeline:
m*_[12]w.fits - extracted first(1) and second (2) fibers.
             Wavelength calibrated (uniform step). Not merged.
             2D spectrum, where each order is one line in 2D spectrum.
             This is the most compact format, but only MIDAS can understand
             it easily.

m*_[12]we.fits - extracted first(1) and second(2) fibers, but each order
             is written as separate FITS extension. FITS file has as many
             extensions as extracted orders. Can be easily understand by IRAF.
             NOT corrected for the blaze effect.

m*_[12]wm.fits - extracted fibers, wavelength calibrated and merged.
             Simple 1D FITS spectrum. Both MIDAS and IRAF understand it.
             NOT corrected for the blaze effect.

m*_u[12]w.fits - extracted fibers, corrected for the blaze effect.
             Wavelength calibrated (uniform step). Not merged.
             2D spectrum, where each order is one line in 2D spectrum.
             This is the most compact format, but only MIDAS can understand
             it easily.

m*_u[12]we.fits - extracted fibers, corrected for the blaze effect.
             Wavelength calibrated (uniform step). Not merged.
             Each order is written as separate FITS extension.
             FITS file has as many extensions as extracted orders.
             Can be easily understand by IRAF.

m*_[12]wm.fits - extracted fibers, corrected for the blaze effect,
             wavelength calibrated (uniform step) and merged.
             Simple 1D FITS spectrum. Both MIDAS and IRAF understand it.

m*_wm.fits - the final result after sky fiber was subtracted.
             Simple 1D FITS spectrum. Both MIDAS and IRAF understand it.


For more descriptions of the processing, please see:
http://www.saao.ac.za/~akniazev/pub/HRS_MIDAS/HRS_pipeline.pdf

If you use these files in your science work, please cite:

For using m*fits reduced data:
Crawford, S.M. et al. 2010
http://adsabs.harvard.edu/abs/2010SPIE.7737E..25C

For LR data:
Kniazev, A.Y.; Gvaramadze, V.V.; Berdnikov, L.N.,
http://adsabs.harvard.edu/abs/2016MNRAS.459.3068K

For MR and HR data:
Kniazev, A.Y.; Gvaramadze, V.V.; Berdnikov, L.N.,
http://adsabs.harvard.edu/abs/2016arXiv161200292K


"""
    fout = open(pdir+'/README', 'w')
    fout.write(readme_str)
    fout.close()

    

