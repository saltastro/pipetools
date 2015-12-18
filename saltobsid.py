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
# Martin Still (SAAO)    0.2          01 Aug 2007
# S M Crawford (SAAO)    0.3          29 Jan 2008

# saltobsid compiles raw and reduced data into obsid-dependent
# directories
# 29 Jan changed the directories into propid directories

from pyraf import iraf
import pyfits, string, os, glob

from saltsafekey import fastmode

import saltsafeio as saltio
import saltsafekey as saltkey
import saltsafestring as saltstring
from saltsafelog import logging
from salterror import SaltError, SaltIOError

debug=True

# -----------------------------------------------------------
# core routine

def saltobsid(propcode,obslog,rawpath,prodpath,outpath,prefix='mbxgp', fprefix='bxgp',clobber=False,logfile='salt.log',verbose=True):


   with logging(logfile,debug) as log:

       # are the arguments defined
       pids = saltio.argunpack('propcode',propcode)

       # check observation log file exists
       obslog = obslog.strip()
       saltio.fileexists(obslog)

       #open the observing log
       obstruct = saltio.openfits(obslog)
       obstab = saltio.readtab(obstruct[1],obslog)
       saltio.closefits(obstruct)

       #read in the file information
       filenames = saltstring.listfunc(obstab.field('filename'),'lstrip')
       instrumes = saltstring.listfunc(obstab.field('instrume'),'lstrip')
       proposers = saltstring.listfunc(obstab.field('proposer'),'clean')
       propids = saltstring.listfunc(obstab.field('propid'),'clean')
       ccdtypes = saltstring.listfunc(obstab.field('ccdtype'),'clean')
       ccdsums = saltstring.listfunc(obstab.field('ccdsum'),'clean')
       gainsets = saltstring.listfunc(obstab.field('gainset'),'clean')
       rospeeds = saltstring.listfunc(obstab.field('rospeed'),'clean')
       detmodes = saltstring.listfunc(obstab.field('detmode'),'clean')
       filters = saltstring.listfunc(obstab.field('filter'),'clean')
       gratings = saltstring.listfunc(obstab.field('grating'),'clean')
       gr_angles = obstab.field('gr-angle')
       ar_angles = obstab.field('ar-angle')
 
       # Create the list of proposals
       try:
           pids=saltio.cleanpropcode(pids, propids)
       except SaltIOError:
           #throw a warning adn exit if not data needs to be filterd
           log.warning('No data to filter\n', with_stdout=verbose)
           return

       # check paths exist, end with a "/" and convert them to absolute paths
       rawpath = saltio.abspath(rawpath)
       prodpath = saltio.abspath(prodpath)
       outpath = saltio.abspath(outpath)

       #create the symlink raw path
       rawsplit=rawpath.strip().split('/')
       symrawpath='../../%s/%s/' % (rawsplit[-3], rawsplit[-2])
       prodsplit=prodpath.strip().split('/')
       symprodpath='../../%s/%s/' % (prodsplit[-3], prodsplit[-2])
  

       # create PI directories
       for pid in pids:
           saltio.createdir(outpath+pid)
           saltio.createdir(outpath+pid+'/raw')
           saltio.createdir(outpath+pid+'/product')

       #copy the data that belongs to a pid into that directory
       log.message('SALTOBSID -- filtering images to proposal directories\n', with_stdout=verbose)

       #copy data for a given proposal to the raw and produce directories
       for i in range(len(obstab)):
           if os.path.exists(outpath+obstab[i]['propid']):
             if obstab[i]['object'].upper() not in ['ZERO', 'BIAS']:
               fname=obstab[i]['filename']
               pdir=obstab[i]['propid']
               detmode=obstab[i]['detmode']
               linkfiles(fname, pdir,detmode, symrawpath, symprodpath, outpath, prefix, fprefix, clobber)
               message='Copying %s to %s' % (fname, pdir)
               log.message(message, with_header=False, with_stdout=verbose)

       #look through the bias/flat/arc/standard data to see if there is any relavent data
       log.message('SALTOBSID -- filtering calibration files to proposal directories\n', with_stdout=verbose)

       caldata=['ZERO', 'FLAT', 'ARC']
       biasheader_list=['DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED']
       flatheader_list=['DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED', 'FILTER', 'GRATING', 'GR-ANGLE', 'AR-ANGLE']
       archeader_list=['OBSMODE', 'DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED', 'FILTER', 'GRATING', 'GR-ANGLE', 'AR-ANGLE']
       
       calproplist=['CAL_SPST']
       #Include bias frames
       log.message('SALTOBSID -- filtering bias files to proposal directories\n', with_stdout=verbose)

       for i in range(len(obstab)):
           fname=obstab[i]['filename']
           prop_list=[]
           #if it is a zero, check to see what other data have the same settings 
           if obstab[i]['CCDTYPE'].strip().upper()=='ZERO' or obstab[i]['OBJECT'].strip().upper() in ['BIAS', 'ZERO']:
               for j in range(len(obstab)):
                   if comparefiles(obstab[i], obstab[j], biasheader_list):
                       prop_list.append(obstab[i]['PROPID'])

           prop_list=saltio.removebadpids(set(prop_list))
           for pdir in prop_list:
                   detmode=obstab[i]['detmode']
                   linkfiles(fname, pdir, detmode, symrawpath, symprodpath, outpath,  fprefix, fprefix, clobber)
                   message='Copying %s to %s' % (fname, pdir)
                   log.message(message, with_header=False, with_stdout=verbose)

       #Include calibration  frames
       log.message('SALTOBSID -- filtering  calibration files to proposal directories\n', with_stdout=verbose)
 
       for i in range(len(obstab)):
           fname=obstab[i]['filename']
           prop_list=[]

           #if it is a flat, check to see what other data have the same settings 
           if obstab[i]['CCDTYPE'].strip().upper()=='FLAT':
               for j in range(len(obstab)):
                   if comparefiles(obstab[i], obstab[j],  flatheader_list):
                       prop_list.append(obstab[j]['PROPID'])

           #if it is a arc, check to see what other data have the same settings 
           if obstab[i]['CCDTYPE'].strip().upper()=='ARC':
               for j in range(len(obstab)):
                   if comparefiles(obstab[i], obstab[j],  archeader_list):
                       prop_list.append(obstab[j]['PROPID'])


           #if it is a calibration standard, see what other data have the same settings
           if obstab[i]['PROPID'].strip().upper() in calproplist:
               for j in range(len(obstab)):
                   if comparefiles(obstab[i], obstab[j],  flatheader_list):
                       prop_list.append(obstab[j]['PROPID'])


           prop_list=saltio.removebadpids(set(prop_list))
           for pdir in prop_list:
               if pdir!=obstab[i]['propid']:
                   detmode=obstab[i]['detmode']
                   linkfiles(fname, pdir, detmode, symrawpath, symprodpath, outpath,  prefix, fprefix, clobber)
                   message='Copying %s to %s' % (fname, pdir)
                   log.message(message, with_header=False, with_stdout=verbose)

       #Include master (bias or flat) frames
       log.message('SALTOBSID -- filtering master calibration files to proposal directories\n', with_stdout=verbose)
       masterlist=glob.glob(prodpath+'*Bias*')+glob.glob(prodpath+'*Flat*')
       for bimg in masterlist:
           struct=pyfits.open(bimg)
           bdict={}
           prop_list=[]
           for k in biasheader_list:
               bdict[k]=saltkey.get(k, struct[0])
           for i in range(len(obstab)):
               if comparefiles(obstab[i], bdict,  biasheader_list):
                   prop_list.append(obstab[i]['PROPID'])
           struct.close()

           #copy the files over to the directory
           prop_list=saltio.removebadpids(set(prop_list))
           for pdir in prop_list:
               fname=os.path.basename(bimg)
               infile = symprodpath+fname
               link = outpath+pdir+'/product/'+fname
               saltio.symlink(infile,link,clobber)
               message='Copying %s to %s' % (fname ,pdir)
               log.message(message, with_header=False, with_stdout=verbose)

       


def comparefiles(afile, bfile, headers):
    """Compare the headers in two sets of tab entries
       and see if they are the same or different
    """
    for k in headers:
        if isinstance(afile[k], str):
           if afile[k].strip().upper()!=bfile[k].strip().upper(): return False
        if isinstance(afile[k], int):
           if afile[k]==bfile[k]: return False
        if isinstance(afile[k], float):
           if abs(afile[k]-bfile[k])>0.01: return False
    return True

def linkfiles(fname, pdir, detmode, rawpath, prodpath, outpath, prefix='mbxgp', fprefix='bxgp', clobber=False):

   #copy the raw data
   infile=rawpath+fname
   link=outpath+pdir+'/raw/'+fname
   saltio.symlink(infile,link,clobber)

   #copy the product data
   if not fastmode(detmode):
       pfname=prefix+fname
   else:
       pfname=fprefix+fname
   infile = prodpath+pfname
   link = outpath+pdir+'/product/'+pfname
   if fname[0] in ['S', 'P', 'H', 'R']: 
      saltio.symlink(infile,link,clobber)


# -----------------------------------------------------------
# main code
if not iraf.deftask('saltobsid'):
  parfile = iraf.osfn("pipetools$saltobsid.par")
  t = iraf.IrafTaskFactory(taskname="saltobsid",value=parfile,function=saltobsid, pkgname='pipetools')
