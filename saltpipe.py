################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################


#!/usr/bin/env python

"""
SALTPIPE handles most of the major features of pipeline the data

Author                 Version      Date
-----------------------------------------------
Martin Still (SAAO)    0.2          21 Jul 2006
S M Crawford (SAAO)    0.3          22 Feb 2008
S M Crawford (SAAO)    0.4          22 Jun 2012

Updates
-------------------------------------------------
20120622  -Updated the code to new error handling
          -Included loading the data onto the ftp server
           and uploading information into database
20121109  -Added slotreadtimefix 

"""

# SALT pipeline process

from __future__ import with_statement

import os, time, ftplib, glob, shutil
import numpy as np
import scipy as sp
from astropy.io import fits
import matplotlib
matplotlib.use('Agg')

from sdb_mysql import mysql


from pyraf import iraf
from pyraf.iraf import pysalt


import salttime
import saltsafekey as saltkey
import saltsafemysql as saltmysql
import saltsafeio as saltio
import saltsafestring as saltstring

from saltsafelog import logging, history

from saltbin2fit import saltbin2fit
from salthrspreprocess import salthrspreprocess

from saltobsid import saltobsid
from salthtml import salthtml
from saltsdbloadfits import saltsdbloadfits
from saltftp import saltftp
from bvitftp import bvitftp

from saltemail import saltemail
from saltarchive import saltarchive
from saltclean import saltclean
from salteditkey import salteditkey
from saltelsdata import saltelsdata
from slotreadtimefix import slotreadtimefix
from pipelinestatus import pipelinestatus


from hrsclean import hrsclean
from saltadvance import saltadvance

from salterror import SaltError

debug=True

# Make sure the plotting functions work with an older version of matplotlib


# -----------------------------------------------------------
# core routine

def saltpipe(obsdate,pinames,archive,ftp,email,emserver,emuser,empasswd,bcc, qcpcuser,qcpcpasswd,
             ftpserver,ftpuser,ftppasswd,sdbhost, sdbname, sdbuser, sdbpass, elshost, elsname, 
             elsuser, elspass, median,function,order,rej_lo,rej_hi,niter,interp,
             clobber, runstatus, logfile,verbose):

   # set up

   basedir=os.getcwd()
   propcode=pinames
   sender = emuser + '@salt.ac.za'
   recipient = sender
   emessage = ''
   emailfile = '../piemaillist/email.lis'

   # check the observation date is sensible
   if ('/' in obsdate or '20' not in obsdate or len(obsdate) != 8):
       emessage = 'Observation date does not look sensible - YYYYMMDD\n'
       raise SaltError(emessage)


   # stop if the obsdate temporary directory already exists

   obsdir='%s' % obsdate
   if os.path.exists(obsdir):
       emessage += 'The temporary working directory ' + os.getcwd() + '/'
       emessage += obsdate + ' already exists. '
       raise SaltError(emessage)

   # create a temporary working directory and move to it
   saltio.createdir(obsdir)
   saltio.changedir(obsdir)
   workpath = saltio.abspath('.')

   # test the logfile
   logfile = workpath+logfile
   logfile = saltio.logname(logfile)

   #note the starttime
   starttime = time.time()

   #start logging
   with logging(logfile,debug) as log:

       #connect to the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, sdbpass)
       
       #get the nightinfo id   
       nightinfoid=saltmysql.getnightinfoid(sdb, obsdate)

       #Get the list of proposal codes
       state_select='Proposal_Code'
       state_tables='ProposalCode'
       state_logic=""
       records=saltmysql.select(sdb, state_select, state_tables, state_logic)
       propids=[k[0] for k in records]

       # Calculate the current date
       currentdate=salttime.currentobsdate()

       # are the arguments defined
       saltio.argdefined('obsdate',obsdate)

       # check email and ftp arguments are consistent
       if email and not ftp:
           message =  'ERROR: SALTPIPE -- cannot send email to PI(s) unless data is transferred '
           message += 'to the FTP server; use ftp=\'yes\' email=\'yes\''
           raise SaltError(message)

       # identify a potential list of keyword edits
       keyfile = '../newheadfiles/list_newhead_' + obsdate
       if not os.path.isfile(keyfile):
           message = '\nSALTPIPE -- keyword edits ' + keyfile + ' not found locally'
           log.message(message)


       # check directories for the raw RSS data
       rssrawpath = makerawdir(obsdate, 'rss')

       # check directories for the raw SALTICAM data
       scmrawpath = makerawdir(obsdate, 'scam')


       # check raw directories for the disk.file record and find last file number
       #check rss data
       lastrssnum = checkfordata(rssrawpath, 'P', obsdate, log)
       #check scame data
       lastscmnum =  checkfordata(scmrawpath, 'S', obsdate, log)
       #check for HRS Data--not filedata yet, so cannot check
   
       if lastrssnum == 1 and lastscmnum == 1:
           message = 'SALTPIPE -- no SALTICAM or RSS data obtained on ' + obsdate
           emessage += '\n' + message + '\n'
           log.message(message)

       #copy the data to the working directory
       if lastrssnum > 1:
           message = 'Copy ' + rssrawpath + ' --> ' + workpath + 'raw/'
           log.message(message)
           saltio.copydir(rssrawpath,'rss/raw')
       if lastscmnum > 1:
           message = 'Copy ' + scmrawpath + ' --> ' + workpath + 'raw/'
           log.message(message)
           saltio.copydir(scmrawpath,'scam/raw')

       #copy and pre-process the HRS data
       saltio.createdir('hrs')
       saltio.createdir('hrs/raw')

       hrsbrawpath = makerawdir(obsdate, 'hbdet')
       message = 'Copy ' + hrsbrawpath + ' --> ' + workpath + 'raw/'
       log.message(message)
       salthrspreprocess(hrsbrawpath, 'hrs/raw/', clobber=True, log=log, verbose=verbose)
       lasthrbnum=len(glob.glob('hrs/raw/*fits'))
       
       hrsrrawpath = makerawdir(obsdate, 'hrdet')
       message = 'Copy ' + hrsrrawpath + ' --> ' + workpath + 'raw/'
       log.message(message)
       salthrspreprocess(hrsrrawpath, 'hrs/raw/', clobber=True, log=log, verbose=verbose)
       lasthrsnum=max(lasthrbnum, len(glob.glob('hrs/raw/*fits')))

       if lastrssnum>1 or lastscmnum>1:
           message = 'Copy of data is complete'
           log.message(message)
       else:
           message = 'No data was taken on %s' % obsdate
           log.message(message)

       #process the data RSS data
       if lastrssnum>1:
           preprocessdata('rss', 'P', obsdate, keyfile, log, logfile, verbose)

       #process the  SCAM data
       if lastscmnum>1:
           preprocessdata('scam', 'S', obsdate, keyfile, log, logfile, verbose)

       #process the HRS data
       if lasthrsnum>=1:
           preprocessdata('hrs', 'H', obsdate, keyfile, log, logfile, verbose)
           preprocessdata('hrs', 'R', obsdate, keyfile, log, logfile, verbose)
         


       #check that all data was given a proper proposal id
       #only do it for semesters after the start of science operations
       if int(obsdate)>=20110901:
           # Check to see that the PROPID keyword exists and if not add it
           message = '\nSALTPIPE -- Checking for PROPID keyword'
           log.message(message)

           #check rss data
           rssstatus=runcheckforpropid(glob.glob('rss/raw/P*.fits'), propids, log)
           #check scam data
           scmstatus=runcheckforpropid(glob.glob('scam/raw/S*.fits'), propids, log)
           #check hrsB data
           hrsbstatus=runcheckforpropid(glob.glob('hrs/raw/H*.fits'), propids, log)
           #check hrsB data
           hrsrstatus=runcheckforpropid(glob.glob('hrs/raw/R*.fits'), propids, log)

           if not rssstatus  or not scmstatus or not hrsbstatus or not hrsrstatus: 
               msg='The PROPIDs for these files needs to be updated and re-start the pipeline'
               raise SaltError("Invalid PROPID in images:"+msg)

       sdb.close()
       #process the RSS data
       rssrawsize, rssrawnum, rssprodsize, rssprodnum=processdata('rss', obsdate,  propcode, median, function, order, rej_lo, rej_hi, niter, interp,logfile, verbose)
      
       #advance process the data
       #NB: Turned off right now due to RSS being off
       if rssrawnum > 0:
           pass #advanceprocess('rss', obsdate,  propcode, median, function, order, rej_lo, rej_hi, niter, interp,sdbhost, sdbname, sdbuser, sdbpass, logfile, verbose)

       #process the SCAM data
       scmrawsize, scmrawnum, scmprodsize, scmprodnum=processdata('scam', obsdate, propcode,  median, function, order, rej_lo, rej_hi, niter, interp,logfile, verbose)

       #process the HRS data
       hrsrawsize, hrsrawnum, hrsprodsize, hrsprodnum=hrsprocess('hrs', obsdate, propcode, median, function, order, rej_lo, rej_hi, niter, interp, logfile, verbose)

       #upload the data to the database
       img_list=glob.glob(workpath+'scam/product/*bxgp*.fits')
       img_list.extend(glob.glob(workpath+'rss/product/*bxgp*.fits'))
       img_list.extend(glob.glob(workpath+'hrs/raw/*.fits'))
       if img_list:
           img=','.join('%s' %  (k) for k in img_list)
           saltsdbloadfits(images=img, sdbname=sdbname, sdbhost=sdbhost, sdbuser=sdbuser, \
                  password=sdbpass, logfile=logfile, verbose=verbose)


       #add junk sources to the database
       raw_list=glob.glob(workpath+'scam/raw/S*.fits')
       raw_list.extend(glob.glob(workpath+'rss/raw/P*.fits'))
       if raw_list:
          img=''
          for img in raw_list:
              hdu=fits.open(img)
              if hdu[0].header['PROPID'].strip()=='JUNK':
                saltsdbloadfits(images=img, sdbname=sdbname, sdbhost=sdbhost, sdbuser=sdbuser, \
                    password=sdbpass, logfile=logfile, verbose=verbose)
              hdu.close()

       # run advanced pipeline -- currently this assumes all files are in the database
       if hrsrawnum>0:
           log.message('Processing {} HRS images'.format(hrsrawnum))
           run_hrsadvance(obsdate, sdbhost, sdbname, sdbuser, sdbpass)
       

       # construct observation and pipeline documentation
       if lastrssnum > 1 and rssrawnum>0:
           rssobslog = 'rss/product/P' + obsdate + 'OBSLOG.fits'
       else:
           rssobslog = 'none'

       if lastscmnum > 1 and scmrawnum>0:
           scmobslog = 'scam/product/S' + obsdate + 'OBSLOG.fits'
       else:
           scmobslog = 'None'

       if lasthrsnum > 1 and hrsrawnum>0:
           hrsobslog = 'hrs/product/H' + obsdate + 'OBSLOG.fits'
       else:
           hrsobslog = 'None'



       if rssrawnum==0 and scmrawnum==0 and hrsrawnum==0:
           msg='No data  processed for  %s' % obsdate
           email=False
           ftp=False
           log.message(msg)

       htmlpath = '.'
       nightlog = '../nightlogs/' + obsdate + '.log'
       readme = iraf.osfn('pipetools$html/readme.template')
       if not os.path.isfile(nightlog):
           nightlog = ''
           message = 'No night log {} found'.format(nightlog)
           log.warning(message)

       if (rssrawnum > 0 or scmrawnum > 0 or hrsrawnum>0):
           salthtml(propcode=propcode,scamobslog=scmobslog,rssobslog=rssobslog, hrsobslog=hrsobslog, htmlpath=htmlpath,
                      nightlog=nightlog,readme=readme,clobber=True,logfile=logfile,
                      verbose=verbose)

       #add a pause to allow syncing of the databases
       time.sleep(10)

       #Add in the environmental information
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, sdbpass)
       if (rssrawnum > 0 or scmrawnum > 0 or hrsrawnum>0):
           propids=saltmysql.getpropcodes(sdb, obsdate)
           for pid in propids:
               try:
                  saltelsdata(pid, obsdate, elshost, elsname, elsuser, elspass,
                           sdbhost,sdbname,sdbuser, sdbpass, clobber, logfile,verbose) 
               except:
                  continue

               try:
                  outfile='%s_%s_elsdata.fits' % (pid, obsdate)
                  outdir='%s/doc/' % (pid)
                  shutil.move(outfile, outdir)
               except:
                  os.remove(outfile)
       sdb.close()

       #ftp the data
       beachdir='/salt/ftparea/'
       if ftp:
           try:
               saltftp(propcode=propcode,obsdate=obsdate, datapath=workpath,
                   password=ftppasswd,beachdir=beachdir,sdbhost=sdbhost,
                   sdbname=sdbname,sdbuser=sdbuser,splitfiles=False, 
                   cleanup=True,clobber=True,logfile=logfile, verbose=verbose)
           except Exception,e:
               message="Not able to copy data to FTP area:\n%s " % e
               raise SaltError(message)     
           #run with the splitting of files
           try: 
               saltftp(propcode=propcode,obsdate=obsdate, datapath=workpath,
                   password=ftppasswd,beachdir=beachdir,sdbhost=sdbhost,
                   sdbname=sdbname,sdbuser=sdbuser,splitfiles=True, 
                   cleanup=True,clobber=True,logfile=logfile, verbose=verbose)
           except Exception,e:
               message="Not able to copy data to FTP area:\n%s " % e
               raise SaltError(message)     
           #try moving the BVIT data
           try:
               bvitfile= iraf.osfn('pipetools$html/readme.bvit.template')
               bvitftp('ALL', obsdate, sdbhost=sdbhost,sdbname=sdbname,sdbuser=sdbuser, 
                       password=ftppasswd, server=emserver,username=emuser,sender=sender, 
                       bcc=bcc, emailfile=bvitfile, notify=True, clobber=True,
                       logfile=logfile, verbose=verbose)
           except Exception, e:
               message="ERROR: Not able to copy BVIT to FTP area:\n%s " % e
               print SaltError(message)     
               saltio.email(emserver,emuser,ftppassword,sender,'crawford@saao.ac.za','', 'BVIT Died',message)
           



       #send notifications if emails have not already been sent
       #first check to see if emails have been sent
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, sdbpass)
       if email:
          record=saltmysql.select(sdb, 'EmailSent', 'PipelineStatistics', 'NightInfo_Id=%i' % nightinfoid)
          if len(record)>0:
             if record[0][0]==1:  
               email=False
               log.warning("According to the Sdb, notifications have already been sent for %s." % obsdate)

       if int(obsdate)<20110901:
           email=False
           log.warning("Emails will not be sent for data taken before 20110901")
       
       if email:
           try:
               #send email
               saltemail(propcode=propcode, obsdate=obsdate, readme=readme, server=emserver,username=emuser,
                   password=empasswd, bcc=bcc, sdbhost=sdbhost, sdbname=sdbname,sdbuser=sdbuser,
                   logfile=logfile, verbose=verbose)
           except Exception,e:
               message="Not able to send notification emails:\n%s " % e
               raise SaltError(message)     

           #update pipeline status
           pipelinestatus(obsdate, 'Email', message=None, rawsize=None, reducedsize=None, runtime=None, emailsent=1,
               sdbhost=sdbhost, sdbname=sdbname, sdbuser=sdbuser, password=sdbpass, logfile=logfile, verbose=verbose)


       #Calculate the amount of time it took to process
       processing_time = time.time() - starttime

       #caculate the total amount data produced
       rawsize=rssrawsize + scmrawsize + hrsrawsize
       prodsize=rssprodsize + scmprodsize + hrsprodsize

       #update the pipeline status
       if runstatus: 
           pipelinestatus(obsdate, 'Reduced', message=None, rawsize=rawsize, reducedsize=prodsize, runtime=processing_time,
                      sdbhost=sdbhost, sdbname=sdbname, sdbuser=sdbuser, password=sdbpass, logfile=logfile, verbose=verbose)

       

       #format the different outputs
       rawsize, rawunit=calcsizeunit(rawsize)
       prodsize, produnit=calcsizeunit(prodsize)
       if (processing_time < 3600):
          processing_time = int(processing_time / 60 + 0.5)
          time_unit = ' min'
       else:
          processing_time = float(int(processing_time / 360 + 0.5)) / 10
          time_unit = ' hrs'
       #output the message with this information
       message = 'Pipeline Statistics:\n'
       message += 'Processing time: %s %s\n' %  (str(processing_time), time_unit)
       message += 'Number of files: %i\n' %  (rssrawnum+scmrawnum+hrsrawnum)
       message += 'Total Raw Data: %s %s\n' % (rawsize, rawunit)
       message += 'Total Product Data: %s %s\n' % (prodsize, produnit)
       #message += 'Number of Proposals: \n'  % (1)
       log.message(message)
   sdb.close()

   #return to the original working directory
   saltio.changedir(basedir)


def calcsizeunit(size):
    if (size < 1.e6):
        rawsize = str(int(size / 1e3))
        rawunit = 'KB'
    elif (size >= 1.e6 and size < 1.e9):
        rawsize = str(int(size / 1e6))
        rawunit = 'MB'
    else:
        rawsize = str(float(int(size / 1e8 + 0.5)) / 10)
        rawunit = 'GB'
    return rawsize, rawunit

 


def runcheckforpropid(imlist, propids, log):
    pstatus=True 
    imlist.sort()
    for image in imlist:
       try:
           checkforpropid(image,propids)
       except SaltError, e:
           pstatus=False
           log.message(str(e), with_header=False)
    return pstatus


def checkforpropid(image,propids):
    """subroutine to check to see if the propid keyword exists

       returns status
    """

    #open up the image
    struct=fits.open(image, mode='update')
    if struct:
        #get proposal code
        propid=saltkey.get('PROPID', struct[0])

        #check to see if it is none
        if saltio.checkfornone(propid) is None or propid is 'None':
           message='\nNo value in PROPID keyword for %s' % image
           raise SaltError(message)
 
        #check to see if it is an eng or cal proposal
        if propid.count('ENG_') or propid.count('CAL_'):
           return
 
        #clean up junk ones
        if propid in ['BIAS','COMMON','JUNK','TEST','UNKNOWN']:
           return 

        #check to see if it is in the sdb
        if propid not in propids:
           message='\n%s for PROPID keyword for %s is invalid' % (propid, image)
           raise SaltError(message)

def makerawdir(obsdate, instr):
    rawdir='/salt/%s/data/%s/%s/raw/' % (instr, obsdate[0:4], obsdate[4:])
    if (not os.path.exists(rawdir)):
           message  = 'Raw data path does not exist: %s' % rawdir
           raise SaltError(message)
    return rawdir

def checkfordata(rawpath, prefix, obsdate, log):
   """Check to see if the data have downloaded correctly"""
   lastnum=1
   saltio.fileexists(rawpath+'disk.file')
   content = saltio.openascii(rawpath+'disk.file','r')
   for line in content:
       lastnum = saltstring.filenumber(line)
   saltio.closeascii(content)
   lastfile = saltstring.filename(prefix,obsdate,lastnum-1)

   if lastnum==1: return lastnum

   #check to see that the data are present
   if (os.path.isfile(rawpath+lastfile) or os.path.isfile(rawpath+lastfile.replace('fits','bin'))):
       message  = 'Data download complete for %s\n' % rawpath
       log.message(message)
   else:
       message  = 'Data download incomplete to %s' % rawpath
       log.error(message)

   return lastnum

def convertbin(inpath, fitsconfig, logfile, verbose):
    if len(glob.glob(inpath+'/*.bin')) > 0:
        saltbin2fit(inpath=inpath,outpath=inpath,cleanup=True,fitsconfig=fitsconfig,logfile=logfile,verbose=verbose)
        for bfile in glob.glob(inpath+'/*.bin'):
            saltio.delete(bfile)
            ffile=bfile.replace('bin', 'fits')
            slotreadtimefix(ffile, ffile, '', clobber=True, logfile=logfile, verbose=verbose)
         

def preprocessdata(instrume, prefix,  obsdate, keyfile, log, logfile, verbose):
   """Run through all of the processing of the individual data files"""

   log.message('Beginning pre-processing of %s data' % instrume.upper())

   #set up the input path
   inpath=instrume+'/raw/'

   #creaate the product directory
   prodpath=instrume+'/product/'
   saltio.createdir(prodpath)

   # convert any slot mode binary data to FITS
   convertbin(inpath, iraf.osfn('pysalt$data/%s/%s_fits.config' % (instrume, instrume)), logfile, verbose)


   # fix sec keywords for data of unequal binning obtained before 2006 Aug 12
   if int(obsdate) < 20060812:
       pinfiles = instrume+'/raw/*.fits'
       log.message('Fixing SEC keywords in older data')
       log.message('SALTFIXSEC -- infiles=' + pinfiles)
       pipetools.saltfixsec(infiles=pinfiles)

   #fix the key words for the data set
   recfile = prodpath+prefix+ obsdate + 'KEYLOG.fits'
   img=','.join(glob.glob(inpath+prefix+'*fits'))
   if img:
       salteditkey(images=img,outimages=img,outpref='',keyfile=keyfile,recfile=recfile,
                       clobber=True,logfile=logfile,verbose=verbose)

def hrsprocess(instrume, obsdate,propcode, median, function, order, rej_lo, rej_hi, niter, interp,  logfile, verbose):
   """Clean and process HRS data"""
   from saltobslog import saltobslog

   prefix = 'H'
   rawpath = instrume+'/raw'
   prodpath = instrume+'/product'
   img_list=[]
   for img in glob.glob('%s/raw/%s*fits' % (instrume, 'H')):
        struct=fits.open(img)
        if struct[0].header['PROPID'].upper().strip() != 'JUNK':
           img_list.append(img)
        struct.close()
   for img in glob.glob('%s/raw/%s*fits' % (instrume, 'R')):
        struct=fits.open(img)
        if struct[0].header['PROPID'].upper().strip() != 'JUNK':
           img_list.append(img)
        struct.close()

   if len(img_list)>0:
      img_str=','.join(img_list)
      obslog = '%s/%s%sOBSLOG.fits' % (prodpath, prefix, obsdate)
      hrsclean(images=img_str,outpath=prodpath,obslogfile=obslog,
               subover=True,trim=True, median=median,function=function,order=order,rej_lo=rej_lo,
               rej_hi=rej_hi,niter=niter,masbias=True,subbias=False,interp=interp,
               clobber=True,logfile=logfile,verbose=verbose)

   rawsize = 0.
   rawnum = 0
   prodsize = 0.
   prodnum = 0
   if len(img_list)>0:
       files = glob.glob('%s/raw/%s*.fits' % (instrume, prefix))
       rawnum = len(files)
       for infile in files:
           rawsize += os.stat(infile).st_size
       files = glob.glob('%s/product/*%s*.fits' % (instrume, prefix))
       prodnum = len(files)
       for infile in files:
           prodsize += os.stat(infile).st_size

   # collate HRS data for individual PIs
   outpath = '.'
   if len(img_list)>0:
       saltobsid(propcode=propcode,obslog=obslog,rawpath=rawpath,prodpath=prodpath, outpath=outpath, prefix='mbgph', fprefix='bgph',clobber=True,logfile=logfile,verbose=verbose)


   return  rawsize, rawnum, prodsize, prodnum

   
def advanceprocess(instrume, obsdate, propcode, median, function, order, rej_lo, rej_hi, niter, interp, sdbhost, sdbname, sdbuser, sdbpass, logfile, verbose):
   """Advance process the rss data"""
   rawpath = instrume+'/raw'
   prodpath = instrume+'/advance'
   os.mkdir(prodpath)
   instrume_name='RSS'
   prefix = 'P'
   img_list=[]
   for img in glob.glob('%s/raw/%s*fits' % (instrume, prefix)):
        struct=fits.open(img)
        if struct[0].header['PROPID'].upper().strip() != 'JUNK':
           img_list.append(img)
        struct.close()
   images=','.join(img_list)
   obslog = '%s/%s%sOBSLOG.fits' % (prodpath, prefix, obsdate)
   gaindb = iraf.osfn('pysalt$data/%s/%samps.dat' % (instrume, instrume_name))
   xtalkfile = iraf.osfn('pysalt$data/%s/%sxtalk.dat' % (instrume, instrume_name))
   geomfile = iraf.osfn('pysalt$data/%s/%sgeom.dat' % (instrume, instrume_name))

   saltadvance(images, prodpath, obslogfile=obslog, gaindb=gaindb, xtalkfile=xtalkfile,
        geomfile=geomfile, subover=True,trim=True,masbias=None,
        subbias=False, median=False, function='polynomial', order=5,rej_lo=3,
        rej_hi=3, niter=5,interp='linear',  sdbhost=sdbhost, sdbname=sdbname,sdbuser=sdbuser, password=sdbpass,
        clobber=True, logfile=logfile, verbose=verbose)

   
   return

def processdata(instrume, obsdate, propcode, median, function, order, rej_lo, rej_hi, niter, interp, logfile, verbose):
   """Clean and process the data"""

   #set up instrument specific naming
   if instrume=='rss':
      instrume_name='RSS'
      prefix='P'
   elif instrume=='scam':
      instrume_name='SALTICAM'
      prefix='S'

   rawpath = instrume+'/raw'
   prodpath = instrume+'/product'
   img_list=[]
   for img in glob.glob('%s/raw/%s*fits' % (instrume, prefix)):
        struct=fits.open(img)
        if struct[0].header['PROPID'].upper().strip() != 'JUNK':
           img_list.append(img)
        struct.close()
   img_str=','.join(img_list)
   obslog = '%s/%s%sOBSLOG.fits' % (prodpath, prefix, obsdate)
   gaindb = iraf.osfn('pysalt$data/%s/%samps.dat' % (instrume, instrume_name))
   #gaindb = ''
   xtalkfile = iraf.osfn('pysalt$data/%s/%sxtalk.dat' % (instrume, instrume_name))
   geomfile = iraf.osfn('pysalt$data/%s/%sgeom.dat' % (instrume, instrume_name))
   if len(img_list)>0:
        saltclean(images=img_str,outpath=prodpath,obslogfile=obslog,gaindb=gaindb,
                       xtalkfile=xtalkfile,geomfile=geomfile,subover=True,trim=True,
                       median=median,function=function,order=order,rej_lo=rej_lo,
                       rej_hi=rej_hi,niter=niter,masbias=True,subbias=False,interp=interp,
                       clobber=True,logfile=logfile,verbose=verbose)

   rawsize = 0.
   rawnum = 0
   prodsize = 0.
   prodnum = 0
   if len(img_list)>0:
       files = glob.glob('%s/raw/%s*.fits' % (instrume, prefix))
       rawnum = len(files)
       for file in files:
           rawsize += os.stat(file).st_size
       files = glob.glob('%s/product/*%s*.fits' % (instrume, prefix))
       prodnum = len(files)
       for file in files:
           prodsize += os.stat(file).st_size

   # collate RSS data for individual PIs
   outpath = '.'
   if len(img_list):
       saltobsid(propcode=propcode,obslog=obslog,rawpath=rawpath,prodpath=prodpath, outpath=outpath,clobber=True,logfile=logfile,verbose=verbose)
     
   return  rawsize, rawnum, prodsize, prodnum


def run_hrsadvance(obsdate, sdbhost, sdbname, sdbuser, sdbpass):
    #os.system('/usr/bin/env python  /home/sa/smc/hrs/run_hrsadvance.py  -c -m {} '.format(obsdate))
    from hrsadvance import hrsbias, run_science, run_hrsflat, run_hrsarcs

    rawpath = os.getcwd() + '/hrs/raw/'
    outpath = os.getcwd() + '/hrs/product/'
    symdir = './'
    mfs = None
    link = False
    nlim = 180

    port = 3306
    sdb = mysql(sdbhost,sdbname,sdbuser,sdbpass, port=port)

    # run the bias frames
    hrsbias(rawpath, outpath, clobber=True, sdb=sdb, link=link)

    # run the flat frames
    run_hrsflat(obsdate, rawpath, outpath, sdb=sdb, nlim=nlim, link=link)

    # run the arcs
    run_hrsarcs(obsdate,  rawpath, outpath, nlim=nlim, sdb=sdb, link=link)
    
    # run the science frames
    run_science(obsdate, rawpath=rawpath, outpath=outpath, sdb=sdb, symdir=symdir, mfs=mfs)

 
# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltpipe.par")
t = iraf.IrafTaskFactory(taskname="saltpipe",value=parfile,function=saltpipe, pkgname='pipetools')
