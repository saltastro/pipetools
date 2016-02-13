################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

#!/usr/bin/env python

"""
SALTADVANCE performs advanced data reduction of the SALT data.  It also
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
from astropy.io import fits

from pyraf import iraf
from pyraf.iraf import pysalt

from saltobslog import obslog, createobslogfits
from saltprepare import prepare, CreateVariance, createbadpixel
from saltgain import gain as rungain
from saltxtalk  import xtalk
from saltbias import bias
from saltflat import flat
from saltcrclean import multicrclean
from saltcombine import saltcombine 
from saltmosaic import saltmosaic

from specidentify import specidentify
from specrectify import specrectify

import saltstat
import saltsafestring as saltstring
import saltsafekey as saltkey
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging, history

from salterror import SaltError

debug=True

biasheader_list=['INSTRUME', 'DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED', 'NWINDOW']
flatheader_list=['INSTRUME', 'DETMODE', 'CCDSUM', 'GAINSET', 'ROSPEED', 'FILTER', 'GRATING', 'GR-ANGLE', 'AR-ANGLE', 'NWINDOW']


# -----------------------------------------------------------
# core routine

def saltadvance(images, outpath, obslogfile=None, gaindb=None,xtalkfile=None, 
	geomfile=None,subover=True,trim=True,masbias=None, 
        subbias=False, median=False, function='polynomial', order=5,rej_lo=3,
        rej_hi=3,niter=5,interp='linear',  sdbhost='',sdbname='',sdbuser='', password='',
        clobber=False, cleanup=True, logfile='salt.log', verbose=True):
   """SALTADVANCE provides advanced data reductions for a set of data.  It will 
      sort the data, and first process the biases, flats, and then the science 
      frames.  It will record basic quality control information about each of 
      the steps.
   """
   plotover=False

   #start logging
   with logging(logfile,debug) as log:

       # Check the input images 
       infiles = saltio.argunpack ('Input',images)
       infiles.sort()

       # create list of output files 
       outpath=saltio.abspath(outpath)

       #log into the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)

       #does the gain database file exist
       if gaindb:
           dblist= saltio.readgaindb(gaindb)
       else:
           dblist=[]

       # does crosstalk coefficient data exist
       if xtalkfile:
           xtalkfile = xtalkfile.strip()
           xdict = saltio.readxtalkcoeff(xtalkfile)
       else:
           xdict=None
       #does the mosaic file exist--raise error if no
       saltio.fileexists(geomfile)


       # Delete the obslog file if it already exists
       if os.path.isfile(obslogfile) and clobber: saltio.delete(obslogfile)

       #read in the obsveration log or create it
       if os.path.isfile(obslogfile):
           msg='The observing log already exists.  Please either delete it or run saltclean with clobber=yes'
           raise SaltError(msg)
       else:
           headerDict=obslog(infiles, log)
           obsstruct=createobslogfits(headerDict)
           saltio.writefits(obsstruct, obslogfile)

       #create the list of bias frames and process them
       filename=obsstruct.data.field('FILENAME')
       detmode=obsstruct.data.field('DETMODE')
       obsmode=obsstruct.data.field('OBSMODE')
       ccdtype=obsstruct.data.field('CCDTYPE')
       propcode=obsstruct.data.field('PROPID')
       masktype=obsstruct.data.field('MASKTYP')

       #set the bias list of objects
       biaslist=filename[(ccdtype=='ZERO')*(propcode=='CAL_BIAS')]
       masterbias_dict={}
       for img in infiles:
           if os.path.basename(img) in biaslist:
               #open the image
               struct=fits.open(img)
               bimg=outpath+'bxgp'+os.path.basename(img)

               #print the message
               if log:
                   message='Processing Zero frame %s' % img
                   log.message(message, with_stdout=verbose)

               #process the image
               struct=clean(struct, createvar=True, badpixelstruct=None, mult=True, 
                            dblist=dblist, xdict=xdict, subover=subover, trim=trim, subbias=False,
                            bstruct=None, median=median, function=function, order=order,
                            rej_lo=rej_lo, rej_hi=rej_hi, niter=niter, plotover=plotover, log=log,
                            verbose=verbose)
 
               #update the database
               updatedq(os.path.basename(img), struct, sdb)

               #write the file out
               # housekeeping keywords
               fname, hist=history(level=1, wrap=False, exclude=['images', 'outimages', 'outpref'])
               saltkey.housekeeping(struct[0],'SPREPARE', 'Images have been prepared', hist)
               saltkey.new('SGAIN',time.asctime(time.localtime()),'Images have been gain corrected',struct[0])
               saltkey.new('SXTALK',time.asctime(time.localtime()),'Images have been xtalk corrected',struct[0])
               saltkey.new('SBIAS',time.asctime(time.localtime()),'Images have been de-biased',struct[0])

               # write FITS file
               saltio.writefits(struct,bimg, clobber=clobber)
               saltio.closefits(struct)

               #add files to the master bias list
               masterbias_dict=compareimages(struct, bimg, masterbias_dict, keylist=biasheader_list)

       #create the master bias frame
       for i in masterbias_dict.keys():
           bkeys=masterbias_dict[i][0]
           blist=masterbias_dict[i][1:]
           mbiasname=outpath+createmasterbiasname(blist, bkeys)
           bfiles=','.join(blist)
           saltcombine(bfiles, mbiasname, method='median', reject='sigclip', mask=False, 
                       weight=False, blank=0, scale=None, statsec=None, lthresh=3,    \
                       hthresh=3, clobber=False, logfile=logfile,verbose=verbose)

           

       #create the list of flatfields and process them
       flatlist=filename[ccdtype=='FLAT']
       masterflat_dict={}
       for img in infiles:
           if os.path.basename(img) in flatlist:
               #open the image
               struct=fits.open(img)
               fimg=outpath+'bxgp'+os.path.basename(img)

               #print the message
               if log:
                   message='Processing Flat frame %s' % img
                   log.message(message, with_stdout=verbose)

               #process the image
               struct=clean(struct, createvar=True, badpixelstruct=None, mult=True, 
                            dblist=dblist, xdict=xdict, subover=subover, trim=trim, subbias=False,
                            bstruct=None, median=median, function=function, order=order,
                            rej_lo=rej_lo, rej_hi=rej_hi, niter=niter, plotover=plotover, log=log,
                            verbose=verbose)

               #update the database
               updatedq(os.path.basename(img), struct, sdb)

               #write the file out
               # housekeeping keywords
               fname, hist=history(level=1, wrap=False, exclude=['images', 'outimages', 'outpref'])
               saltkey.housekeeping(struct[0],'SPREPARE', 'Images have been prepared', hist)
               saltkey.new('SGAIN',time.asctime(time.localtime()),'Images have been gain corrected',struct[0])
               saltkey.new('SXTALK',time.asctime(time.localtime()),'Images have been xtalk corrected',struct[0])
               saltkey.new('SBIAS',time.asctime(time.localtime()),'Images have been de-biased',struct[0])

               # write FITS file
               saltio.writefits(struct,fimg, clobber=clobber)
               saltio.closefits(struct)

               #add files to the master bias list
               masterflat_dict=compareimages(struct, fimg, masterflat_dict,  keylist=flatheader_list)

       #create the master flat frame
       for i in masterflat_dict.keys():
           fkeys=masterflat_dict[i][0]
           flist=masterflat_dict[i][1:]
           mflatname=outpath+createmasterflatname(flist, fkeys)
           ffiles=','.join(flist)
           saltcombine(ffiles, mflatname, method='median', reject='sigclip', mask=False, 
                       weight=False, blank=0, scale=None, statsec=None, lthresh=3,    \
                       hthresh=3, clobber=False, logfile=logfile,verbose=verbose)

       #process the arc data
       arclist=filename[(ccdtype=='ARC') * (obsmode=='SPECTROSCOPY') * (masktype=='LONGSLIT')]
       for i, img in enumerate(infiles):
           nimg=os.path.basename(img)
           if nimg in arclist:
               #open the image
               struct=fits.open(img)
               simg=outpath+'bxgp'+os.path.basename(img)
               obsdate=os.path.basename(img)[1:9]

               #print the message
               if log:
                   message='Processing ARC frame %s' % img
                   log.message(message, with_stdout=verbose)


               struct=clean(struct, createvar=False, badpixelstruct=None, mult=True, 
                            dblist=dblist, xdict=xdict, subover=subover, trim=trim, subbias=False,
                            bstruct=None, median=median, function=function, order=order,
                            rej_lo=rej_lo, rej_hi=rej_hi, niter=niter, plotover=plotover, 
                            log=log, verbose=verbose)

               # write FITS file
               saltio.writefits(struct,simg, clobber=clobber)
               saltio.closefits(struct)

               #mosaic the images
               mimg=outpath+'mbxgp'+os.path.basename(img)
               saltmosaic(images=simg, outimages=mimg,outpref='',geomfile=geomfile,
                    interp=interp,cleanup=True,clobber=clobber,logfile=logfile,
                    verbose=verbose)

               #remove the intermediate steps
               saltio.delete(simg)


               #measure the arcdata
               arcimage=outpath+'mbxgp'+nimg
               dbfile=outpath+obsdate+'_specid.db'
               lamp = obsstruct.data.field('LAMPID')[i]
               lamp = lamp.replace(' ', '')
               lampfile = iraf.osfn("pysalt$data/linelists/%s.salt" % lamp)
               print arcimage, lampfile, os.getcwd()
               specidentify(arcimage, lampfile, dbfile, guesstype='rss', 
                                guessfile='', automethod='Matchlines', function='legendre',
                                order=3, rstep=100, rstart='middlerow', mdiff=20, thresh=3,
                                startext=0, niter=5, smooth=3, inter=False, clobber=True, logfile=logfile, 
                                verbose=verbose)
               try:
                   ximg = outpath+'xmbxgp'+os.path.basename(arcimage)
                   specrectify(images=arcimage, outimages=ximg, outpref='', solfile=dbfile, caltype='line',
                              function='legendre', order=3, inttype='interp', w1=None, w2=None, dw=None,
                              nw=None, blank=0.0, conserve=True, nearest=True, clobber=True,
                              logfile=logfile, verbose=verbose)
               except:
                   pass


              
       #process the science data
       for i, img in enumerate(infiles):
           nimg=os.path.basename(img)
           if not (nimg in flatlist or nimg in biaslist or nimg in arclist):
     
               #open the image
               struct=fits.open(img)
               if struct[0].header['PROPID'].count('CAL_GAIN'): continue
               simg=outpath+'bxgp'+os.path.basename(img)
   

               #print the message
               if log:
                   message='Processing science frame %s' % img
                   log.message(message, with_stdout=verbose)


               #Check to see if it is RSS 2x2 and add bias subtraction
               instrume=saltkey.get('INSTRUME', struct[0]).strip()
               gainset = saltkey.get('GAINSET', struct[0])    
               rospeed = saltkey.get('ROSPEED', struct[0])    
               target = saltkey.get('OBJECT', struct[0]).strip()
               exptime = saltkey.get('EXPTIME', struct[0])
               obsmode = saltkey.get('OBSMODE', struct[0]).strip()
               detmode = saltkey.get('DETMODE', struct[0]).strip()
               masktype = saltkey.get('MASKTYP', struct[0]).strip()
  
               
               xbin, ybin = saltkey.ccdbin( struct[0], img)
               obsdate=os.path.basename(img)[1:9]
               bstruct=None
               crtype=None
               thresh=5 
               mbox=11 
               bthresh=5.0,
               flux_ratio=0.2 
               bbox=25 
               gain=1.0 
               rdnoise=5.0 
               fthresh=5.0 
               bfactor=2
               gbox=3 
               maxiter=5
    
               subbias=False
               if instrume=='RSS' and gainset=='FAINT' and rospeed=='SLOW':
                   bfile='P%sBiasNM%ix%iFASL.fits' % (obsdate, xbin, ybin)
                   if os.path.exists(bfile):
                      bstruct=fits.open(bfile)
                      subbias=True
                   if detmode=='Normal' and target!='ARC' and xbin < 5 and ybin < 5:
                       crtype='edge' 
                       thresh=5 
                       mbox=11 
                       bthresh=5.0,
                       flux_ratio=0.2 
                       bbox=25 
                       gain=1.0 
                       rdnoise=5.0 
                       fthresh=5.0 
                       bfactor=2
                       gbox=3 
                       maxiter=3
    
               #process the image
               struct=clean(struct, createvar=True, badpixelstruct=None, mult=True, 
                            dblist=dblist, xdict=xdict, subover=subover, trim=trim, subbias=subbias,
                            bstruct=bstruct, median=median, function=function, order=order,
                            rej_lo=rej_lo, rej_hi=rej_hi, niter=niter, plotover=plotover, 
                            crtype=crtype,thresh=thresh,mbox=mbox, bbox=bbox,      \
                            bthresh=bthresh, flux_ratio=flux_ratio, gain=gain, rdnoise=rdnoise, 
                            bfactor=bfactor, fthresh=fthresh, gbox=gbox, maxiter=maxiter,
                            log=log, verbose=verbose)

               

               #update the database
               updatedq(os.path.basename(img), struct, sdb)

               #write the file out
               # housekeeping keywords
               fname, hist=history(level=1, wrap=False, exclude=['images', 'outimages', 'outpref'])
               saltkey.housekeeping(struct[0],'SPREPARE', 'Images have been prepared', hist)
               saltkey.new('SGAIN',time.asctime(time.localtime()),'Images have been gain corrected',struct[0])
               saltkey.new('SXTALK',time.asctime(time.localtime()),'Images have been xtalk corrected',struct[0])
               saltkey.new('SBIAS',time.asctime(time.localtime()),'Images have been de-biased',struct[0])

               # write FITS file
               saltio.writefits(struct,simg, clobber=clobber)
               saltio.closefits(struct)

               #mosaic the files--currently not in the proper format--will update when it is
               if not saltkey.fastmode(saltkey.get('DETMODE', struct[0])):
                   mimg=outpath+'mbxgp'+os.path.basename(img)
                   saltmosaic(images=simg, outimages=mimg,outpref='',geomfile=geomfile,
                        interp=interp,fill=True, cleanup=True,clobber=clobber,logfile=logfile,
                        verbose=verbose)

                   #remove the intermediate steps
                   saltio.delete(simg)

               #if the file is spectroscopic mode, apply the wavelength correction
               if obsmode == 'SPECTROSCOPY' and masktype.strip()=='LONGSLIT':
                  dbfile=outpath+obsdate+'_specid.db'
                  try:
                     ximg = outpath+'xmbxgp'+os.path.basename(img)
                     specrectify(images=mimg, outimages=ximg, outpref='', solfile=dbfile, caltype='line', 
                              function='legendre', order=3, inttype='interp', w1=None, w2=None, dw=None,
                              nw=None, blank=0.0, conserve=True, nearest=True, clobber=True, 
                              logfile=logfile, verbose=verbose)
                  except Exception, e:
                     log.message('%s' % e)


       #clean up the results
       if cleanup:
          #clean up the bias frames
          for i in masterbias_dict.keys():
               blist=masterbias_dict[i][1:]
               for b in blist: saltio.delete(b)

          #clean up the flat frames
          for i in masterflat_dict.keys():
               flist=masterflat_dict[i][1:]
               for f in flist: saltio.delete(f)


def updatedq(img, struct, sdb):
   """Add information about the image to the database
   """
 
   #get the filenumber
   #check to see if the FileData was created
   logic="FileName='%s'" % img
   records=saltmysql.select(sdb,'FileData_Id','FileData',logic)
   try:
       FileData_Id=records[0][0]
   except:
       message='WARNING:  File not yet in database'
       print message
       return


   #get the information from the image
   for i in range(1,len(struct)):
     if struct[i].name=='SCI':
       try:
           omean=struct[i].header['OVERSCAN']
       except:
           omean=None
       try:
           orms=struct[i].header['OVERRMS']
       except:
           orms=None
       #lets measureme the statistics in a 200x200 box in each image
       try:
          my,mx=struct[i].data.shape
          dx1=int(mx*0.5)
          dx2=min(mx,dx1+200)
          dy1=int(my*0.5)
          dy2=min(my,dy1+200)
          mean,med,sig=saltstat.iterstat(struct[i].data[dy1:dy2,dx1:dx2], 5, 5)
       except:
          mean, med, sig=(None, None, None)

       #update the database with this information
       #check to see if you need to update or insert
       record=saltmysql.select(sdb, 'FileData_Id', 'PipelineDataQuality_CCD', 'FileData_Id=%i and Extension=%i' % (FileData_Id, i))
       update=False
       if record: update=True
 
       ins_cmd=''
       if omean is not None: ins_cmd='OverscanMean=%s,' % omean
       if orms is not None:  ins_cmd+='OverscanRms=%s,' % orms
       if mean is not None:  ins_cmd+='BkgdMean=%f,' % mean
       if sig is not None:   ins_cmd+='BkgdRms=%f' % sig 
       if update:
          ins_cmd=ins_cmd.rstrip(',')    
          saltmysql.update(sdb, ins_cmd, 'PipelineDataQuality_CCD', 'FileData_Id=%i and Extension=%i' % (FileData_Id, i))
       else:
          ins_cmd+=',FileData_Id=%i, Extension=%i' % (FileData_Id, i)
          saltmysql.insert(sdb, ins_cmd, 'PipelineDataQuality_CCD')

def compareimages(struct, oimg, imdict, keylist):
   """See if the current structure is held in the dictionary of images.  If it is,
       then add it to the list.  If it isn't then create a new entry
   """
   #create the list of header parameters
   klist=[]
   for k in keylist:
       try:
           value=str(struct[0].header[k]).strip()
       except:
           value=''
       klist.append(value)

   if len(imdict)==0: 
       imdict[oimg]=[klist, oimg]
       return imdict

   #compare each value of imdict to the structure
   for i in imdict.keys():
       if klist==imdict[i][0]:
          imdict[i].append(oimg)
          return imdict

   #create a new one if it isn't found
   imdict[oimg]=[klist, oimg]
       
   return imdict
 

def clean(struct, createvar=False, badpixelstruct=None, mult=True, dblist=None, ampccd=2,
          xdict=[], subover=True,trim=True, subbias=False, bstruct=None,
         median=False, function='polynomial',order=3,rej_lo=3,rej_hi=3,niter=10,
         crtype=None,thresh=5,mbox=3, bbox=11,        \
         bthresh=3, flux_ratio=0.2, gain=1, rdnoise=5, bfactor=2, fthresh=5,\
         gbox=3, maxiter=5,  \
         plotover=False, log=None, verbose=True):


   infile=struct

   #prepare the files
   struct=prepare(struct, createvar=False, badpixelstruct=None)

   #bias correct the file
   struct=bias(struct,subover=subover, trim=trim, subbias=subbias,
               bstruct=bstruct, median=median, function=function,
               order=order, rej_lo=rej_lo, rej_hi=rej_hi, niter=niter,
               plotover=plotover, log=log, verbose=verbose)


   #reset the names in the structures
   for i in range(1,len(struct)):
       struct[i].name=struct[i].header['EXTNAME']

   #add the variance and bad pixel frame
   nextend=len(struct)-1
   nsciext=nextend
   if createvar:
       #create the inv. variance frames

       for i in range(1, nsciext+1):
           hdu=CreateVariance(struct[i], i, nextend+i)
           try:
               pass
           except Exception, e:
               msg='Cannot create variance frame in extension %i of  %s because %s' % (nextend+i, infile, e)
               raise SaltError(msg)
           struct[i].header['VAREXT'] = (nextend+i, 'Extension for Variance Frame')
           struct.append(hdu)
       nextend+=nsciext

       #create the badpixelframes
       for i in range(1, nsciext+1):
           try:
               hdu=createbadpixel(struct, badpixelstruct, i, nextend+i)
           except Exception, e:
               msg='Could not create bad pixel extension in ext %i of %s because %s' % (nextend+i, infile, e)
               raise SaltError(msg)
           struct[i].header['BPMEXT'] = (nextend+i, 'Extension for Bad Pixel Mask')
           struct.append(hdu)
       nextend+=nsciext

   #update the number of extensions
   saltkey.new('NSCIEXT',nsciext,'Number of science extensions', struct[0])
   saltkey.new('NEXTEND',nextend,'Number of data extensions', struct[0])


   #gain correct the files
   usedb=False
   if dblist:  usedb=True
   struct=rungain(struct, mult=mult,usedb=usedb, dblist=dblist, ampccd=ampccd, log=log, verbose=verbose)

   #xtalk correct the files
   usedb=False
   if xdict:  
       obsdate=saltkey.get('DATE-OBS', struct[0])
       try:
           obsdate=int('%s%s%s' % (obsdate[0:4],obsdate[5:7], obsdate[8:]))
           xkey=np.array(xdict.keys())
           date=xkey[abs(xkey-obsdate).argmin()]
           xcoeff=xdict[date]
       except Exception,e : 
           msg='WARNING--Can not find xtalk coefficient for %s because %s' % (e, infile)
           if log: log.warning(msg)
           xcoeff=xdict[xdict.keys()[-1]]
   else:
       xcoeff=[]
   struct = xtalk(struct, xcoeff, log=log, verbose=verbose)

   #bias correct the files
   if saltkey.fastmode(saltkey.get('DETMODE', struct[0])): order=1

   #crclean the files
   if crtype is not None:
      struct=multicrclean(struct, crtype, thresh, mbox, bbox, bthresh, flux_ratio, \
                          gain, rdnoise, bfactor, fthresh, gbox, maxiter, log=log, verbose=True)

   #mosaic correct the files


   return struct

def createmasterbiasname(infiles, biaskeys):
    """Create the name for the master bias file based on its parameters.  The format for 
       hte name is 
       [S/P][YYYYMMDD]Bias[MODE][BINNING][GAINSET][ROSPEED].fits
    
       where the following abbreviations are used:
      
       [S/P]--Scam or RSS
       [YYYYMMDD]--obsdate of the data or most common obsdate if multiple dates
       [MODE]--Mode of the observations:
               Normal: NM
               Framte Transfer: FT
               Slot Mode:   SL
               Drift Scanning: DS
       [BINNING]--CCD binning in XBINxYBIN
       [GAINSET]--Gain setting
                  Bright: BR
                  Faint: FA
       [ROSPEED]--Read out speed
                  FAST: FA
                  SLOW: SL
       
    """
    
    #setup the in the instrument
    instr=saltstring.makeinstrumentstr(biaskeys[0])
    
    #setup the obsdate--assumes fixed naming scheme
    obsdate=saltstring.makeobsdatestr(infiles)
        
    #if len(obsdate)<4: obsdate=''
    print obsdate

    #set the mode string
    mdstr=saltstring.makedetmodestr(biaskeys[1])
 
    #set binning
    binstr=saltstring.makebinstr(biaskeys[2])

    #set gain
    gnstr=saltstring.makegainstr(biaskeys[3])

    #set readout
    rostr=saltstring.makereadoutstr(biaskeys[4])

    biasname='%s%sBias%s%s%s%s.fits' % (instr, obsdate, mdstr, binstr, gnstr, rostr)
    return biasname


def createmasterflatname(infiles, flatkeys):
    """Create the name for the master flat file based on its parameters.  The format for 
       hte name is 
       [S/P][YYYYMMDD]Flat[MODE][BINNING][GAINSET][ROSPEED][FILTER].fits
    
       where the following abbreviations are used:
      
       [S/P]--Scam or RSS
       [YYYYMMDD]--obsdate of the data or most common obsdate if multiple dates
       [MODE]--Mode of the observations:
               Normal: NM
               Framte Transfer: FT
               Slot Mode:   SL
               Drift Scanning: DS
       [BINNING]--CCD binning in XBINxYBIN
       [GAINSET]--Gain setting
                  Bright: BR
                  Faint: FA
       [ROSPEED]--Read out speed
                  FAST: FA
                  SLOW: SL
       [FILTER]--Filter used
       
    """
    #setup the in the instrument
    instr=saltstring.makeinstrumentstr(flatkeys[0])
    
    #setup the obsdate--assumes fixed naming scheme
    obsdate=saltstring.makeobsdatestr(infiles)
        
    #if len(obsdate)<4: obsdate=''
    print obsdate

    #set the mode string
    mdstr=saltstring.makedetmodestr(flatkeys[1])
 
    #set binning
    binstr=saltstring.makebinstr(flatkeys[2])

    #set gain
    gnstr=saltstring.makegainstr(flatkeys[3])

    #set readout
    rostr=saltstring.makereadoutstr(flatkeys[4])
    
    fltstr=flatkeys[5].strip()

    if flatkeys[6].count('SKY'): 
       skystr='Sky'
    else:
       skystr=''

    flatname='%s%s%sFlat%s%s%s%s%s.fits' % (instr, obsdate, skystr, mdstr, binstr, gnstr, rostr, fltstr)
    return flatname



