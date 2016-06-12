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

import os
import sys
import string
import time
import struct
import numpy
from astropy.io import fits

import saltsafeio as saltio
import saltsafekey as saltkey

from salterror import SaltError, SaltIOError


def ascii_time(time_1970):
   """Reads a string containing the number of seconds since 1970
       and returns the date and time of the observations

        return date and ut
   """
   try:
        milliseconds = 1000.0*(time_1970-int(time_1970))+0.5;
        obs_time=time.gmtime(time_1970)
   except:
        message = 'ERROR: VID2BIN.ascii_time -- Cannot convert time %11.5f ' % time_1970
        raise SaltError(message)

   date="%.4d-%.2d-%.2d" % (obs_time.tm_year,obs_time.tm_mon,obs_time.tm_mday)
   ut="%.2d:%.2d:%.2d.%.3d" % (obs_time.tm_hour,obs_time.tm_min,obs_time.tm_sec,milliseconds)

   return date,ut

def write_ext_header (hdu, file, bhdu, ut,date,bscale,bzero,exptime,gain,rdnoise,
        datasec,detsec,ccdsec,ampsec,biassec, deadtime=None, framecnt=None):
   """Update the header values for the extension

       returns 
   """
   #if (status==0): status = saltkey.new("BSCALE",bscale,"Val=BSCALE*pix+BZERO",hdu,file,logfile)
   #if (status==0): status = saltkey.new("BZERO",bzero,"Val=BSCALE*pix+BZERO",hdu,file,logfile)
   saltkey.new("UTC-OBS",ut,"UTC start of observation",hdu,file)
   saltkey.new("TIME-OBS",ut,"UTC start of observation",hdu,file)
   saltkey.new("DATE-OBS",date,"Date of the observation",hdu,file)
   saltkey.new("EXPTIME",exptime,"Exposure time",hdu,file)
   saltkey.new("GAIN",gain,"Nominal CCD gain (e/ADU)",hdu,file)
   saltkey.new("RDNOISE",rdnoise,"Nominal readout noise in e",hdu,file)
   saltkey.new("CCDSUM", bhdu.header['CCDSUM'], 'On chip summation', hdu, file)
   saltkey.copy(hdu,bhdu,"CCDSUM")
   saltkey.copy(hdu,bhdu,"DETSIZE")
   saltkey.new("DATASEC",datasec,"Data Section",hdu,file)
   saltkey.new("DETSEC",detsec,"Detector Section",hdu,file)
   saltkey.new("CCDSEC ",ccdsec,"CCD Section",hdu,file)
   saltkey.new("AMPSEC",ampsec,"Amplifier Section",hdu,file)
   saltkey.new("BIASSEC",biassec,"Bias Section",hdu,file)
   if deadtime:
       saltkey.new("DEADTIME",deadtime,"Milliseconds waiting for readout", hdu, file)
   if framecnt:
       saltkey.new("FRAMECNT",framecnt,"Frame counter", hdu, file)
 
   return hdu

def processline(line):
    """process the line, remove comments, and return a key and value
       based on the syntax of the line
    """

    #first thing to do is to get rid of the comments
    try:
        i=line.index('#')
        line = line[:i]
    except:
        pass
    #now for everything left, let's parse the line
    #so that the value in <> goes into the key and
    #any other value is placed into the value
    if len(line) > 0:
        k1=line.index('<')
        k2=line.index('>')
        key=line[k1+1:k2]
        value=line[:k1-1].split()
        if key and value: return key, value

    #if key and value are given real values, then set status to zero

    return '',''

def fitsconfig(config):
   """reads in the configuration file and calculates the appropriate parameters
       for the sections

       returns dictiionary
   """

   condict={}
   #open and read the config file
   construct=saltio.openascii(config,'r')
   try:
       condata = construct.read().split('\n')
   except:
       message = 'ERROR: VID2BIN.FITSCONFIG -- cannot read in ' + file
       raise SaltError(message)


   for conline in condata:
       ckey,cval=processline(conline)
       if ckey and cval: condict[ckey]=cval

   return condict

def create_header_values(condict, bhdu, amp, row):
   """Calculate the sections for the header file
   """
   #calcuate the binning for the data
   binx = int(bhdu.header['CCDSUM'].split()[0])
   biny = int(bhdu.header['CCDSUM'].split()[1])

   #set up the keys
   dimkey='Dim_1'
   datakey='DataSec_%i' % binx
   overscanapply='OverScan_Apply'
   prescanapply='PreScan_Apply'
   biaskey = 'OverScan_%i' % binx
   if int(condict[prescanapply][0])==1:
       biaskey ='PreScan_%i' % binx

   #set up the amplifier, minimum dimensions, and gap size
   slotrow=2053
   na = amp % 2
   min_dim = int(condict[dimkey][0])-int(condict[dimkey][2])-int(condict[dimkey][3])
   gap=0
   if amp > 1: gap=109

   #fill in the detsec keyword
   x1=amp*min_dim + 1 + gap;
   x2=x1+min_dim
   y1=slotrow
   y2=y1+biny*(row)
   detsec='[%i:%i,%i:%i]' % (x1,x2,y1,y2)
   #fill in the ccdsec keyword
   x1=int(condict[dimkey][2+na])+1+na*min_dim
   x2=x1+min_dim
   ccdsec='[%i:%i,%i:%i]' % (x1,x2,y1,y2)

   #fill in the ampsec keyword
   if (na==0):
        x1=int(condict[dimkey][2])+1
        x2=x1+min_dim
   else:
        x2=int(condict[dimkey][2])+1
        x1=x2+min_dim

   ampsec='[%i:%i,%i:%i]' % (x1,x2,y1,y2)

   #fill in the datasec keyword
   x1=int(condict[datakey][2*na])
   x2=int(condict[datakey][2*na+1])
   y1=1
   y2=row
   datasec='[%i:%i,%i:%i]' % (x1,x2,y1,y2)
   #fill in the biassec keyword
   x1=int(condict[biaskey][2*na])
   x2=int(condict[biaskey][2*na+1])
   biassec='[%i:%i,%i:%i]' % (x1,x2,y1,y2)

   return datasec,detsec,ccdsec,ampsec,biassec

def softwareversion(detsvw):
    try:
      detsvw=detsvw.split('-')[-1]
      detsvw=detsvw.split('.')
      detsvw='%s.%s' % (detsvw[0], detsvw[1])
      detsvw=float(detsvw)
    except Exception,e :
      message='VID2FITS--No software version determined due to %s' % e
      raise SaltError(message)
    return detsvw

def vid2fits(inhead, inbin,outfile, config):
   """
    Convert bin files made during the video process to
    regular fits files

    Format python vid2fits.py inhead inbin outfits config

    Returns
   """

   #Check that the input files exists
   saltio.fileexists(inhead)
   saltio.fileexists(inbin)
   saltio.fileexists(config)

   #if output file exists, then delete
   if os.path.isfile(outfile): saltio.delete(outfile)

   #read in and process the config file
   condict=fitsconfig(config)

   #read in the header information
   infits=saltio.openfits(inhead)
   inheader = infits['Primary'].header
   instrume=inheader['INSTRUME']
   detswv=softwareversion(inheader['DETSWV'])
   

   #create a new image and copy the header to it
   try:
       hdu = fits.PrimaryHDU()
       hdu.header=inheader
       hduList = fits.HDUList(hdu)
       #hduList.verify()
       hduList.writeto(outfile, output_verify='ignore')
   except:
       message  = 'ERROR -- VID2FIT: Could not create new fits file'
       raise SaltError(message)

   #Now open up the file that you just made and update it from here on
   hduList = fits.open(outfile, mode='update')

   #open the binary file
   bindata = saltio.openbinary(inbin,'rb')

   #read in header information from binary file

   #some constants that are needed for reading in the binary data
   sizeofinteger=struct.calcsize('i')
   sizeofunsignshort=struct.calcsize('H')
   sizeofdouble=struct.calcsize('d')
   sizeoffloat =struct.calcsize('f')
   

   #read in the number of exposures, geometry of image (width and height) and number of amps
   nframes= saltio.readbinary(bindata,sizeofinteger,"=i")
   if detswv<=4.78 and instrume=='SALTICAM':
      fwidth= saltio.readbinary(bindata,sizeofinteger,"=i")
      fheight= saltio.readbinary(bindata,sizeofinteger, "=i")
   elif (detswv>=7.01 and instrume=='SALTICAM') or (detswv>=4.37 and instrume=='RSS'):
      fwidth= saltio.readbinary(bindata,sizeofunsignshort,"=H")
      fheight= saltio.readbinary(bindata,sizeofunsignshort, "=H")
      pbcols=saltio.readbinary(bindata,sizeofunsignshort, "=H")
      pbrows=saltio.readbinary(bindata,sizeofunsignshort,"=H")
   else:
      message='VID2FITS--Detector Software version %s is not supported' % detswv
      raise SaltError(message)
   nelements=fwidth*fheight
   namps=saltio.readbinary(bindata,sizeofinteger,"=i")
   #read in the gain
   gain = numpy.zeros(namps,dtype=float)
   for i in range(namps):
        gain[i]=saltio.readbinary(bindata,sizeofdouble,"=d")

   #read in the rdnoise
   rdnoise = numpy.zeros(namps, dtype=float)
   for i in range(namps):
       rdnoise[i]=saltio.readbinary(bindata,sizeofdouble,"=d")

   #set the scale parameters
   bzero=32768
   bscale=1
   otime=0

   #start the loop to read in the data
   for i in range(nframes):
       #read in the start of the data,time
       starttime= saltio.readbinary(bindata,sizeofdouble,"=d")
       date_obs, time_obs= ascii_time(starttime)

       #read in the exposure time
       exptime= saltio.readbinary(bindata,sizeofdouble,"=d")

       #read in the dead time  in milliseconds
       if (detswv>=7.01 and instrume=='SALTICAM') or (detswv>=4.37 and instrume=='RSS'):
           deadtime= saltio.readbinary(bindata,sizeofinteger,"=i")
       else:
           deadtime=None
       #read in the dead time 
       if (detswv>=7.01 and instrume=='SALTICAM') or (detswv>=4.37 and instrume=='RSS'):
           framecnt= saltio.readbinary(bindata,sizeofinteger,"=i")
       else:
           framecnt=None
       otime=starttime

       #read in the data
       shape =  (fheight,fwidth)
       imdata = numpy.fromfile(bindata,dtype=numpy.ushort,count=nelements)
       imdata = imdata.reshape(shape)

       #for each amplifier write it to the image
       if namps > 0: awidth=fwidth/namps

       for j in range(namps):
               ###create the new extension ###

               #cut each image by the number of amplifiers
               y1=j*awidth
               y2=y1+awidth

               data = imdata[:,y1:y2].astype(numpy.ushort)
               hdue = fits.ImageHDU(data)
               hdue.scale('int16','',bzero=bzero)

               #set the header values
               datasec,detsec,ccdsec,ampsec,biassec=  \
                          create_header_values(condict,hdu,j,fheight)

               #fill in the header data
               hdue = write_ext_header(hdue,outfile,hdu,time_obs,date_obs,bscale,bzero, \
                          exptime,gain[j],rdnoise[j],datasec,detsec,ccdsec,ampsec, \
                          biassec, deadtime=deadtime, framecnt=framecnt )

               #append the extension to the image
               hduList.append(hdue)

   try:
       hduList.flush()
       hduList.close()
   except Exception, e:
       message = 'ERROR: VID2BIN -- Fail to convert %s due to %s' % (outfile, e)
       raise SaltError(message) 


   return 


if __name__ == "__main__":
    if (len(sys.argv)>1):
        myhead   = sys.argv[1]
        mybin    = sys.argv[2]
        myout    = sys.argv[3]
        myconfig = sys.argv[4]
        logfile  = sys.argv[5]
        vid2fits(myhead,mybin,myout,myconfig)
    else:
        print vid2fits.__doc__
