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

"""
 saltedtky reads pairs of data from a formatted ascii file. The first
 item of the pair is an old or new keyword name. The second item is
 the desired value for the keyword. Each pair is coupled to a file
 number. Only the specified file, or range of files, will be edited.
 This tool is intended for use in the automated salt pipeline
 using a keyword list prepared by a salt astronomer, but other users
 will probably find employment for it.

Author                 Version      Date
-----------------------------------------------
Martin Still (SAAO)    0.2          20 May 2007
S M Crawford (SAAO)    0.3          03 Mar 2008
S M Crawford (SAAO)    0.4          16 Sep 2011

 Updates:
 20090716 SMC  Changed it so it would insert a keyword value with the proper format
               and also fixed an error in how it reads in the fixes
 20110916 SMC  Rename to salteditkey

"""

from __future__ import with_statement

import os, re, glob, time, shutil, pyfits, numpy

from pyraf import iraf
from pyraf.iraf import pysalt
import os, string, sys, glob, pyfits, time

import saltsafestring as saltstring
import saltsafekey as saltkey
import saltsafeio as saltio
from saltsafelog import logging, history

from salterror import SaltError    

class EditKeyError(SaltError):
    """Errors involving IO should cause this exception to be raised."""
    pass


debug=True



# -----------------------------------------------------------
# core routine

def salteditkey(images,outimages,outpref, keyfile, recfile=None,clobber=False,logfile='salt.log',verbose=True):


   with logging(logfile,debug) as log:

       # Check the input images 
       infiles = saltio.argunpack ('Input',images)

       # create list of output files 
       outfiles=saltio.listparse('Outfile', outimages, outpref,infiles,'')

       #verify that the input and output lists are the same length
       saltio.comparelists(infiles,outfiles,'Input','output')


       #is key file defined
       saltio.argdefined('keyfile',keyfile)
       keyfile = keyfile.strip()
       saltio.fileexists(keyfile)

       # if the data are the same, set up to use update instead of write
       openmode='copyonwrite'
       if (infiles!=outfiles): openmode='copyonwrite'

       # determine the date of the observations
       obsdate=saltstring.makeobsdatestr(infiles, 1,9)
       if len(obsdate)!=8:
           message = 'Either FITS files from multiple dates exist, '
           message += 'or raw FITS files exist with non-standard names.'
           log.warning(message)

       # FITS file columns to record keyword changes
       fitcol = []
       keycol = []
       oldcol = []
       newcol = []

       # Set up the rules to change the files
       keyedits=readkeyfile(keyfile, log=log, verbose=verbose)

       #now step through the images
       for img, oimg in zip(infiles, outfiles):

           #determine the appropriate keyword edits for the image
           klist=[]
           for frange in keyedits:
               if checkfitsfile(img, frange, keyedits[frange]):
                   klist.append(keyedits[frange][3])

           if klist:

               #open up the new files
               struct = saltio.openfits(img,mode=openmode)

               for kdict in klist:
                   for keyword in kdict:
                       #record the changes
                       value=kdict[keyword]
                       fitcol.append(img)
                       keycol.append(keyword)
                       newcol.append(value)
                       try:
                           oldcol.append(struct[0].header[keyword].lstrip())
                       except:
                           oldcol.append('None')
                       #update the keyword
                       if saltkey.found(keyword, struct[0]):
                           try:
                               saltkey.put(keyword,value,struct[0])
                               message='\tUpdating %s in %s to %s' % (keyword, os.path.basename(img), value)
                               log.message(message, with_header=False, with_stdout=verbose)
                           except Exception, e:
                               message = 'Could not update %s in %s because %s' % (keyword, img, str(e))
                               raise SaltError(message)
                       else:
                           try:
                               saltkey.new(keyword.strip(),value,'Added Comment',struct[0])
                               message='\tAdding %s in %s to %s' % (keyword, os.path.basename(img), value)
                               log.message(message, with_header=False, with_stdout=verbose)
                           except Exception,e :
                               message = 'Could not update %s in %s because %s' % (keyword, img, str(e))
                               raise SaltError(message)

               #updat the history keywords
               #fname, hist=history(level=1, wrap=False, exclude=['images', 'outimages', 'outpref'])
               #saltkey.housekeeping(struct[0],'SAL-EDT', 'Keywords updated by SALTEDITKEY', hist)

               #write the file out
               if openmode=='update':
                   saltio.updatefits(struct)
                   message = 'Updated file ' + os.path.basename(oimg)
               else:
                   saltio.writefits(struct, oimg, clobber)
                   message = 'Created file ' + os.path.basename(oimg)
               log.message(message, with_header=False, with_stdout=True)

               struct.close()

 
       #write out the upate of the file
       if recfile:
           createrecord(recfile, fitcol, keycol, oldcol, newcol, clobber)
                 

def createrecord(recfile, fitcol, keycol, oldcol, newcol, clobber):
   """Create the fits table record of all of the changes that were made"""

   col1 = pyfits.Column(name='FILE',format='32A',array=fitcol)
   col2 = pyfits.Column(name='KEYWD',format='8A',array=keycol)
   col3 = pyfits.Column(name='OLD_VAL',format='24A',array=oldcol)
   col4 = pyfits.Column(name='NEW_VAL',format='24A',array=newcol)
   cols = pyfits.ColDefs([col1,col2,col3,col4])
   try:
       rectable = pyfits.new_table(cols)
   except:
       message='Cannot open FITS structure ' + recfile
       raise SaltError

   #add some additional keywords
   saltkey.new('EXTNAME','NEWKEYS','Name of this binary table extension', rectable)
   saltkey.new('OBSERVAT','SALT','South African Large Telescope', rectable)
   saltkey.new('SAL-TLM',time.asctime(time.localtime()), 'File last updated by PySALT tools',rectable)
   saltkey.new('SAL-EDT',time.asctime(time.localtime()), 'Keywords updated by SALTEDTKY',rectable)

   #write it out
   saltio.writefits(rectable, recfile, clobber)

  


def readkeyfile(keyfile, log=None, verbose=True):
   """Read in the keyword file"""
 
   #open the key word file
   kfile = saltio.openascii(keyfile,'r')

   #extract keyword edits
   keyedits={}
   for line in kfile:
       if len(line.strip()) > 0 and not line.startswith('#'):
           try:
               frange, finstr, fstar, fend=getfrange(line)
               #print frange, finstr, fstar, fend
               keyedits[frange]=[finstr, fstar, fend, getitems(line, frange)]
               #print frange, keyedits[frange]
               #print getitems(line, frange)
           except EditKeyError,e :
               message="Could not extract keyword edits for %s because %s" % (line, e)
               log.error(message)
          

   return keyedits


# -----------------------------------------------------------
# Get the numbers of the fits file that it will be over

def getfrange(line):
    "Get the file names of the input files.  Check to see that they are correct"

    #the names should be the first entry
    try:
        line.strip()
        frange=line.split()[0]
        instr, start, stop=checkkeyrange(frange)
    except Exception, e:
        raise EditKeyError(e)


    return frange, instr, start, stop


# -----------------------------------------------------------
# check to see if the format of the range is correct

def checkkeyrange(frange):
    """check to see if the format of the range is correct"""

    try:
        instr=frange[0]
        start=0
        stop=0
    except Exception, e:
        raise EditKeyError(e)

    if instr not in ['S', 'P', 'H', 'R']:
       raise EditKeyError('%s: Not a support instrument' % instr)
 
    frange=frange[1:].split('-')
    try:
        start=int(frange[0])
        stop=start
    except Exception, e:
        raise EditKeyError(e)

    if len(frange)==2:
        try:
            stop=int(frange[1])
        except Exception, e:
            raise EditKeyError(e)
     
    return instr, start, stop

# -----------------------------------------------------------
# extract the items from the line
def getitems(line, frange):
    """Extract the items from the information such that for each
    element in the list there is one keyword=value pair
    """
    #set up the variables
    items={}

    #remove anything from the line that isn't going into
    #items
    line=line.split(frange)[-1].strip()
    #print line

    #Now we have to break up line into each of the individual parts
    #items=line.split()[1:]
    space=True
    start_in=0
    end_in=0
    keyword=''
    value=''
    schar=''
    outside=True
    i=0
    for i in range(len(line)):
        #at the end of the value
        if line[i]==schar and keyword and not outside:
           try: 
               #add things which should stay as strings
               if keyword.strip() in ['FI-STA','PCS-VER','SM-STA','BVISITID', 'BLOCKID'] : raise Exception('fail')
               value = float(value.replace(schar, ''))
           except:
               value=value.strip()
           items[keyword.strip()]=value
           outside=True
           value=''
           keyword=''

        #start of value
        if (line[i]=='\"' or line[i]=="\'") and outside and keyword: 
           schar=line[i]
           outside=False

        if outside and line[i]!='=' and line[i]!=schar:
           keyword+=line[i]

        if not outside and line[i]!=schar:
           value+=line[i]

    return items

# -----------------------------------------------------------
# check to see if a given fits file is one to be edited

def checkfitsfile(infile, frange, klist):

    #make sure the file exists
    if not os.path.isfile(infile): return False
 
    #expeand klist
    finstr, fstart, fstop, keyitems=klist

    #check that the file is of the right instrument and date
    if not infile.count(finstr): return False

    #check that the files are in the appropriate range
    try:
       nid=saltstring.filenumber(infile, -9, -5)
    except Exception, e:
       return False
    if fstart <= nid <= fstop: return True
    return False



# -----------------------------------------------------------
# main code
if not iraf.deftask('salteditkey'):
  parfile = iraf.osfn("pipetools$salteditkey.par")
  t = iraf.IrafTaskFactory(taskname="salteditkey",value=parfile,function=salteditkey, pkgname='pipetools')
