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

# Author                 Version      Date         Comment
# -----------------------------------------------------------------------
# Martin Still (SAAO)    0.2          10 Jan 2007
# S M Crawford (SAAO)    0.3          21 Oct 2007  Removed c-code

# saltbin2fit converts binary files from SALT slot mode to standard
# FITS format.

from pyraf import iraf
import os, time, glob, string
import vid2fits

import saltprint, saltio
from saltsafelog import logging
from salterror import SaltError, SaltIOError

debug=True

# -----------------------------------------------------------
# core routine

def saltbin2fit(inpath,outpath,cleanup,fitsconfig,logfile,verbose,status=0):

    status = 0
    output = {}
    headlist18 = []
    headlist19 = []

# test the logfile

    logfile = saltio.logname(logfile)

# log the call

    saltprint.line(logfile,verbose)
    message = 'SALTBIN2FIT -- '
    message += 'inpath='+inpath+' '
    message += 'outpath='+outpath+' '
    yn = 'n'
    if (cleanup): yn = 'y'
    message += 'cleanup='+yn+' '
    message += 'fitsconfig='+fitsconfig+' '
    message += 'logfile='+logfile+' '
    yn = 'n'
    if (verbose): yn = 'y'
    message += 'verbose='+yn+'\n'
    saltprint.log(logfile,message,verbose)

# start time

    saltprint.time('SALTBIN2FIT started at  ',logfile,verbose)

# check directory inpath exists

    if (status == 0): inpath, status = saltio.pathexists(inpath,logfile)

# check directory outpath exists or create it

    outpath = outpath.strip()
    if (outpath[-1] != '/'): outpath += '/'
    if (status == 0 and not os.path.exists(outpath)):
        status = saltio.createdir(outpath,'no',logfile)

# are there binary files in directory inpath?

    if (status == 0):
        binlist = glob.glob(inpath+'*.bin')
        if (len(binlist) == 0):
            message = 'SALTBIN2FIT: ERROR -- no binary files (*.bin) found in '+inpath
            status = saltprint.err(logfile,message)

# determine date of the observations

    if (status == 0):
        filemax = 4
        binpath = binlist[0].split('/')
        binfile = binpath[len(binpath)-1]
        if (len(binfile) < 19):
            date = binfile[-16:-8]
            instrument = binfile[-17]
        else:
            date = binfile[-17:-9]
            instrument = binfile[-18]
            filemax = 5

# check all files are consistent with one date and have understandable names

    if (status == 0):
        for file in binlist:
            binpath = file.split('/')
            file = binpath[len(binpath)-1]
            if (len(file) == 17 and file[-16:-8] != date):
                status = 10
            elif (len(file) == 18 and file[-17:-9] != date):
                status = 10
            if (status == 10):
                message = 'ERROR: SALTBIN2FIT -- Either binary files from multiple dates exist, '
                message += 'or binary files exist with non-standard names.'
                status = saltprint.err(logfile,message)
            if (file[0:1] != instrument):
                message = 'ERROR: SALTBIN2FIT -- there may be binary files for more than one instrument '
                message += 'because file names do not all start with the same character - '+file
                status = saltprint.err(logfile,message)

# create list of header definition files

    if (status == 0):
        headlist = glob.glob(inpath+'*.head')
        headlist.sort()
        if (len(headlist) == 0):
            message = 'SALTBIN2FIT: ERROR -- no header definition files (*.head) found in '+inpath
            status = saltprint.err(logfile,message)

# create list of bin files

    if (status == 0):
        binlist = glob.glob(inpath+'*.bin')
        binlist.sort()
        #set the maximum head value that can be used
        maxnexthead=findimagenumber(binlist[-1])+10
        if (len(binlist) == 0):
            message = 'SALTBIN2FIT: ERROR -- no bin files (*.bin) found in '+inpath
            status = saltprint.err(logfile,message)

# run vid2fits to convert the data


    print maxnexthead
    if (status == 0):

        #set yo tge counting for the image headers
        i=0
        inhead=headlist[i]
        i+=1
        #if only one image header exists
        nexthead=maxnexthead
        if i<len(headlist):
        #if more than one header list
            nexthead=findimagenumber(headlist[i])

        #loop through the binned images to convert each one
        for binimg in binlist:
            #name the output file
            fitsimg=string.replace(binimg,'.bin','.fits')

            #find the right inhead for each frame
            if findimagenumber(binimg) >= nexthead:
                inhead=headlist[i]
                i += 1
                if i < len(headlist):
                    nexthead=findimagenumber(headlist[i])
                else:
                    nexthead=maxnexthead

            #convert the images
            vid2fits.vid2fits(inhead,binimg,fitsimg,fitsconfig)
            try:
                 message = 'SALTBIN2FIT: Created '+fitsimg+' from '+binimg
                 message += ' using header file  '+inhead
                 saltprint.log(logfile,message,verbose)
            except:
                message = 'SALTBIN2FIT ERROR: Unable to create '+fitsimg+' from '+binimg
                message += ' using header file  '+inhead
                saltprint.log(logfile,message,verbose)



# end time

    if (status == 0):
        saltprint.log(logfile,' ',verbose)
        saltprint.time('SALTBIN2FIT completed at',logfile,verbose)
    else:
        saltprint.time('SALTBIN2FIT aborted at  ',logfile,verbose)

def findimagenumber (filename):
    """find the number for each image file"""
    #split the file so that name is a string equal to OBSDATE+number
    name=filename.split('/')[-1].split('.')[0]
    return int(name[9:])


# -----------------------------------------------------------
# main code

if not iraf.deftask('saltbin2fit'):
  parfile = iraf.osfn("pipetools$saltbin2fit.par")
  t = iraf.IrafTaskFactory(taskname="saltbin2fit",value=parfile,function=saltbin2fit, pkgname='pipetools')
