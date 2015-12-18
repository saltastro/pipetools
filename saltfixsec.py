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

# Author                  Version        Date
# --------------------------------------------------
# Martin Still (SAAO)     0.2            11 Jul 2006
# S M Crawford (SAA0)     0.3            22 Feb 2008

# saltfixsec is a temporary task, written to perfrom a quick fix to
# SALT keywords headers TRIMSEC, BIASSEC and DATASEC which are
# written at the telescope when the row and column CCD binning is
# unequal. The correction will only be successful for data that has
# not been windowed. windowed data will require manual editing
# using e.g. HEASARC's fv tool.
#
# 22-Feb-2008:  Updated to remove access to iraf.tables
# 08-Sep-2009:  Removed call for keyword EXTVER, which isn't in all data
#               Overall made a bit more robust against failure
import glob, pyfits
from pyraf import iraf
from math import fmod

# core routine

def updatekeyword(hdr, keyword,value,comment):
    """Update or add the keyword"""
    try:
        hdr[keyword]=value
    except KeyError:
        hdu.header.update('DATASEC',datastring, '')
    except Exception, e:
        print "ERROR: "+e

    return hdr

def saltfixsec(infiles):

# Find input files

    DataFiles = glob.glob(infiles)

# loop through input files

    for file in DataFiles:

        #open up the file
        try:
            struct = pyfits.open(file, mode='update')
        except:
            message='Failed to open '+file
            print message
            return


        # loop through each of the headers
        next=0
        for hdu in struct:
            if hdu.header['NAXIS']==2:
                status=0
                next += 1
                # read binning keyword CCDSUM
                try:
                    binning=hdu.header['CCDSUM']
                    binx = int(binning[:1])
                    biny = int(binning[1:])
                except:
                    binx=0
                    biny=binx
                    message='No binning information in '+file
                    print message

                # if binned pixels are not square, correct keywords
                if (binx != biny):

                    try:
                        val = hdu.header['DETSEC']
                        xstr = val.split('[')[-1].split(',')[0]
                    except:
                        message='ERROR:  DETSEC not defined in '+file
                        print message
                        status=1


                    if status == 0:
                        # DATA section keyword
                        datastring='['+str(int(50)/binx+1)+':'+str(int(1074/binx))+',2:'+str(int(4102)/biny-1)+']'
                        hdu.header=updatekeyword(hdu.header, 'DATASEC', datastring, '')

                        # TRIM section keyword
                        trimstring='['+str(int(50)/binx+2)+':'+str(int(1074)/binx-1)+',3:'+str(int(4102)/biny-2)+']'
                        hdu.header=updatekeyword(hdu.header, 'TRIMSEC', trimstring, '')

                        # BIAS section keyword
                        if fmod(next,2):
                            biasstring='[2:'+str(int(50)/binx-1)+',2:'+str(int(4102)/biny-1)+']'
                        else:
                            biasstring='['+ str(int(1075)/binx+1)+':'+str(int(1124)/binx-1)+',2:'+str(int(4102)/biny-1)+']'
                        hdu.header=updatekeyword(hdu.header,'BIASSEC',biasstring,'')

# Write out the file

        try:
            struct.flush()
            struct.close()
        except:
            message='Unable to upate '+file

# main code

parfile = iraf.osfn("pipetools$saltfixsec.par")
t = iraf.IrafTaskFactory(taskname="saltfixsec",value=parfile, function=saltfixsec, pkgname='pipetools')
