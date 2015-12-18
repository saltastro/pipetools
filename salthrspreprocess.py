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
# S M Crawford (SAAO)    0.3          11 Oct 2013  

# salthrspreprocess converts .fit files to fits and places HRS data
# into the standard SALT fits format


import os, glob
import saltsafeio as saltio

debug=True

# -----------------------------------------------------------
# core routine

def salthrspreprocess(inpath,outpath,clobber=True, log=None,verbose=True):
    """Convert .fit files to .fits files and place HRS data into 
       standard SALT FITS format
    """
    #first get a list of images in the directory
    infiles=glob.glob(inpath+'*.fit')
    if log is not None:
       log.message('Processing HRS data in %s' % inpath)

    #open the file and write out as a fits files in the output directory
    for img in infiles:
        oimg=outpath+os.path.basename(img)+'s'
        hdu=saltio.openfits(img)
        hdu=hrsprepare(hdu)
        if log is not None:
           log.message('Writing %s to %s' % (img, oimg), with_header=False)
        saltio.writefits(hdu, oimg, clobber=clobber)
    
 
    return 

def hrsprepare(hdu):
    """Prepare HRS data to be similar to other SALT file formats
       This includes splitting each amplifier into multi-extension
       formats 
    """
    return hdu
