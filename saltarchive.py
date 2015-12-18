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
# S. M. Crawford (SAAO)    1.0        19 Oct 2007

# saltarchive moves the temporary reduction directory to the ctfileserver
# for archiving.

#Updates
#6 Apr 2011   Updated to use the new io tools

# Ensure python 2.5 compatibility
from __future__ import with_statement


from pyraf import iraf
import saltprint, salttime, saltstring
import tarfile, glob, os, ftplib

import saltsafeio as saltio

from saltsafelog import logging
from salterror import SaltError


debug=True


# -----------------------------------------------------------
# core routine

def saltarchive(obsdate,archpath, clobber,logfile,verbose):
   """Archive the SALT data.  Move the data to /salt/data after it has been run by the pipeline"""

   with logging(logfile,debug) as log:
       #check the entries
       saltio.argdefined('obsdate',obsdate)
       saltio.argdefined('archpath',archpath)

       # create the directory if it does not already exist
       if (not os.path.exists(archpath+obsdate[:4])):
           saltio.createdir(archpath+obsdate[:4])

       #cleen up the directory if it already exists
       archdir=archpath+obsdate[:4]+'/'+obsdate[4:]
       if (os.path.exists(archdir) and clobber):
           for root, dirs, files in os.walk(archpath+obsdate[:4]+'/'+obsdate[4:],topdown=False):
               for file in files:
                   os.remove(os.path.join(root,file))
               for dir in dirs:
                   os.rmdir(os.path.join(root,dir))
           os.rmdir(archpath+obsdate[:4]+'/'+obsdate[4:])
       elif (os.path.exists(archdir) and not clobber):
           message  = 'Cannot overwrite '+archpath+obsdate[:4]+'/'+obsdate[4:]
           raise SaltError(message)
       elif (not os.path.exists(archdir)):
            pass

       saltio.move(obsdate,archdir)

# -----------------------------------------------------------
# main code

if not iraf.deftask('saltarchive'):
   parfile = iraf.osfn("pipetools$saltarchive.par")
   t = iraf.IrafTaskFactory(taskname="saltarchive",value=parfile,function=saltarchive, pkgname='pipetools')
