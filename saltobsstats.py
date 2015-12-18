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
SALTOBSSTATS measures various statistics about a night and updates the 
database as needed.   These statistics include:
-stats per night
--night length
--science time
--open shutter time
--time lost to weather
--time lost to engineer
--time on alignment

-Block stats
--time from pointing start to track start
--time to guiding
--time to focus
--time to first exposure
--shutter open time
--total time of block

-Program Stats
--total charged time for the night
--total open time for the night

Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1          5  July 2011

"""
# Ensure python 2.5 compatibility
from __future__ import with_statement


import os, time, glob, string
from pyraf import iraf
import salttime
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging
from salterror import SaltError


debug=True

# -----------------------------------------------------------
# core routine

def saltobsstats(obsdate, statfile, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', clobber=False, logfile='salt.log', verbose=True):
    """Measure statistics about SALT observations

       obsdate: Night for measuring statistics
       statfile: Output file for statistics

    """

    with logging(logfile,debug) as log:
 
       #connect to the sdb
       sdb=saltmysql.connect(sdbhost, sdbname, sdbuser, password)

       
# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltquery.par")
t = iraf.IrafTaskFactory(taskname="saltquery",value=parfile,function=saltquery, pkgname='pipetools')
