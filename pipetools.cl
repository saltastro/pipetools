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

# Package script for the saltred package

# Load necessary packages - only those that are used by all packages
pysalt

#add python pass
pyexecute("pipetools$addpath.py",verbose=no)

#define the PyRAF tasks automatically
pyexecute("pipetools$salteditkey.py",verbose=no)
pyexecute("pipetools$saltbin2fit.py",verbose=no)
pyexecute("pipetools$saltemail.py",verbose=no)
pyexecute("pipetools$saltfixsec.py",verbose=no)
pyexecute("pipetools$saltfast.py",verbose=no)
pyexecute("pipetools$saltftp.py",verbose=no)
pyexecute("pipetools$salthtml.py",verbose=no)
pyexecute("pipetools$saltobsid.py",verbose=no)
pyexecute("pipetools$saltpipe.py",verbose=no)
pyexecute("pipetools$saltsdbloadfits.py",verbose=no)
pyexecute("pipetools$saltquery.py",verbose=no)
pyexecute("pipetools$saltarchive.py",verbose=no)
pyexecute("pipetools$saltcalibrations.py",verbose=no)

package pipetools

;
clbye()
