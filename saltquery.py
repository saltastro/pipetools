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
SALTQUERY queries the SALT Science Database for FITS data in the FileData tables. It provides a pyraf interface for querying the tables.

The user inputs the selection they want to use. They can select for any of the columns in FileData or for any of the keywords in the FITS header. The list can either be provide as a comma separated list or in a file with '@' proceeding the file name. 

The user than supplies the task with a bit of logic to select the data. This should be supplied as a string and following mysql syntax. The user also supplies it with start and end dates which are automatically applied to the where statement. 

Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1          5  July 2011


Updates  
-----------------------------------------------------------------------

20120607- Updated to query RSS information and then SALTICAM information

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

def saltquery(selection, logic, startdate, enddate, outfile=None, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', clobber=False, logfile='saltlog.log', verbose=True):
    """Query the salt database for FITS files

    """

    with logging(logfile,debug) as log:

       #check the outfiles
       if not saltio.checkfornone(outfile):
          outfile=None
          
       #check that the output file can be deleted
       if outfile:
           saltio.overwrite(outfile, clobber)

       #open outfile
       if outfile:
          fout=saltio.openascii(outfile, 'w')


       #connect to the database
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser,password)

       #Create a list of the selection and then unpack it
       selection_list = saltio.argunpack('Selection', selection)
       selection=','.join(selection_list)

       #write out the header for the outfile
       outstr='#'+' '.join(['%s' % x for x in selection_list]) 
       if outfile:
          fout.write(outstr+'\n')
       else:
          print outstr


       #set up the table
       rsstable=''' FileData 
  left join FitsHeaderImage using (FileData_Id) 
  inner join FitsHeaderRss using (FileData_Id) 
'''
       #set up the table
       scamtable=''' FileData 
  left join FitsHeaderImage using (FileData_Id) 
  inner join FitsHeaderSalticam using (FileData_Id) 
'''

       #set up the logic
       logic=makelogic(logic, startdate, enddate)

       for tab in [rsstable, scamtable]:

          msg='''
Mysql querying data is:

  SELECT %s
  FROM   %s
  WHERE  %s
'''  % (selection, tab, logic)
          log.message(msg, with_stdout=verbose)


          record=saltmysql.select(sdb, selection, tab, logic)

          print record
  
          for r in record:
             outstr=' '.join(['%s' % x for x in r]) 
             if outfile:
                 fout.write(outstr+'\n')
             else:
                 print outstr

       #close outfile
       if outfile: fout.close()


def makelogic(logic, startdate, enddate):

   #set up the logic for different dates
   date_logic=''
   if startdate: 
      d,m,y=salttime.breakdate(startdate)
      y=str(y)
      m=string.zfill(m, 2)
      d=string.zfill(d, 2)
      date_logic += "UTStart > '%s-%s-%s 12:00:01'" % (y,m,d)
   if startdate and enddate:
      date_logic += " and "
   if enddate:
      edate=salttime.getnextdate(enddate)
      d,m,y=salttime.breakdate(str(edate))
      y=str(y)
      m=string.zfill(m, 2)
      d=string.zfill(d, 2)
      date_logic += "UTStart < '%s-%s-%s 11:59:59'"  % (y,m,d)

   if logic and date_logic:
      logic = '('+logic+')' + ' and ' + date_logic
   else:
      logic
   return logic
       
# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltquery.par")
t = iraf.IrafTaskFactory(taskname="saltquery",value=parfile,function=saltquery, pkgname='pipetools')
