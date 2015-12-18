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
# S M Crawford (SAAO)    0.3          21 Jul 2006

"""SALTFAST data response.  SALTFAST does the
 following steps:
 1. It copies the nightlog into that directory
      as a txt file and the list of observed images
      as a txt file.
 2. It then copies that directory to the SAAO webserver.
      to a constant name directory.
 3. It then sends email notification to the user
"""

from __future__ import with_statement


from pyraf import iraf
import tarfile, glob, os, ftplib
import numpy
from saltemail import saltemail

import saltsafeio as saltio
import saltsafemysql as saltmysql
import salttime, saltstring

from saltsafelog import logging
from salterror import SaltError

debug=True



# -----------------------------------------------------------
# core routine

def saltfast(obsdate, readme, emailserver, username,password, bcc, sdbhost,
             sdbname,sdbuser, clobber,logfile,verbose):

   # set up
   nightlog = ''

 
   with logging(logfile,debug) as log:
       # determine current directory
       workdir = os.getcwd()
       logfile = workdir + '/' + os.path.basename(logfile)


       #log into the database
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser,password)

       #query the data base for all of the data taken last night and the proposal codes
       select='FileName, Target_Name, Proposal_Code'
       table='FileData join ProposalCode using (ProposalCode_Id)'
       logic="FileName like '%"+obsdate+"%'"
       records=saltmysql.select(sdb, select, table, logic)

       if len(records)==0:
          message='No data taken on %s\n' % obsdate
          log.message(message)
          return 

       #determine the list of files, targets, and propcodes
       file_list=[]
       target_list=[]
       propcode_list=[]
       propcode_dict={}
       for name,targ,pid in records: 
           file_list.append(name)
           target_list.append(targ)
           propcode_list.append(pid)
           try:
               propcode_dict[pid].append(targ)
           except KeyError:
               propcode_dict[pid]=[targ]


       # check to see if any of the PI directories requires fast response
       for pid in propcode_dict.keys():
            target=set(propcode_dict[pid])
            if checkforfast(pid, target, sdb):
                #log the move
                message='Copying  data from %s to the ftp site' % pid
                log.message(message, with_stdout=verbose)

                #get the username
                piptuser=saltmysql.getpiptusername(sdb, pid) 
                #create the fast directory
                fastdir='/salt/ftparea/%s/FAST/' % piptuser

                if os.path.isdir(fastdir):   saltio.deletedir(fastdir)
                os.mkdir(fastdir)
 
                for i in range(len(file_list)):
                    if propcode_list[i]==pid:
                       #create the data filename
                       if file_list[i].startswith('S'):
                          instr='scam'
                       elif file_list[i].startswith('P'):
                          instr='rss'
                       filepath='/salt/%s/data/%s/%s/raw/%s' % (instr, obsdate[0:4], obsdate[4:8], file_list[i])
                       saltio.copy(filepath, fastdir)
                           
                #make the temporary readme
                mailmessage = maketempreadmefast(pid, sdb, readme)

                #send the email
                subject='SALT Raw Data available for %s' % pid
                sender='sa@salt.ac.za'
                recipient=saltmysql.getpiptemail(sdb, piptuser)
                bcc='crawford@saao.ac.za'#sa_internal@saao.ac.za'
                #saltio.email(emailserver, username, password, sender,recipient,bcc, subject, mailmessage)



def maketempreadmefast(pid, sdb, readme):
   """Creates the readme file to send to the PI.  It adds on the observing log for those
      observations
   """
   #read in the readme
   f=saltio.openascii(readme, 'r')
   rstring=f.read()
   saltio.closeascii(f)

   #replace propcode with the actual propcode
   if pid: rstring=rstring.replace('PROPCODE', pid)

   #add in the observing log

   return rstring 


def checkforfast(pid, target, db):
   """Check the database to see if fast response is needed

        returns status"""

   record=''
   #log into database and determine response type of proposal
   #If the object is not in the proposal, and there is a request for a fast response
   #for other objects in the proposal, then it should responsd with a fast response.

   state_select='T.Target_name, pdam.DataAccessMethod'
   state_from="""
Proposal 
  join ProposalCode using (ProposalCode_Id) 
  join Block using (Proposal_Id) 
  join Pointing using (Block_Id) 
  join Observation using (Pointing_Id) 
  join Target as T using (Target_Id) 
  join PipelineConfig using (Pointing_Id) 
  join  PipelineDataAccessMethod as pdam using (PipelineDataAccessMethod_Id)
"""
   state_logic="Current=1"
   state_logic +=" and Proposal_Code='%s'" % pid
   #state_logic +=" and pr.Proposal_Code='%s' and T.Target_Name like '%s' " % (pid, target)
   #print "select %s from %s whre %s" % (state_select,state_from,state_logic)
   record=saltmysql.select(db,state_select,state_from,state_logic)
   record=set(record)

   #if there are no records in the set
   if len(record)==0: return False

   #TODO:  Add a check to make sure that
   for t,m in record:
       if m.upper()=='FAST' and t in target: return True 

   return False
# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltfast.par")
t = iraf.IrafTaskFactory(taskname="saltfast",value=parfile,function=saltfast, pkgname='pipetools')
