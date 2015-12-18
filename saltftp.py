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
SALTFTP tar gzips and transfers PI-collated data to the FTP server
for collection by the PI.  

 Author                 Version      Date
 -----------------------------------------------
 Martin Still (SAAO)    0.2          21 Jul 2006
 S M Crawford (SAAO)    0.3          21 Jul 2007


UPDATES
------------------------------------------------
21 Jul 2007 --changed from PI to proposal code
            --moves things to the beachhead facility rather than the
              ftp site

6 Apr 2011-Updated to use the new error handling.  
          -Created new method for determinging the date

15 Jun 2011-Changed the call to the database to make it more robust
"""

# Ensure python 2.5 compatibility
from __future__ import with_statement

import string

from pyraf import iraf
import tarfile, glob, os, ftplib

import saltsafeio as saltio
import saltsafemysql as saltmysql
from salterror import SaltError, SaltIOError

from saltsafelog import logging
debug=True

# -----------------------------------------------------------
# core routine

def saltftp(propcode, obsdate, datapath, beachdir, 
            sdbhost,sdbname,sdbuser, password, cleanup=True, splitfiles=False,
            clobber=True,logfile='salt.log',verbose=True):

   # set up

   proposers = []
   propids = []
   pids = []
   
   with logging(logfile,debug) as log:

       # are the arguments defined
       if propcode.strip().upper()=='ALL':
          pids=getproposalcodes(str(obsdate), sdbhost,sdbname,sdbuser, password)
       else:
          pids = saltio.argunpack('propcode',propcode)
       print pids
       
       if len(pids)==0:
           #throw a warning adn exit if not data needs to be filterd
           log.warning('No data to distribute\n', with_stdout=verbose)
           return
 
     
       #get the current working directory
       curdir=os.getcwd()
  
       #set the obsdate to a string
       obsdate=str(obsdate)
     
       # check datapath exists, ends with a "/" and convert to absolute path
       if not saltio.checkfornone(datapath):
           datapath='/salt/data/%s/%s/' % (obsdate[0:4], obsdate[4:8])
       datapath = saltio.abspath(datapath)

       # check PI directories exist
       for pid in pids:
           pidpath = datapath + pid.upper()
           
           if os.path.exists(pidpath):
               if splitfiles:
                   tarsplitfile(obsdate, pid, datapath, beachdir, sdbhost, sdbname, sdbuser,password,clobber, log, verbose)
               else:
                   tarallfiles(obsdate, pid, datapath, beachdir, sdbhost, sdbname, sdbuser,password,clobber, log, verbose)
 
           else:  
               log.warning('No directory exists for %s on %s at:\n%s' % (pid, obsdate, pidpath))

     
       saltio.changedir(curdir)

def tarsplitfile(obsdate, pid, datapath, beachdir, sdbhost, sdbname, sdbuser,password,clobber, log, verbose):
   """Split the directories so there is one tar file for each directory"""
   pidpath = datapath + pid.upper()

   for data_dir in ['product', 'raw', 'doc']:
       # overwrite tar files
       zipfile = '%s.%s_%s.tar.bz2' % (pid.upper(),obsdate, data_dir)
       if (os.path.isfile(pidpath + zipfile) and clobber):
           saltio.delete(pidpath+zipfile)
       elif (os.path.isfile(pidpath+zipfile) and not clobber):
           message = 'ERROR: SALTFTP -- file ' + pidpath + zipfile + ' exists. Use clobber=y'
           raise SaltError(message)

       #tar the files
       saltio.changedir(pidpath)
       tfile = tarfile.open(zipfile,'w:bz2')
       tfile.dereference = True
       log.message('\nSALTFTP -- tarring data for %s/%s in %s' % (pid.upper(), data_dir, zipfile))
       print glob.glob('%s/*' % data_dir)
       for zfile in glob.glob('%s/*' % data_dir):
           if 'bz2' not in zfile:
               #log.message('SALTFTP -- ' + pid.upper() + '/' + zipfile + ' <-- ' + zfile)
               tfile.add(zfile)
       tfile.close()

       #Copy data to the beachhead location
       message=movetobeachhead(pid, zipfile, datapath, beachdir, sdbhost, sdbname, sdbuser,password,clobber)
       log.message(message)

       #delete tar files
       saltio.delete(datapath+pid.upper()+'/'+zipfile)



def tarallfiles(obsdate, pid, datapath, beachdir, sdbhost, sdbname, sdbuser,password,clobber, log, verbose):
   """Put all files for a proposal into a single tar file"""

   pidpath = datapath + pid.upper()
   # overwrite tar files
   zipfile = '%s.%s.tar.bz2' % (pid.upper(),obsdate)
   if (os.path.isfile(pidpath + zipfile) and clobber):
        saltio.delete(pidpath+zipfile)
   elif (os.path.isfile(pidpath+zipfile) and not clobber):
        message = 'ERROR: SALTFTP -- file ' + pidpath + zipfile + ' exists. Use clobber=y'
        raise SaltError(message)

   saltio.changedir(pidpath)
   tfile = tarfile.open(zipfile,'w:bz2')
   tfile.dereference = True
   log.message('\nSALTFTP -- tarring data for %s in %s' % (pid.upper(), zipfile))
   for zfile in glob.glob('*'):
       if 'bz2' not in zfile:
           log.message('SALTFTP -- ' + pid.upper() + '/' + zipfile + ' <-- ' + zfile)
           tfile.add(zfile)
   tfile.close()

   #Copy data to the beachhead location
   message=movetobeachhead(pid, zipfile, datapath, beachdir, sdbhost, sdbname, sdbuser,password,clobber)
   log.message(message)

   #delete tar files
   saltio.delete(datapath+pid.upper()+'/'+zipfile)

def getproposalcodes(obsdate, sdbhost,sdbname,sdbuser, sdbpassword):
    """Retrieve all the proposal observed on a given date"""
    db=saltmysql.connectdb(sdbhost,sdbname,sdbuser,sdbpassword)
    state_select='Distinct Proposal_Code'
    state_from='FileData join ProposalCode using (ProposalCode_Id)'
    state_logic="FileName like '%"+obsdate+"%'"
    record=saltmysql.select(db,state_select,state_from,state_logic)
    if len(record)==0:  
       return []
    else:
       pids=[x[0] for x in record]
    pids=saltio.removebadpids(pids)
    pids=saltio.removeengineeringpids(pids)
    return pids


def getdatefrompath(datapath):
    """From the datapath, retrieve the date"""
    date=''
    for x in datapath:
        if x in string.digits:
             date+=x
    return date

def movetobeachhead(pid,zipfile, datapath, beachdir, sdbhost, sdbname, sdbuser,password, clobber):
   """sub-routine to move the files to the mirror location for the beachhead

        returns status
   """
   #change to the PI directory
   pipath = datapath + pid.upper()
   saltio.changedir(pipath)

   #log into database and determine username from proposer name
   sdbpassword=password
   db=saltmysql.connectdb(sdbhost,sdbname,sdbuser,sdbpassword)
   state_select='p.Username'
   state_from='Proposal as pr join ProposalCode as c using  (ProposalCode_Id) join ProposalContact as pc using (Proposal_Id) join Investigator as i on (pc.Contact_Id=i.Investigator_Id) join PiptUser as p using (PiptUser_Id)'
   state_logic="pr.current=1 and  c.Proposal_Code='%s'" % pid
   record=saltmysql.select(db,state_select,state_from,state_logic)
   record=set(record)

   #check to see if it was successful and raise an error if not
   if len(record)<1:
       message='SALTFTP--Unable to find username for %s' % pid
       raise SaltError(message)

   #go through and add the data to each entry in username
   for entry in record:
       pi_username=entry[0]
       beachpath = beachdir + pi_username
       #copy the zip file to the beachhead directory
       if (os.path.exists(beachpath)): 
           beachzipfile=beachpath+'/'+zipfile
           if clobber and os.path.exists(beachzipfile): saltio.delete(beachzipfile)
           #copy the file
           saltio.copy(zipfile,beachzipfile)
           message = 'SALTFTP -- %s/%s --> %s' % (pid.upper(), zipfile , beachzipfile)
       else:
           msg='SALTFTP--Could not copy %s to %s because %s does not exist' \
                % (zipfile, beachpath, beachpath)
           raise SaltError(msg)

   return message

def movetoftpsite(pid, zipfile, datapath, server, username, password):
   """Move the data to the ftp site"""

   #move into the right directory
   pidpath = datapath + pid.upper()
   saltio.changedir(pidpath)

   #log into the ftp server
   try:
       ftpserv = ftplib.FTP(server,username,password)
       ftpserv.set_pasv(False)
   except Exception, e:
       message='ERROR: SALTFTP -- cannot connect to %s due to %s' % ( server, e)
       raise SaltError(message)

   try:
       ftpserv.cwd(pid.upper())
   except:
       ftpserv.mkd(pid.upper())
       ftpserv.cwd(pid.upper())

   try:
       f = open(zipfile,'rb')
   except Exception, e:
       message='ERROR: SALTFTP -- cannot open %s due to %s' % (zipfile, e)
       raise SaltError(message)

   try:
       ftpserv.storbinary('STOR ' + zipfile,f)
   except Exception, e:
       message = 'ERROR: SALTFTP -- cannot transfer %s to %s due to %s' %  (zipfile, server, e)
       raise SaltError(message)

   try:
       ftpserv.quit()
   except Exception, e:
       message='ERROR: SALTFTP -- cannot disconnect from %s due to %s' % (server, e)

   return 

# -----------------------------------------------------------
# main code

if not iraf.deftask('saltftp'):
  parfile = iraf.osfn("pipetools$saltftp.par")
  t = iraf.IrafTaskFactory(taskname="saltftp",value=parfile,function=saltftp, pkgname='pipetools')
