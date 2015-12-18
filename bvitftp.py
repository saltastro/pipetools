################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

#!/usr/bin/env python
"""
BVITFTP transfers PI-collated data to the FTP server
for collection by the PI.  

 Author                 Version      Date
 -----------------------------------------------
 S M Crawford (SAAO)    0.1          10 Mar 2013


UPDATES
------------------------------------------------
"""

# Ensure python 2.5 compatibility
from __future__ import with_statement

import string

from pyraf import iraf
import tarfile, glob, os, ftplib, shutil

import saltsafeio as saltio
import saltsafemysql as saltmysql
from salterror import SaltError, SaltIOError

from saltsafelog import logging
debug=True

from saltemail import findpropinfo

# -----------------------------------------------------------
# core routine

def bvitftp(propcode, obsdate, sdbhost,sdbname,sdbuser, password, 
            server,username,sender, bcc, emailfile,
            notify=False, clobber=True,logfile='salt.log',verbose=True):

   # set up

   proposers = []
   propids = []
   pids = []
   
   with logging(logfile,debug) as log:

       # are the arguments defined
       if propcode.strip().upper()=='ALL':
          pids=getbvitproposalcodes(str(obsdate))
       else:
          pids = saltio.argunpack('propcode',propcode)
       
       if len(pids)==0:
           #throw a warning adn exit if not data needs to be filterd
           log.warning('No data to distribute\n', with_stdout=verbose)
           return
   
       #log into the database
       sdb=saltmysql.connectdb(sdbhost,sdbname,sdbuser,password)
 
     
       # check PI directories exist
       ftpdir='/salt/ftparea/'
       for datapath in pids:
           pid=os.path.basename(datapath)
           movebvitdata(pid, datapath, ftpdir, obsdate, sdb, clobber)

           if notify:
               notifybvituser(sdb, pid, obsdate, server,username,password,sender, bcc, emailfile)
           

def notifybvituser(sdb, pid, obsdate, server,username,password,sender, bcc, emailfile):
    """Send email notification to the user"""
    
    propinfo=findpropinfo(pid,sdb)

    for surname, recipient in zip(propinfo[pid][0], propinfo[pid][1]):
        subject='BVIT data available for download for %s' % pid
        msg=open(emailfile).read()
        msg=msg.replace('YYYY-INST-PID',pid.upper())
        msg=msg.replace('yyyymmdd',obsdate)
        saltio.email(server,username,password,sender,recipient,bcc, subject,msg)
    
           

def getbvitproposalcodes(obsdate):
    """Retrieve all the proposal observed on a given date"""
    pids=glob.glob('/salt/bvit/data/%s/%s/*-*-*' % (obsdate[0:4], obsdate[4:8]))
    for p in pids:
        if p.count('Detector-Background'):pids.remove(p)
    pids=saltio.removebadpids(pids)
    pids=saltio.removeengineeringpids(pids)
    return pids


def movebvitdata(pid, datapath, ftpdir, obsdate, sdb, clobber):
   """sub-routine to move the files to the mirror location for the beachhead

        returns status
   """
   #log into database and determine username from proposer name
   state_select='p.Username'
   state_from='Proposal as pr join ProposalCode as c using  (ProposalCode_Id) join ProposalContact as pc using (Proposal_Id) join Investigator as i on (pc.Contact_Id=i.Investigator_Id) join PiptUser as p using (PiptUser_Id)'
   state_logic="pr.current=1 and  c.Proposal_Code='%s'" % pid
   record=saltmysql.select(sdb,state_select,state_from,state_logic)
   record=set(record)

   #check to see if it was successful and raise an error if not
   if len(record)<1:
       message='SALTFTP--Unable to find username for %s' % pid
       raise SaltError(message)

   #go through and add the data to each entry in username
   for entry in record:
       pi_username=entry[0]
       ftppath=ftpdir+'/'+pi_username+'/'+pid+'_'+obsdate
       #copy the zip file to the beachhead directory
       if clobber and os.path.exists(ftppath): shutil.rmtree(ftppath)
           #copy the file
       shutil.copytree(datapath, ftppath)
       message = 'SALTFTP -- %s --> %s' % (datapath, ftppath)


   return message

# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$bvitftp.par")
t = iraf.IrafTaskFactory(taskname="bvitftp",value=parfile,function=bvitftp, pkgname='pipetools')
