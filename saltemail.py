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
# Martin Still (SAAO)    0.2          17 Aug 2007
# S M Crawford (SAAO)    0.3          01 Feb 2007

# email PI with 'data-is-ready' notification letter
# v0.3:
# --updated to include mysql query
# --updated to look for propcode and not PI name

from pyraf import iraf
import smtplib, string
from email.mime.text import MIMEText

import saltsafekey as saltkey
import saltsafeio as saltio
import saltsafemysql as saltmysql
import saltsafestring as saltstring
from saltsafelog import logging, history

from salterror import SaltError, SaltIOError

debug=True


# -----------------------------------------------------------
# core routine

def saltemail(propcode, obsdate,readme,server='mail.saao.ac.za',username='', password='',
              bcc='',sdbhost='sdb.saao',sdbname='sdb',sdbuser='pipeline',
              logfile='salt.log',verbose=True):
   """For a given date,  look into the database for observations made on that
      date.  If propcode='all' send an email to all of the PIs that had data taken on that date.
      If propcode gives a specific proposal, only send emails to that PI.
   """

   # set up
   sender = 'sa@salt.ac.za'
   bcclist = bcc.split(',')
   date_obss = []
   propids = []
   pis = []
   pids = []
   email = []


   with logging(logfile,debug) as log:

       #set up the proposal list  
       pids = saltio.argunpack('propcode',propcode)

       #check to see if the argument is defined
       saltio.argdefined('obsdate',obsdate)

       # check readme file exists
       saltio.fileexists(readme)

       #connect to the email server
       try:
            smtp = smtplib.SMTP()
            smtp.connect(server)
	    smtp.ehlo()
	    smtp.starttls()
	    smtp.ehlo()
       except Exception, e:
            message = 'Cannot connect to %s because %s' % (server, e)
            log.error(message)

       try:
            smtp.login(username,password)
       except Exception, e:
            message = 'Cannot login to %s as %s because %s' % (server, username, e)
            log.error(message)

       # log into the mysql data base and download a list of proposal codes
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)
       select='distinct Proposal_Code'
       table='FileData join ProposalCode using (ProposalCode_Id)'
       logic="FileName like '%" + obsdate + "%'"
       records=saltmysql.select(sdb, select, table, logic)

       if len(records)<1:  
          msg="No observations available for %s" % obsdate
          log.warning(msg)
          return
       else:
          for p in records:
              propids.append(p[0])

       #clean the proposal list
       print pids, propids
       try:
           pids=saltio.cleanpropcode(pids, propids)
           pids=saltio.removebadpids(pids)
           pids=saltio.removeengineeringpids(pids)
       except SaltIOError:
           msg="No notifications necessary for %s" % obsdate
           log.warning
           return

       #loop through each of the pids and send the email
       for pid in pids:
         propinfo=findpropinfo(pid, sdb)
         if propinfo:
           for pi, email in zip(propinfo[pid][0], propinfo[pid][1]):
               letter=saltio.openascii(readme,'r')
               msg=letter.read()
               msg=msg.replace('yourname',pi)
               msg=msg.replace('YYYY-INST-PID',pid.upper())
               msg=msg.replace('yyyymmdd',obsdate)
               saltio.closeascii(letter)

               #set up the message to be sent
               recip = []
               #uncomment the following lines if you just want to send the email
               #to yourself
               #email='crawford@saao.ac.za'
               #bcclist=[]
               recip.append(email)
               for bccobj in bcclist:
                   recip.append(bccobj)
               msg = MIMEText(msg)
               msg['Subject'] = 'SALT data available for download for %s' % pid.upper()
               msg['From'] = sender
               msg['To'] = email
               msg['bcc'] = bcc
               try:
                   smtp.sendmail(sender,recip,msg.as_string())
                   pass
               except:
                   message = 'Failed to send email to ' + pi
                   log.error(message)

               message='Email sent to %s at %s' % (pi, email)
               log.message(message)

       #disconnect from the email server
       try:
            smtp.quit()
       except:
            message = 'Cannot disconnect from email server ' + server
            log.error(message)



def findpropinfo(pid,sdb):
    """query the Sdb server and determine the email and surname
       for the contact information for the proposal

       return a dictionary with Surname,Email
    """
    propinfo={}
    #setup the the query
    state_select='pr.Proposal_Id,c.Proposal_Code,i2.Surname,i2.Email'
    state_from='''
	Investigator as i join PiptUser using (PiptUser_Id) join Investigator as i2 on (PiptUser.Investigator_Id=i2.Investigator_Id),
	Proposal as pr 
	  join ProposalCode as c using (ProposalCode_Id) 
	  join  ProposalContact as pc using (Proposal_Id) '''
    state_logic="i.Investigator_Id=pc.Contact_ID and pc.Proposal_ID=pr.Proposal_ID and pr.current=1 and c.Proposal_Code='%s'" %pid
    # left over by intersting way to call it: ORDER BY pr.Proposal_ID DESC" % pid
    record=saltmysql.select(sdb,state_select,state_from,state_logic)

    if len(record)<1:
       message = pid + ' is not in the Science database'
       raise SaltError(message)
    elif len(record)==1:
       surname=[record[0][2]]
       email=[record[0][3]]
       propinfo[pid]=(surname,email)
    else:
       prop_id=-1
       surname=[]
       email=[]
       for entry in record:
           if entry[0] > prop_id:
               prop_id=entry[0]
               prop_code=entry[1]
               surname.append(entry[2])
               email.append(entry[3])
       propinfo[pid]=(surname,email)

    return propinfo

# -----------------------------------------------------------
# main code

if not iraf.deftask('saltemail'):
  parfile = iraf.osfn("pipetools$saltemail.par")
  t = iraf.IrafTaskFactory(taskname="saltemail",value=parfile,function=saltemail, pkgname='pipetools')
