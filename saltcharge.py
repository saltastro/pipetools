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
SALTCHARGE calculates the amount of charged time for a night.  It goes
through looking at the SOMMI log, the images taken, and other information
in the database.  

Author                 Version      Date         Comment
-----------------------------------------------------------------------
 S M Crawford (SAAO)    0.1          20 Oct 2011 

"""
# Ensure python 2.5 compatibility
from __future__ import with_statement


import os, time, glob, string, datetime
from pyraf import iraf
import salttime
import saltsafeio as saltio
import saltsafemysql as saltmysql
from saltsafelog import logging
from salterror import SaltError


debug=True

# -----------------------------------------------------------
# core routine

def saltcharge(obsdate, outfile, sdbhost='sdb.saao', sdbname='sdb', \
              sdbuser='', password='', clobber=False, logfile='saltlog.log', verbose=True):
    """Calculate the charged time for proposals observed during a night

    """

    with logging(logfile,debug) as log:

       #check the outfiles
       if not saltio.checkfornone(outfile):
          outfile=None
          
       #check that the output file can be deleted
       if outfile:
           saltio.overwrite(outfile, clobber)

       #connect the database
       sdb=saltmysql.connectdb(sdbhost, sdbname, sdbuser, password)

       #get all the proposals observed during the night
       select_state='distinct Proposal_Code'
       table_state='FileData Join ProposalCode using (ProposalCode_Id)'
       logic_state="FileName like '%"+obsdate+"%'"

       records=saltmysql.select(sdb, select_state, table_state, logic_state)
       pids=[]
       pid_time={}
       for r in records:
           pids.append(r[0])
           pid_time[r[0]]=0
       if len(pids)==0:
           message='There are no proposal to charge time for %s' % obsdate
           log.message(message)
           return

       #get the nightinfo_id
       night_id=saltmysql.getnightinfoid(sdb, obsdate)
       print night_id
       
       #get a list of all the images taken during the night
       select_state='FileName, Proposal_Code, Target_Name, ExposureTime, UTSTART, INSTRUME, OBSMODE, DETMODE, CCDTYPE, NExposures'
       table_state='FileData Join ProposalCode using (ProposalCode_Id) join FitsHeaderImage using (FileData_Id)'
       logic_state="FileName like '%"+obsdate+"%'"
       img_list=saltmysql.select(sdb, select_state, table_state, logic_state) 

       #get all the blocks visited
       select_state='Block_Id, Accepted, Proposal_Code'
       table_state='Block join BlockVisit using (Block_Id) join Proposal using (Proposal_Id) join ProposalCode using (ProposalCode_Id)'
       logic_state='NightInfo_Id=%i' % night_id
       block_list=saltmysql.select(sdb, select_state, table_state, logic_state)
       print block_list

       #get the start and end of twilight
       nightstart=saltmysql.select(sdb, 'EveningTwilightEnd', 'NightInfo', 'NightInfo_Id=%i' % night_id)[0][0]
       nightend=saltmysql.select(sdb, 'MorningTwilightStart', 'NightInfo', 'NightInfo_Id=%i' % night_id)[0][0]
       print nightstart, nightend, (nightend-nightstart).seconds
       print

       #download the SOMMI log from the night
       try:
           sommi_log=saltmysql.select(sdb, 'SONightLog', 'NightLogs', 'NightInfo_Id=%i' % night_id)[0][0]
       except Exception,e :
           msg='Unable to read in SOMMI log for %s' % obsdate
           raise SaltError(msg)

       #add to account for error in SOMMI log
       if int(obsdate)<20111027:
          sommi_log=sommi_log.replace('Object name', '\nObject name')

       #parse the sommi log
       point_list=parseforpointings(sommi_log)

       #now find all blocks observed during a night 
       for point in point_list: 
           #proposal for that block
           pid=point[0].strip()

           #start time for that block
           starttime=point[1]


           #find the end time for that block
           endtime=findnexttime(point[1], point_list, nightend)

           #find the start time of the first object to be observed 
           #for this block
           startimage=findfirstimage(pid, starttime, img_list)
           lastimage=findlastimage(pid, endtime, img_list)

           #check to see if the end time for the last file observed for 
           #this block
           if startimage  is not None:
               if startimage >endtime: startimage =None
   
           #determine the acquisition time for the block
           if startimage  is not None:
               acqdelta=(startimage-starttime).seconds
           else:
               acqdelta=-1

           #find the shutter open time
           shuttertime=calculate_shutteropen(img_list, starttime, endtime)

           #if the shutter and time of the last image is substantially different from the 
           #end of the block, then take the last image as the end of the block
           #*TODO* Change to use expected block time
           st=starttime+datetime.timedelta(0,shuttertime+acqdelta)
           if (endtime-st).seconds>900:
              if lastimage is None: 
                   endtime=st
              elif (lastimage-st).seconds>3600:
                   endtime=st
              else:
                   endtime=lastimage
           #account for the time for that block
           tblock=(endtime-starttime).seconds

           if acqdelta>-1:
               #find the associated block
               block_id, blockvisit_id, accepted=getblockid(sdb, night_id, pid, img_list, starttime, endtime)

 
               if accepted and pid.count('-3-'):
                   #charged time for that block
                   try:
                       pid_time[pid] += tblock
                   except KeyError:
                       pid_time[pid] = tblock 

                   print block_id, blockvisit_id, pid, starttime, tblock, acqdelta, shuttertime, shuttertime/tblock, block_id, accepted
               #update the charge time
               slewdelta=0
               scidelta=tblock-acqdelta
               update_cmd='TotalSlewTime=%i, TotalAcquisitionTime=%i, TotalScienceTime=%i' % (slewdelta, acqdelta, scidelta)
               table_cmd='BlockVisit'
               logic_cmd='BlockVisit_Id=%i' % blockvisit_id
               saltmysql.update(sdb, update_cmd, table_cmd, logic_cmd)

       return
                  
       #update the charged time information
       #TODO: Check to see if the block was approved
       ptime_tot=0
       stime_tot=0
       for k in pid_time:
           ptime=pid_time[k]
           if ptime>0:  
              obsratio=stime/ptime
           else:
              obsratio=0
           print '%25s %5i %5i %3.2f' % (k, ptime, stime, obsratio)
           if k.count('-3-'):
               ptime_tot += ptime
               stime_tot += stime


       #print out the total night statistics
       tdelta=nightend-nightstart
       print tdelta.seconds, ptime_tot, stime_tot

    

def getblockid(sdb, night_id, pid, img_list, starttime, endtime):
    """Return the block id for this set of data"""
    blockid=-1
    targlist=[]
    for img in img_list:
        itime=img[4]+datetime.timedelta(0,7200.0)
        if itime>starttime and itime<endtime:
           if img[8]=='OBJECT':
               targlist.append(img[2])

    for t in set(targlist):
       select_state='Block_Id, BlockVisit_Id, Accepted'
       table_state='''Block 
   join BlockVisit using (Block_Id) 
   join Pointing using (Block_Id) 
   join Observation using (Pointing_Id) 
   join Target using (Target_Id)
   join Proposal using (Proposal_Id) 
   join ProposalCode using (ProposalCode_Id)
'''
       logic_state="NightInfo_Id=%i and Target_Name='%s' and Proposal_Code='%s'" % (night_id, t, pid.strip())
       block_list=saltmysql.select(sdb, select_state, table_state, logic_state)
       

    try:
       return block_list[0]
    except:
       return -1, -1, -1

def calculate_shutteropen(img_list, starttime, endtime):
    """Calculate the amount of shutter open time for all object
       type of observations that occur between starttime and endtime

    """ 
    stime=0
    for img in img_list:
        itime=img[4]+datetime.timedelta(0,7200.0)
        if itime>starttime and itime<endtime:
           if img[8]=='OBJECT':
               stime+=img[3]*img[9]

    return stime

'''
def calculate_shutteropen(sdb, obsdate, pid):
   """Calculate the amount of shutter open time for a proposal on a given obsdate 
      
      TODO--Need to account for slotmode observations
          --Need to only retrieve data taken during the night
   """
   select_state='Sum(ExposureTime*NExposures)'
   table_state='FileData Join ProposalCode using (ProposalCode_Id) Join FitsHeaderImage using (FileData_Id)'
   logic_state="FileName like '%"+obsdate+"%'and CCDTYPE='OBJECT' and Proposal_Code='"+pid+"'"

   records=saltmysql.select(sdb, select_state, table_state, logic_state)
   if records[0][0] is not None:
       return records[0][0]
   else:
       return 0
'''

def findfirstimage(pid, starttime, img_list):
   tdiff=1e5
   tfirst=None
   fname=''
   for img in img_list:
       if pid==img[1]:
           tdelta=img[4]+datetime.timedelta(0,7200)-starttime
           if tdelta.days==0 and tdelta.seconds< tdiff:
              tdiff=tdelta.seconds
              tfirst=img[4]+datetime.timedelta(0,7200)
              fname=img[0]
   return tfirst

def findlastimage(pid, endtime, img_list):
   tdiff=1e5
   tfirst=None
   for img in img_list:
       if pid==img[1]:
           tdelta=endtime-img[4]-datetime.timedelta(0,7200)
           if tdelta.days==0 and tdelta.seconds< tdiff:
              tdiff=tdelta.seconds
              tfirst=img[4]+datetime.timedelta(0,7200)
   return tfirst


def findnexttime(stime, point_list, nightend):
   """Find the next time in the point list"""
   etime=nightend
   dt=3600*24
   for t in point_list:
       tdelta=t[1]-stime
       if tdelta.days==0 and tdelta.seconds<dt and tdelta.seconds>0:
          etime=t[1]
          dt=tdelta.seconds

   if etime>nightend: return nightend

   return etime
       
def parseforpointings(sommi_log):
   """Parse the sommi log for all the point to target commands"""
   point_list=[]

   slog=sommi_log.split('\n')
   found=0
   for i in range(len(slog)):
       if slog[i].count('Point to Target command'):
          #get the proposal code
          propid=slog[i+1].split(':')[1].strip()

          #create teh datetime of the obsevation
          try:
             oinfo=slog[i-2].split()
             odate=oinfo[0].split('/')
             otime=oinfo[1].split(':')
             year=int(odate[0])
             month=int(odate[1])
             day=int(odate[2])
             hour=int(otime[0])
             minute=int(otime[1])
             second=int(otime[2])
             odatetime=datetime.datetime(year, month, day, hour, minute, second)
 
             #get the target
             target=slog[i+2].split(':')[1].strip()
              
             point_list.append([propid, odatetime, target])
          except Exception, e:
             print "On line %i in SOMMI Log, failed to read in Point to Target command" % i
             print e

 
   return point_list
       
# -----------------------------------------------------------
# main code

parfile = iraf.osfn("pipetools$saltcharge.par")
t = iraf.IrafTaskFactory(taskname="saltcharge",value=parfile,function=saltcharge, pkgname='pipetools')
