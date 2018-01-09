#!/usr/bin/env python

import os, sys, time, getopt
from saltsdb import SALTSdb


def findnightlog(obsdate):
    """Find the nightlog and create files necessary for the pipeline

    Parameters
    ----------
    obsdate: string
       Observing date to download night log
    """

    # set up the nightlogfile
    logdir='/home/sa/nightlogs/'
    obslog=logdir+obsdate+'.log'

    # download from the database
    getdb(obsdate, obslog)

    # create the listnewheads
    createnewhead(obsdate,obslog)


def createnewhead(obsdate, obslog):
    """Create the new head file from the observing log"""
    print obslog
    obsdata=open(obslog).read()

    notes_str = """------------------------------------------------------------------------
Pipeline Notes
------------------------------------------------------------------------"""
    i=obsdata.index(notes_str)
    j = len(notes_str)
    m=obsdata[i+j:].index('------------------------------------------------------------')
 
    newheadfile='/salt/logs/newheadfiles/list_newhead_%s' % obsdate

    #prevent overwriting
    if os.path.isfile(newheadfile): return

    #write it out to the newheads file
    fout=open(newheadfile, 'w')
    if obsdata[i+j:i+j+m].strip().startswith("No"): 
       fout.write('#')
    fout.write(obsdata[i+j:i+j+m].strip())
    fout.close()


def getdb(obsdate, obslog):
    SDB_SALT_HOST = os.environ['SDB_SALT_HOST']
    SDB_SALT_USER = os.environ['SDB_SALT_USER']
    SDB_SALT_PASS = os.environ['SDB_SALT_PASS']
    SDB_SALT_DB = os.environ['SDB_SALT_DB']

    sdb = SALTSdb(SDB_SALT_HOST, SDB_SALT_DB, SDB_SALT_USER, SDB_SALT_PASS, 3306)
    state_select='SaNightLog'
    state_from='SaLog join NightInfo using (NightInfo_Id)'
    state_logic="Date='%s-%s-%s'" % (obsdate[0:4], obsdate[4:6], obsdate[6:8])
    record=sdb.select(state_select,state_from,state_logic)
    
    body=record['SaNightLog'][0].encode('utf-8')

    fout=open(obslog, 'w')
    fout.write(body)
    fout.close()
       

if __name__ == "__main__":
    import argparse
    import datetime as dt
    parser = argparse.ArgumentParser(description='Find the nightlog and create files for the pipeline')
    parser.add_argument('--date', dest='obsdate', default=None, help='Observation date')
    args = parser.parse_args()

    if args.obsdate is None:
       x = dt.datetime.now()
       x = x - dt.timedelta(seconds=86400)
       obsdate = x.strftime('%Y%m%d')
    else:
       obsdate = args.obsdate
       
    findnightlog(obsdate)

    # this step syncs the nightlogs so they are accessible on the web
    os.system('/usr/local/bin/sanightlogs_sync.sh')
