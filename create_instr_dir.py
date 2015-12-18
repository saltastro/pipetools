#!/usr/bin/env python
"""For a given instrument, check to see if the raw data directory exists and
if not, create it
"""
import os
import sys
import datetime as dt
import getopt
from pynsca import NSCANotifier

def usage():
    print """

create_instr_dir [instr] 

Create an instrument directory for an observing date.  If no observing date is given, 
it will use today's date.  Otherwise please specify the observing date using

create_instr_dir --date=20140909 [instr]

The [instr] should be one of scam, rss, hrdet, hbdet

"""
def nagios_passive_report(host,monitoredhost,service,level,message):
        # use nsca to report to nagios
        status={0:"OK: ",1:"WARNING: ",2:"CRITICAL: "}
        try:
                notif = NSCANotifier(host,5667,0)
                notif.svc_result(monitoredhost, service, level, status[level]+message)
        except:
                return False
        return True

    
obsdate = (dt.datetime.now() - dt.timedelta(seconds=86400)).strftime('%Y%m%d')

#get the arguments
try:
    opts, args = getopt.getopt(sys.argv[1:],"h:d:",["help","date="])
except getopt.GetoptError:
    usage()
    sys.exit(2)

if len(args)<1: 
    usage()
    sys.exit(2)

for o, a in opts:
    if o in ("-h", "--help"):
	   usage()
	   sys.exit()
    if o in ("-d", "--date"):
	   obsdate = str(a)


instr = args[0]

#set up the two directories that should exist
rawdir = '/salt/%s/data/%s/%s/raw/' % (instr, obsdate[0:4], obsdate[4:8])
diskfile = rawdir+'disk.file'

#check to see if directory exists
print rawdir
if os.path.isdir(rawdir):
   print '%s alread exists' % rawdir
   exit()

print 'Creating new directory'
#if not create it
if instr=='scam':
   prefix='S'
if instr=='rss':
   prefix='R'
if instr=='hrdet':
   prefix='R'
if instr=='hbdet':
   prefix='H'

try:
   os.mkdir('/salt/%s/data/%s/%s/' %  (instr, obsdate[0:4], obsdate[4:8]))
   os.mkdir(rawdir)
   fout = open(diskfile, 'w')
   fout.write('%s%s0001.fits' % (prefix, obsdate))
   fout.close()
   nagios_passive_report("manage1.cape.saao.ac.za","saltpipe.cape.saao.ac.za","check_product_data_dir_exists",1,instr+" created dir : "+rawdir)   
except Exception, e:
   nagios_passive_report("manage1.cape.saao.ac.za","saltpipe.cape.saao.ac.za","check_product_data_dir_exists",2,instr+" failed to create "+rawdir+" due to "+str(e))
   raise e
   
   



