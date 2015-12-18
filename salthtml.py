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
# Martin Still (SAAO)    0.2          21 Jul 2006
# S M Crawford (SAAO)    0.3          06 Feb 2008
# S M Crawford (SAAO)    0.4          10 Oct 2013

# salthtml creates HTML documentation describing telescope logs and pipeline processing.
# 20080206--Updated to use propcode instead of proposer name

from pyraf import iraf
import saltio, saltprint, salttime
import os, glob, string

# -----------------------------------------------------------
# core routine

def salthtml(propcode,scamobslog,rssobslog, hrsobslog, htmlpath,nightlog,readme,clobber,logfile,verbose):

# set up
   
    filenames = []
    proposers = []
    propids = []
    instrumes = []
    objects = []
    ras = []
    decs = []
    detmodes = []
    ccdtypes = []
    ccdsums = []
    gainsets = []
    rospeeds = []
    filters = []
    gratings = []
    gr_angles = []
    ar_angles = []
    time_obss = []
    date_obss = []
    exptimes = []
    hours = []
    filename = {}
    proposer = {}
    propid = {}
    instrume = {}
    object = {}
    ra = {}
    dec = {}
    detmode = {}
    ccdsum = {}
    ccdtype = {}
    gainset = {}
    rospeed = {}
    filter = {}
    grating = {}
    gr_angle = {}
    ar_angle = {}
    time_obs = {}
    exptime = {}
    status=0

# test the logfile

    logfile = saltio.logname(logfile)

# log the call

    saltprint.line(logfile,verbose)
    history = 'SALTHTML -- '
    history += 'scamobslog='+scamobslog+' '
    history += 'rssobslog='+rssobslog+' '
    history += 'htmlpath='+htmlpath+' '
    history += 'nightlog='+nightlog+' '
    history += 'readme='+readme+' '
    yn = 'n'
    if (clobber): yn = 'y'
    history += 'clobber='+yn+' '
    history += 'logfile='+logfile+' '
    yn = 'n'
    if (verbose): yn = 'y'
    history += 'verbose='+yn
    saltprint.log(logfile,history,verbose)

# start time

    saltprint.time('\nSALTHTML -- started at',logfile,verbose)
    saltprint.log(logfile,' ',verbose)

# are the arguments defined

    if (status == 0): pids,status = saltio.argunpack('propcode',propcode ,logfile)
    if (status == 0): status = saltio.argdefined('scamobslog',scamobslog,logfile)
    if (status == 0): status = saltio.argdefined('rssobslog',rssobslog,logfile)
    if (status == 0): status = saltio.argdefined('htmlpath',htmlpath,logfile)



# check htmlpath exists, ends with a "/" and convert to absolute path

    if (status == 0): htmlpath, status = saltio.abspath(htmlpath,logfile)

# check observation log files exist

    if (status == 0 and scamobslog.upper() != 'NONE'): status = saltio.fileexists(scamobslog,logfile)
    if (status == 0 and rssobslog.upper() != 'NONE'): status = saltio.fileexists(rssobslog,logfile)

# read observation logs

    for obslog in [scamobslog, rssobslog, hrsobslog]:
        if (status == 0 and obslog.upper() != 'NONE'): struct,status = saltio.openfits(obslog,logfile)
        if (status == 0 and obslog.upper() != 'NONE'): obstab,status = saltio.readtab(struct[1],obslog,logfile)
        if (status == 0 and obslog.upper() != 'NONE'): status = saltio.closefits(struct,logfile)
        if (status == 0 and obslog.upper() != 'NONE'):
            filenames.extend(obstab.field('filename'))
            objects.extend(obstab.field('object'))
            ras.extend(obstab.field('ra'))
            decs.extend(obstab.field('dec'))
            instrumes.extend(obstab.field('instrume'))
            proposers.extend(obstab.field('proposer'))
            propids.extend(obstab.field('propid'))
            ccdtypes.extend(obstab.field('ccdtype'))
            ccdsums.extend(obstab.field('ccdsum'))
            gainsets.extend(obstab.field('gainset'))
            rospeeds.extend(obstab.field('rospeed'))
            detmodes.extend(obstab.field('detmode'))
            filters.extend(obstab.field('filter'))
            time_obss.extend(obstab.field('time-obs'))
            date_obss.extend(obstab.field('date-obs'))
            exptimes.extend(obstab.field('exptime'))
            if (obslog == rssobslog):
                gratings.extend(obstab.field('grating'))
                gr_angles.extend(obstab.field('gr-angle'))
                ar_angles.extend(obstab.field('ar-angle'))
            else:
                for i in range(len(filenames)):
                    gratings.append(' ')
                    gr_angles.append(0.)
                    ar_angles.append(0.)


# Create the list of proposals

    if (status == 0): pids,status=saltio.cleanpropcode(pids, propids, logfile)


# date of observations
    
    date, caldate = salttime.date_obs2yyyymmdd(date_obss[0])

# sort into chronological order

    for i in range(len(filenames)):
        hours.append(salttime.time_obs2hr(time_obss[i]))
        if (hours[i] < 12.): hours[i] += 24
        filename[str(hours[i])] = filenames[i]
        object[str(hours[i])] = objects[i]
        ra[str(hours[i])] = ras[i]
        dec[str(hours[i])] = decs[i]
        instrume[str(hours[i])] = instrumes[i]
        proposer[str(hours[i])] = proposers[i]
        propid[str(hours[i])] = propids[i]
        ccdsum[str(hours[i])] = ccdsums[i].replace(' ','x')
        ccdtype[str(hours[i])] = ccdtypes[i]
        gainset[str(hours[i])] = gainsets[i]
        rospeed[str(hours[i])] = rospeeds[i]
        detmode[str(hours[i])] = detmodes[i]
        filter[str(hours[i])] = filters[i]
        time_obs[str(hours[i])] = time_obss[i]
        grating[str(hours[i])] = gratings[i]
        gr_angle[str(hours[i])] = gr_angles[i]
        ar_angle[str(hours[i])] = ar_angles[i]
        exptime[str(hours[i])] = exptimes[i]
        if (instrume[str(hours[i])] == 'SALTICAM'): instrume[str(hours[i])] = 'SCM'
        if ('Video Mode' in detmode[str(hours[i])]): detmode[str(hours[i])] = 'VI'
        if ('Slot Mode' in detmode[str(hours[i])]): detmode[str(hours[i])] = 'SL'
        if ('Frame Transfer' in detmode[str(hours[i])]): detmode[str(hours[i])] = 'FT'
        if ('Normal' in detmode[str(hours[i])]): detmode[str(hours[i])] = 'IM'
        if ('Bright' in gainset[str(hours[i])]): gainset[str(hours[i])] = 'BR'
        if ('Faint' in gainset[str(hours[i])]): gainset[str(hours[i])] = 'FA'
        if ('Fast' in rospeed[str(hours[i])]): rospeed[str(hours[i])] = 'FA'
        if ('Slow' in rospeed[str(hours[i])]): rospeed[str(hours[i])] = 'SL'
        if ('OBJECT' not in ccdtype[str(hours[i])].upper() and
            'UNKNOWN' in proposer[str(hours[i])].upper()):
            proposer[str(hours[i])] = ''
    hours.sort()

# create HTML directory in datapath and define html files

    docpath = htmlpath + 'doc/'
    if (status == 0 and not os.path.exists(docpath)): status = saltio.createdir(docpath,False,logfile)

    htmlfile = docpath + 'ObservationSequence' + date +'.html'
    notefile = docpath + 'CapeTownNotes' + date + '.html'
    nlogfile = docpath + 'AstronomersLog' + date + '.html'
    plogfile = docpath + 'PipelineLog' + date + '.html'
    elogfile = docpath + 'EnvironmentLog' + date + '.html'
    dlogfile = docpath + 'InstrumentDiagnostics' + date + '.html'

# Copy css and banner images to the doc directory
    if (status == 0):
        status=saltio.copy(iraf.osfn('pipetools$html/style.css'),docpath,False,logfile)
        status=saltio.copy(iraf.osfn('pipetools$html/style_home.css'),docpath,False,logfile)
        status=saltio.copy(iraf.osfn('pipetools$html/header_salt.jpg'),docpath,False,logfile)


# write observation log html file

    if (status == 0):
        status = writeobslog(htmlfile,filename,object,ra,dec,instrume,detmode,filter,ccdsum,
                             gainset,rospeed,grating,gr_angle,ar_angle,exptime,time_obs,proposer,
                             propid,hours,date,caldate,clobber,logfile,verbose,status)

# write readme html file

    if (status == 0):
        status = writecapetownnotes(notefile,readme,date,caldate,clobber,logfile,verbose,status)

# write nightlog html file

    if (status == 0):
        status = writenightlog(nlogfile,nightlog,date,caldate,clobber,logfile,verbose,status)

# write pipeline log html file

    if (status == 0):
        status = writepipelog(plogfile,date,caldate,clobber,logfile,verbose,status)

# write environment log html file

    if (status == 0):
        status = writeenvlog(elogfile,date,caldate,clobber,logfile,verbose,status)

# write instrument diagnostics html file

    if (status == 0):
        status = writediaglog(dlogfile,date,caldate,clobber,logfile,verbose,status)

# copy html files to PI directories

    if (status == 0):
        saltprint.log(logfile,' ',verbose)
        for pids in set(propids):
            for pid in pids.split(','):
                pid = pid.strip().upper()
                pidpath = htmlpath + pid
                if (os.path.exists(pidpath)):
                    if (os.path.exists(pidpath+'/doc')):
                        for file in glob.glob(pidpath+'/doc/*'):
                            status = saltio.delete(file,False,logfile)
                        status = saltio.deletedir(pidpath+'/doc',logfile)
                    status = saltio.copydir(docpath,pidpath+'/doc',verbose,logfile)
                    if (status == 0):
                        infile, status = saltio.openascii(pidpath+'/doc/CapeTownNotes'+date+'.html','r',logfile)
                    if (status == 0): saltio.delete(pidpath+'/doc/CapeTownNotes'+date+'.html',False,logfile)
                    if (status == 0):
                        outfile, status = saltio.openascii(pidpath+'/doc/CapeTownNotes'+date+'.html','w',logfile)
                        if (status == 0):
                            for line in infile:
                                #line = line.replace('SALT user','Dr ' + string.capwords(pi.lower()))
                                #line = line.replace('yourname',pi)
                                line = line.replace('yyyymmdd',date)
                                outfile.write(line)
                            status = saltio.closeascii(outfile,logfile)

# end time

    if (status == 0):
        saltprint.time('\nSALTHTML -- completed at',logfile,verbose)
    else:
        saltprint.time('\nSALTHTML -- aborted at',logfile,verbose)

# -----------------------------------------------------------
# write html header

def htmlheader(file,date,caldate,title,fontsize,logfile):

# write html header

    status = 0

    file.write("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\">\n")
    file.write("<html>\n")
    file.write("<head>\n")
    file.write("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=iso-8859-1\" />\n")
    file.write("  <base href=\"test.html\" />\n")
    file.write("  <link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" />\n")
    file.write("  <Link rel=\"stylesheet\" type=\"text/css\" href=\"style_home.css\" />\n")
    file.write("<title>" + title + date + "</title>\n")
    file.write("</head>\n")
    file.write("<body bgcolor=\"#FFFFFF\">\n")
    file.write("</STYLE>\n")
    file.write("<table width=\"900px\" align=\"center\" border=\"0\" cellpadding=\"0\" cellspacing=\"0\" bgcolor=\"#FFFFFF\" style=\"table-layout: fixed; margin-top: 0px; margin-bottom: 0px;\">\n")
    file.write("  <tr bgcolor=\"#000000\">\n")
    file.write("    <td>\n")
    file.write("      <img src=\"header_salt.jpg\" width=\"900px\" alt=\"SALT - Southern African Large Telescope\" />\n")
    file.write("    </td>\n")
    file.write("  </tr>\n")
    file.write("  <tr bgcolor=\"#000000\">\n")
    file.write("    <td>\n")
    file.write("          <div align=\"center\" id=\"menu\"><ul id=\"menuList\" class=\"adxm\">\n")
    file.write("              <li><a href=\"ObservationSequence" + date + ".html\">Observation Sequence</a></li>\n")
    file.write("              <li><a href=\"PipelineLog" + date + ".html\">Pipeline Log</a></li>\n")
    file.write("              <li><a href=\"AstronomersLog" + date + ".html\">Astronomer's Log</a></li>\n")
    file.write("              <li><a href=\"EnvironmentLog" + date + ".html\">Environment Log</a></li>\n")
    file.write("              <li><a href=\"InstrumentDiagnostics" + date + ".html\">Instrument Diagnostics</a></li>\n")
    file.write("              <li><a href=\"CapeTownNotes" + date + ".html\">Cape Town Notes</a></li>\n")
    file.write("              <li><a href=\"CapeTownNotes" + date + ".html\"><font color=\"#CCFF66\">" + caldate + "<font></a></li>\n")
    file.write("          </div>\n")
    file.write("    </td>\n")
    file.write("  </tr>\n")
    file.write("  <tr>\n")
    file.write("    <td>\n")
    file.write("      <div id=\"breadcrumbs\"></div>\n")
    file.write("      <div class=\"clear\"></div>\n")
    file.write("      <div id=\"page\">\n")
    file.write("        <div id=\"page-left\">\n")
    file.write("          <a name=\"8\"></a>\n")
    file.write("<font size=\"" + str(fontsize) + "\"\n")
    file.write("face=\"ariel,helvetica,swiss,sanserif\">\n\n")
    file.write("\n")
    file.write("<pre style=\"white-space: -moz-pre-wrap;\n")
    file.write("            white-space: -pre-wrap;\n")
    file.write("            white-space: -o-prewrap;\n")
    file.write("            white-space: pre-wrap;\n")
    file.write("            word-wrap: break-word;\">\n")
    file.write("\n")

    return status

# -----------------------------------------------------------
# write html footer

def htmlfooter(file,logfile):

    file.write("\n</pre>\n")
    file.write("\n</font></center>\n\n")
    file.write("        </div>\n")
    file.write("      </div>\n")
    file.write("      <div id=\"footer\">\n")
    file.write("        &copy SAAO 2007\n")
    file.write("      </div>\n")
    file.write("    </td>\n")
    file.write("  </tr>\n")
    file.write("</table>\n")
    file.write("</body>\n")
    file.write("</html>\n")
    status = saltio.closeascii(file,logfile)

    return status

# -----------------------------------------------------------
# write html template

def templatehtml(file,templatefile,logfile):

    line = ' '
    status = saltio.fileexists(templatefile,logfile)
    if (status == 0): infile, status = saltio.openascii(templatefile,'r',logfile)
    while line:
        line = infile.readline()
        file.write(line)

    return status

# -----------------------------------------------------------
# write observation log page

def writeobslog(htmlfile,filename,object,ra,dec,instrume,detmode,filter,
                ccdsum,gainset,rospeed,grating,gr_angle,ar_angle,exptime,
                time_obs,proposer,propid,hours,date,caldate,clobber,logfile,verbose,status):

# overwrite observation log html file

    if (status == 0 and os.path.isfile(htmlfile) and clobber):
        status = saltio.delete(htmlfile,False,logfile)
    elif (status == 0 and os.path.isfile(htmlfile) and not clobber):
        message = 'ERROR: SALTHTML -- file ' + htmlfile + ' exists. Use clobber=y'
        status = saltprint.err(logfile,message)

# open observation log html file

    if (status == 0):
        line = ' '
        saltprint.log(logfile,'SALTHTML -- creating ObservationSequence' + date + '.html',verbose)
        outfile, status = saltio.openascii(htmlfile,'w',logfile)

# write html header

    if (status == 0): status = htmlheader(outfile,date,caldate,'ObservationLog',-1,logfile)

# write table headings

    if (status == 0):
        outfile.write("%14s %12s %10s %9s %3s %2s %7s %3s %2s %2s %6s %5s %6s %6s %8s %15s %16s \n" %
                      ('file'.ljust(14),
                       'object'.ljust(12),
                       'ra2000'.rjust(10),
                       'dec2000'.rjust(9),
                       'ins'.ljust(3),
                       'md'.ljust(2),
                       'filter'.rjust(7),
                       'bin'.rjust(3),
                       'gn'.rjust(2),
                       'sp'.rjust(2),
                       'grat'.rjust(6),
                       'gr-ang'.ljust(6),
                       'ar-ang'.ljust(6),
                       'exp'.rjust(6),
                       'UT'.rjust(8),
                       'Code'.ljust(15),
                       'PI'.ljust(16)))
        for i in range(140):
            outfile.write('-')
        outfile.write('\n')

# write table

        for i in hours:
            outfile.write("%15s %12s %10s %9s %3s %2s %7s %3s %2s %2s %6s %5.2f %6.2f %6.1f %8s %15s %16s\n" %
                          (filename[str(i)].replace('.fits','')[:13].ljust(13),
                           object[str(i)][:12].ljust(12),
                           ra[str(i)][:10].ljust(10),
                           dec[str(i)][:9].ljust(9),
                           instrume[str(i)][:3].ljust(3),
                           detmode[str(i)][:2].ljust(2),
                           filter[str(i)][:7].rjust(7),
                           ccdsum[str(i)][:3].rjust(3),
                           gainset[str(i)][:2].rjust(2),
                           rospeed[str(i)][:2].rjust(2),
                           grating[str(i)][:6].ljust(6),
                           gr_angle[str(i)],
                           ar_angle[str(i)],
                           exptime[str(i)],
                           time_obs[str(i)][:8].ljust(8),
                           propid[str(i)][:15].ljust(15),
                           proposer[str(i)][:16].ljust(16)))

# write html foolter

    if (status == 0): status = htmlfooter(outfile,logfile)

    return status

# -----------------------------------------------------------
# create Cape Town Note web page

def writecapetownnotes(notefile,readme,date,caldate,clobber,logfile,verbose,status):

# overwrite old observation log html file if it exists

    line = ' '
    if (status == 0): status = saltio.overwrite(notefile,clobber,logfile)

# open observation log html file

    if (status == 0):
        saltprint.log(logfile,'SALTHTML -- creating CapeTownNotes' + date + '.html',verbose)
        outfile, status = saltio.openascii(notefile,'w',logfile)

# write html header

    if (status == 0): status = htmlheader(outfile,date,caldate,'CapeTownNotes',0,logfile)

# readme file exists?

    if (status == 0):
        if (not os.path.isfile(readme)):
            status=1
            message = 'ERRROR: SALTHTML -- readme file does not exist'
        else:

# open nightlog file

            if (status == 0): infile, status = saltio.openascii(readme,'r',logfile)

# append night log to html file

            if (status == 0):
                while line:
                    line = infile.readline()
                    outfile.write(line)

# append readme to html file

    if (status == 0):
        while line:
            line = infile.readline()
            outfile.write(line)

# write html footer

    if (status == 0): status = htmlfooter(outfile,logfile)

    return status

# -----------------------------------------------------------
# create Night Log web page

def writenightlog(nlogfile,nightlog,date,caldate,clobber,logfile,verbose,status):

# overwrite old observation log html file if it exists

    line = ' '
    if (status == 0): status = saltio.overwrite(nlogfile,clobber,logfile)

# open observation log html file

    if (status == 0):
        saltprint.log(logfile,'SALTHTML -- creating AstronomersLog' + date + '.html',verbose)
        outfile, status = saltio.openascii(nlogfile,'w',logfile)

# write html header

    if (status == 0): status = htmlheader(outfile,date,caldate,'AstronomersLog',0,logfile)

# nightlog file exists?

    if (status == 0):
        if (not os.path.isfile(nightlog)):
            message = 'WARNING: SALTHTML -- night log does not exist'
        else:

# open nightlog file

            if (status == 0): infile, status = saltio.openascii(nightlog,'r',logfile)

# append night log to html file

            if (status == 0):
                while line:
                    line = infile.readline()
                    outfile.write(line)

# write html footer

    if (status == 0): status = htmlfooter(outfile,logfile)

# close htmlfile

    if (status == 0): status = saltio.closeascii(outfile,logfile)

    return status

# -----------------------------------------------------------
# create Pipeline Log web page

def writepipelog(plogfile,date,caldate,clobber,logfile,verbose,status):

# overwrite old pipeline log html file if it exists

    line = ' '
    if (status == 0): status = saltio.overwrite(plogfile,clobber,logfile)

# open pipeline log html file

    if (status == 0):
        saltprint.log(logfile,'SALTHTML -- creating PipelineLog' + date + '.html',verbose)
        outfile, status = saltio.openascii(plogfile,'w',logfile)

# write html header

    if (status == 0): status = htmlheader(outfile,date,caldate,'PipelineLog',0,logfile)

# nightlog file exists?

    if (status == 0):
        if (not os.path.isfile(logfile)):
            message = 'WARNING: SALTHTML -- pipeline log does not exist'
        else:

# open nightlog file

            if (status == 0): infile, status = saltio.openascii(logfile,'r',logfile)

# append night log to html file

            if (status == 0):
                while line:
                    line = infile.readline()
                    outfile.write(line)

# write html footer

    if (status == 0): status = htmlfooter(outfile,logfile)

# close htmlfile

    if (status == 0): status = saltio.closeascii(outfile,logfile)

    return status

# -----------------------------------------------------------
# create Environment Log web page

def writeenvlog(elogfile,date,caldate,clobber,logfile,verbose,status):

# overwrite old environemnt log html file if it exists

    line = ' '
    if (status == 0): status = saltio.overwrite(elogfile,clobber,logfile)

# open environment log html file

    if (status == 0):
        saltprint.log(logfile,'SALTHTML -- creating EnvironmentLog' + date + '.html',verbose)
        outfile, status = saltio.openascii(elogfile,'w',logfile)

# write html header

    if (status == 0): status = htmlheader(outfile,date,caldate,'EnvironmentLog',0,logfile)

# content pending

    outfile.write('Content pending\n')

# write html footer

    if (status == 0): status = htmlfooter(outfile,logfile)

# close htmlfile

    if (status == 0): status = saltio.closeascii(outfile,logfile)

    return status

# -----------------------------------------------------------
# create Instrument Diagnostic web page

def writediaglog(dlogfile,date,caldate,clobber,logfile,verbose,status):

# overwrite old diagnostic html file if it exists

    line = ' '
    if (status == 0): status = saltio.overwrite(dlogfile,clobber,logfile)

# open diagnostic log html file

    if (status == 0):
        saltprint.log(logfile,'SALTHTML -- creating InstrumentDiagnostics' + date + '.html',verbose)
        outfile, status = saltio.openascii(dlogfile,'w',logfile)

# write html header

    if (status == 0): status = htmlheader(outfile,date,caldate,'InstrumentDiagnostics',0,logfile)

# content pending

    outfile.write('Content pending\n')

# write html footer

    if (status == 0): status = htmlfooter(outfile,logfile)

# close htmlfile

    if (status == 0): status = saltio.closeascii(outfile,logfile)

    return status

# -----------------------------------------------------------
# main code

if not iraf.deftask('salthtml'):
  parfile = iraf.osfn("pipetools$salthtml.par")
  t = iraf.IrafTaskFactory(taskname="salthtml",value=parfile,function=salthtml, pkgname='pipetools')
