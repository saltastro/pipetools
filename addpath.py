################################# LICENSE ##################################
# Copyright (c) 2009, South African Astronomical Observatory (SAAO)        #
# All rights reserved.                                                     #
#                                                                          #
############################################################################

""" Inserts the directory containing links to all Python-based tasks
    in the SALT tree to the default Python path.
"""
import os, sys
from pyraf import iraf 
import matplotlib
matplotlib.use('Agg')


_path = iraf.osfn('pipetools$')
if _path not in sys.path:
    sys.path.insert(1,_path)

