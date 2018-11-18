#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.SYNOPSIS
  This script imports data from the Api of vaarweginformatie.nl and stores it in a GraphDB
  
.DESCRIPTION

.NOTES
  Author: Daniël Overdevest
"""

import extractor
import graphdb
import logging
import os
import datetime
import pytz

def main():
    scriptTime = datetime.datetime.now(tz=pytz.timezone('Europe/Amsterdam'))
    filename = scriptTime.strftime("%Y-%m-%d-%H-%M-%S")
    filename = '{}/output/{}'.format(cwd, filename)

    """ Retrieve data from vaarweginformatie """
    vaarweginfo = extractor.vaarweginformatie(cache=True, store=True, exportLocation=filename).fetch()

    """ Store it in the GraphDB """
    graphdb.dutchwatergraph('bolt://localhost:7687', truncate=True).createNodes(vaarweginfo.data).close()
    

if __name__ == '__main__':
    cwd = os.path.dirname(os.path.realpath(__file__))
    logging.basicConfig(filename='{}/warnings.log'.format(cwd), level=logging.WARN, format='%(asctime)s;%(levelname)s;%(module)s;%(message)s')
    logging.debug('Script Started')
    main()


