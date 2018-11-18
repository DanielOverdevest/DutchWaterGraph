#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.SYNOPSIS
  Class to retrieve data from vaarweginformatie
  
.DESCRIPTION

.NOTES
  Author: Daniël Overdevest
"""

import types
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import logging
import functools
import os
import csv
import datetime
import pytz
try:
    import cPickle as pickle
except ModuleNotFoundError:
    import pickle

class vaarweginformatie():
    def __init__(self, cache=False, store=False, objectTypes=['isrs', 'fairway', 'bridge', 'lock', 'route', 'operatingtimes'], exportLocation='output/output'):
        self.data = types.SimpleNamespace()
        self.objectTypes = objectTypes
        self.cache = cache
        self.cacheFileName = 'vaarweginfo.pkl'
        self.store = store
        self.exportLocation = exportLocation
        return

    def fetch(self):
        if self.cache:
            return self.load_object(self.cacheFileName)
        for objectType in self.objectTypes:
            result = self.fetchData(objectType)
            self.setData(objectType, result)
            if self.store: self.storeData(result, objectType)
        if self.cache:
            self = self.save_object(self.cacheFileName)
        return self

    def save_object(self, filename):
        with open(filename, 'wb') as output:  # Overwrites any existing file.
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

    def load_object(self, filename):
        with open(filename, 'rb') as input:
            return pickle.load(input)

    def storeData(self, data=None, objectType=""):
        if data == None:
            data = self.data
        if (not data and len(data) == 0):
            return
        exportFile = self.exportLocation + '_' + objectType + '.csv'
        with open(exportFile, 'w+', newline='') as csvfile:
            # Get csv headers / columns
            columns = []
            for row in data:
                columns = list(set(columns + list(row.keys())))
            writer = csv.DictWriter(csvfile, fieldnames=sorted(columns))

            writer.writeheader()
            for row in data:
                writer.writerow(row)        
        return True
        

    def fetchData(self, objectType):    
        geoGenerationUrl = "https://www.vaarweginformatie.nl/wfswms/dataservice/1.3/geogeneration"
        apiUrl = "https://www.vaarweginformatie.nl/wfswms/dataservice/1.3/{geoGeneration}/{objectType}?offset={offset}&count={count}"

        fields = ['commonData.lastModification','commonData.latitude','commonData.longitude','commonData.originator','commonData.isrs','commonData.name','bridgeDetails.bridgeStatus','bridgeDetails.vild','bridgeDetails.note','bridgeDetails.bridgeOpenings.heightClosed','bridgeDetails.bridgeOpenings.heightOpened','bridgeDetails.bridgeOpenings.width','bridgeDetails.bridgeOpenings.number','bridgeDetails.bridgeOpenings.referenceLevel']

        # Settings for retry and auto retry if error code 500 is given
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504),
        )

        session = requests.Session()
        session.mount(geoGenerationUrl, HTTPAdapter(max_retries=retry))

        try:
            getGeoGeneration = session.get(geoGenerationUrl, timeout=5).json()
        except Exception as err:
            logging.exception('Fails: {}'.format(err.__class__.__name__))

        try:
            returnList = []
            offset, count = 0, 100
            for i in range(0,1000):
                url = apiUrl.format(geoGeneration=getGeoGeneration.get("GeoGeneration"), objectType=objectType, offset=offset, count=count)
                r = session.get(url, timeout=5)
                r.raise_for_status()
                vaarwegInfo = r.json()
                returnList = [*returnList, *vaarwegInfo.get("Result")]
                if vaarwegInfo.get("TotalCount") <= (offset + count):
                    break
                offset = offset + count

            # for bridge in getBridgeList.get('commonData', {}):
            #     bridgeUrl = urlBridgeStatusFormat.format(originator=bridge.get('originator', ''), isrs=bridge.get('isrs',''))
            #     bridgeDetail = session.get(bridgeUrl, timeout=5).json()
            #     row = {}
            #     for field in fields:
            #         fieldName = field.split('.')[-1]
            #         row.update({fieldName: dotfield(bridgeDetail, field)})
            #     row.update({'ImportDateTime': datetime.datetime.now(tz=pytz.timezone('Europe/Amsterdam'))})
                # returnList.append(row)
            self.data
            return returnList
        except Exception as err:
            logging.exception('Fails: {}'.format(err.__class__.__name__))
    
    def setData(self, name, value):
        setattr(self.data, name, value)