#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Must have functions
"""

import functools

def dotfield(input_dict, input_key):
""" 
Based and inspired on retrieve_dotnotation_field function of Friso van Vollenhoven. 
https://github.com/godatadriven/jiraview/blob/master/python/jiraview/extract.py
"""
    def reducer(d,k):
        if type(d) == dict:
            return d.get(k)  
        elif type(d) == list and len(d) > 0:
            return d[0].get(k)
        else:
            return None
    return functools.reduce(reducer, input_key.split("."), input_dict)