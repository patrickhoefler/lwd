#!/usr/bin/python
# -*- coding: utf-8 -*-

# Run this script to start the conversion process of
# turning the Wikidata dump into Linked Data

import glob
import os

import lwd
import settings



# Process the Wikidata XML dump
lwd.process_dump()

# Create gzipped files
if settings.create_gzipped_files:
	lwd.compress_ttl_files()
