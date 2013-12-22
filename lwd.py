#!/usr/bin/python
# -*- coding: utf-8 -*-

# Functions for turning the Wikidata dump into Linked Data

import codecs
import glob
import gzip
import json
import math
import os
import sys
import time
import xml.etree.cElementTree as ET

import settings



def process_dump():
    # Print some status info
    print 'Processing ' + settings.dump_filename

    # Make sure the output folders exist
    if not os.path.exists('output'):
        os.mkdir('output')
    if not os.path.exists('output/' + settings.output_folder):
        os.mkdir('output/' + settings.output_folder)
    if not os.path.exists('output/' + settings.output_folder + '/ttl'):
        os.mkdir('output/' + settings.output_folder + '/ttl')

    # Delete all old files
    for f in glob.glob('output/' + settings.output_folder + '/ttl/*.ttl'):
        os.remove(f)

    # Initiate variables
    entity_counter = 0
    element_id = ''

    # Start the clock
    start_time = time.time()

    # Load the dump file and create the iterator
    context = ET.iterparse(settings.dump_filename, events=('start', 'end'))
    context = iter(context)
    event, root = context.next()

    # Iterate over the dump file
    for event, element in context:

        # Check if we have reached the max number of processed entities
        if settings.max_processed_entities > 0 and entity_counter == settings.max_processed_entities:
            break

        # Get the ID of the current entity
        if event == 'end' and element.tag == '{http://www.mediawiki.org/xml/export-0.8/}title':
            if element.text.find('Q') == 0:
               element_id = element.text
            elif element.text.find('Property:P') == 0:
                element_id = element.text.split(':')[1]

        # Get the data of the current entity
        if element_id and event == 'end' and element.tag == '{http://www.mediawiki.org/xml/export-0.8/}text':
            if element.text:
                triples = get_nt_for_entity(element_id, element.text)
                batch_id = str(int(math.floor(int(element_id[1:]) / settings.batchsize)) * settings.batchsize).zfill(8)
                batchfile_ttl_name = 'output/' + settings.output_folder + '/ttl/' + element_id[0] + '_Batch_' + batch_id + '.ttl'

                # If ttl file doesn't exist, create it and add the prefixes
                if not os.path.isfile(batchfile_ttl_name):
                    prefixes = '# Extracted from ' + settings.dump_filename + ' with LWD (http://github.com/patrickhoefler/lwd)'
                    prefixes += """

                    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                    @prefix wd: <http://www.wikidata.org/entity/> .

                    """.replace('  ', '')
                    with codecs.open(batchfile_ttl_name, 'a', 'utf-8') as batchfile_ttl:
                        batchfile_ttl.write(prefixes)

                # Write the triples to the batchfile
                with codecs.open(batchfile_ttl_name, 'a', 'utf-8') as batchfile_ttl:
                    batchfile_ttl.write(triples)

                # One more entity
                entity_counter += 1
                
                # Print some progress
                if entity_counter % 1000 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()

                # Print some statistics
                if entity_counter % 10000 == 0:
                    lap_time = time.time()
                    print '\nProcessed ' + str(entity_counter) + ' entities in ' + str(lap_time - start_time) + ' seconds, on average ' + str(entity_counter / (lap_time - start_time)) + ' per second'

            # Reset the element ID in preparation for the next iteration
            element_id = ''
        
        # Save the memory, save the world
        root.clear()

    # Stop the clock and print some final statistics
    end_time = time.time()
    print('\nProcessed ' + str(entity_counter) + ' entities in ' + str(end_time - start_time) + ' seconds, on average ' + str(entity_counter / (end_time - start_time)) + ' per second')
    number_of_files = len(os.listdir('output/' + settings.output_folder + '/ttl'))
    if number_of_files != 1:
        plural = 's'
    else:
        plural = ''
    print('Created ' + str(number_of_files) + ' .ttl file' + plural + ' in ./' + 'output/' + settings.output_folder + '/ttl')


def get_nt_for_entity(element_id, element_data):
    # Turn the data JSON string into an object
    data = json.loads(element_data)

    entity_uri = 'wd:' + element_id
    triples = ''

    # Get the label in English
    try:
        triples = triples + entity_uri + ' rdfs:label ' + '"' + data['label']['en'].replace('\\', '\\\\').replace('"', '\\"') + '"@en .\n'
    except:
        # print 'No label for ' + element_id
        pass

    # Get the description in English
    try:
        triples = triples + entity_uri + ' rdfs:comment ' + '"' + data['description']['en'].replace('\\', '\\\\').replace('"', '\\"') + '"@en .\n'
    except:
        # print 'No description for ' + element_id
        pass

    # Are there any claims in the current element?
    if data.get('claims'):

        # Iterate over all claims
        for claim in data['claims']:

            predicate_id = 'P' + str(claim['m'][1])
            predicate_uri = 'wd:' + predicate_id

            if len(claim['m']) > 2:

                # Is it an object property?
                if claim['m'][2] == 'wikibase-entityid':
                    object_id = 'Q' + str(claim['m'][3]['numeric-id'])
                    object_uri = 'wd:' + object_id

                    triples = triples + entity_uri + ' ' + predicate_uri + ' ' + object_uri + ' .\n'

                    # Add RDF type
                    if predicate_id == 'P31':
                        triples = triples + entity_uri + ' rdf:type ' + object_uri + ' .\n'

                # Is it a string value property?
                if claim['m'][2] == 'string':
                    triples = triples + entity_uri + ' ' + predicate_uri + ' "' + claim['m'][3].replace('\\', '\\\\').replace('"', '\\"') + '" .\n'

    return triples


def compress_ttl_files():
    # Print some status info
    print 'Compressing'

    # Make sure the output folders exist
    if not os.path.exists('output'):
        os.mkdir('output')
    if not os.path.exists('output/' + settings.output_folder):
        os.mkdir('output/' + settings.output_folder)
    if not os.path.exists('output/' + settings.output_folder + '/gz'):
        os.mkdir('output/' + settings.output_folder + '/gz')

    # Delete all old files
    for f in glob.glob('output/' + settings.output_folder + '/gz/*.gz'):
        os.remove(f)

    # Compress all files
    for input_file_name in glob.glob('output/' + settings.output_folder + '/ttl/*.ttl'):
        with open(input_file_name, 'rb') as input_file:
            with gzip.open('output/' + settings.output_folder + '/gz/' + input_file_name.split('/')[-1] + '.gz', 'wb') as output_file:
                output_file.writelines(input_file)

        # Print some progress
        sys.stdout.write('.')
        sys.stdout.flush()

    # Print some final statistics
    number_of_files = len(os.listdir('output/' + settings.output_folder + '/gz'))
    if number_of_files != 1:
        plural = 's'
    else:
        plural = ''
    print('\nCreated ' + str(number_of_files) + ' .gz file' + plural + ' in ./' + 'output/' + settings.output_folder + '/gz')
