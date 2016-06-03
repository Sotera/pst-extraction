#!/usr/bin/env python3

import csv
import optparse
import os
import pprint
import re
import subprocess
import sys


newman_process_destination = "/srv/software/pst-extraction-master/pst-extract"

parser = optparse.OptionParser()
parser.add_option('-f',
                  help="Configuration file to run the Newman Pipeline against.  Must be a CSV file with the fields: filename,newman-filename,case_id,alternate_reference_label,label,language,type (pst, mbox)",
                  default=None,
                  dest="newman_csv_file")
parser.add_option('-d',
                  action="store_true",
                  default=False,
                  dest="debug")
parser.add_option('-v',
                  action="count",
                  default=0,
                  dest="verbosity")
parser.add_option('-t',
                  action="store_true",
                  default=False,
                  dest="test_mode")


options, remainder = parser.parse_args()

parser.parse_args()


if options.debug is True:
    print("Debugging enabled!")
if options.test_mode is True:
    print("Test Mode ENABLED!")

if options.debug is True:
    print("printing options sent to script")
    print(options)


if options.newman_csv_file is None:
    sys.exit("Error:  No input file given, cannot continue")

# Read in the config file as csv
data={}
with open(str(options.newman_csv_file)) as fin:
    reader=csv.reader(fin, skipinitialspace=True, quotechar="'")
    for row in reader:
        data[row[0]]=row[1:]

# we want to make sure we were able to get the information
if options.debug is True:
    pp = pprint.PrettyPrinter(width=41, compact=True)
    pp.pprint(data)
else:
    print("data file read in.  Ready for processing")


for key, value in data.items():
    filename = str(key)
    pipeline_name = str(value[0])
    case_id = str(value[1])
    reference_label = str(value[2])
    label = str(value[3])
    language = str(value[4])
    mbox_type = str(value[5])

    split_filename = filename.split('/')


    if mbox_type is "mbox":
        if filename.find('.mbox') == -1:
            dest_filename = split_filename[-1] + ".mbox"
        else:
            dest_filename = split_filename[-1]
    else:
        dest_filename = split_filename[-1]

    if options.debug is True:
        print("key (filename) is:  " + filename)
        print("\tpipeline name:  " + pipeline_name)
        print("\tcase_id is:  " + case_id)
        print("\treference_label is:  " + reference_label)
        print("\tlabel is:  " + label)
        print("\tlanguage is:  " + language)
        print("\tmailbox type is:  " + mbox_type)
        print("\tdest_filename is:  " + dest_filename)

    # clean up from any possible previous run
    if options.test_mode is False:
        exitcode = subprocess.call(["sudo rm -fr /srv/software/pst-extraction-master/pst-extract/*"], shell=True)
        os.mkdir(newman_process_destination + "/pst")
        os.mkdir(newman_process_destination + "/mbox")

        run_command = pipeline_name + " " + case_id + " " + reference_label + " " + label + " " + language
        if mbox_type == "pst":
            cp_command = ["cp -R " + str(key) + " " + newman_process_destination + "/pst/" + dest_filename]
            exitcode = subprocess.call(cp_command, shell=True)
            exitcode = subprocess.call(["./bin/pst_all.sh " + run_command.lower()], shell=True)
        elif mbox_type == "mbox":
            cp_command = ["cp -R " + str(key) + " " + newman_process_destination + "/mbox/" + dest_filename]
            exitcode = subprocess.call(cp_command, shell=True)
            exitcode = subprocess.call(["./bin/mbox_all.sh " + run_command.lower()], shell=True)
        else:
            sys.exit("Error:  No type given, must provide pst or mbox.  Use -h to see help")

print("done.")