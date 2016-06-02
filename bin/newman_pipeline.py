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
                  help="Configuration file to run the Newman Pipeline against.  Must be a CSV file with the fields: filename,newman-filename,case_id,alternate_reference_label,label,language",
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


pst_match = re.compile(".pst")
mbox_match = re.compile(".mbox")


for key, value in data.items():
    if options.debug is True:
        pst_match.match(str(key))
        mbox_match.match(str(key))

        print("key (filename) is:  " + str(key))
        print("\tpipeline name:  " + str(value[0]))
        print("\tcase_id is:  " + str(value[1]))
        print("\talternate_reference_label is:  " + str(value[2]))
        print("\tlabel is:  " + str(value[3]))
        print("\tlanguage is:  " + str(value[4]))
        if pst_match.search(str(key)):
            print("\tmailbox type is identified as:  pst")
        if mbox_match.search(str(key)):
            print("\tmailbox type is identified as:  mbox")

    # clean up from any possible previous run
    if options.test_mode is False:
        exitcode = subprocess.call(["sudo rm -fr /srv/software/pst-extraction-master/pst-extract/*"], shell=True)
        os.mkdir(newman_process_destination + "/pst")
        os.mkdir(newman_process_destination + "/mbox")

        run_command = str(value[0]) + " " + str(value[1]) + " " + str(value[2]) + " " + str(value[3]) + " " + str(value[4])
        if pst_match.search(str(key)):
            cp_command = ["cp -R " + str(key) + " " + newman_process_destination + "/pst/"]
            exitcode = subprocess.call(cp_command, shell=True)
            exitcode = subprocess.call(["./bin/pst_all.sh " + run_command.lower()], shell=True)
        if mbox_match.search(str(key)):
            cp_command = ["cp -R " + str(key) + " " + newman_process_destination + "/mbox/"]
            exitcode = subprocess.call(cp_command, shell=True)
            exitcode = subprocess.call(["./bin/mbox_all.sh " + run_command.lower()], shell=True)

print("done.")