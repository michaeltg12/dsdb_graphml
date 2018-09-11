#!/apps/base/python3/bin/python3

import argparse
import os, shutil
import subprocess
from glob import glob
import csv
import re

dqr_regex = re.compile("D\d{6}(\.)*(\d)*")
datastream_regex = re.compile("(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
date_regex = re.compile("[1,2]\d{7}")

help_description = '''
This program will automate testing the variable mapping from raw to cdf/nc files.
It must be run from the directory where the input raw files are.
'''

example = '''
EXAMPLE: TODO
'''

def parse_args():
    #Information needed
    #DQR # --> get from path
    #cleanup old ncreview files
    #assume bash
    #location of raw input data
    #    assume 1 input data
    #location of modification scripts
    #ingest and command
    #clean up 
    #    datastream direcotory
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-m', '--modify', dest='modify', type=int, help='Column to modify and test.', required=True)
    parser.add_argument('--delimiter', dest='delimiter', default=',', help='File delimiter.')
    parser.add_argument('--header', dest='header', type=int, default=0, help='Number of header lines.')
    parser.add_argument('--skip-col', dest='skip_column', nargs='+', type=int, default=[],
                        help='Columns to skip. Must be last argument.')
    parser.add_argument('-I', '--Interactive', action='store_true', dest='interactive', default=False,
                        help='Interactive / partial execution')
    
    args = parser.parse_args()
    if args.modify in args.skip_column:
        print('Skipping modification column, no effect.')
        exit(0)

    return args

def main():
    args = parse_args()
    # try and get arguments from path
    cwd = os.getcwd()
    dqr = dqr_regex.search(cwd).group()
    datastream = datastream_regex.search(cwd).group()
    site = datastream[:3]

    # ask if arguments are correct?
    question = "DQR # = {}\nRaw datastream = {}\nIs this correct? ".format(dqr, datastream)
    if input(question) in ['y', 'yes', 'yea', 'ok']:
        print("\tProceding with test:")
    # else ask for arguments
    else:
        dqr = input("Enter the DQR #:\nExample D180042.4: ")
        datastream = input("Enter the datastream:\nExample sgp30ebbrC1.00: ")
        site = datastream[:3]

    # source environment variables
    reproc_home = os.getenv("REPROC_HOME") # expected to be the current reproc environment
    post_processing = os.getenv("POST_PROC") # is a post processing folder under reproc home
    data_home = f"{reproc_home}/{dqr}" # used to set environment variables for current dqr job 

    # try sourcing by apm created environment file in case it has more than the default
    env_file = os.path.join(reproc_home, dqr, 'env.bash')
    if os.path.isfile(env_file):
        with open(env_file, 'r') as open_env_file:
            lines = open_env_file.readlines()
            for l in lines:
                key, value = l.split("=")
                value = value[1:-1].replace("DATA_HOME", data_home)
                print("{}={}".format(key, value))
                os.environ[key] = value
    else:
        # set environment variables based on this default
        env_vars = {"DATA_HOME" : data_home,
        "DATASTREAM_DATA" : f"{data_home}/datastream",
        "ARCHIVE_DATA" : "/data/archive",
        "OUT_DATA" : f"{data_home}/out",
        "TMP_DATA" : f"{data_home}/tmp",
        "HEALTH_DATA" : f"{data_home}/health",
        "QUICKLOOK_DATA" : f"{data_home}/quicklooks",
        "COLLECTION_DATA" : f"{data_home}/collection",
        "CONF_DATA" : f"{data_home}/conf",
        "LOGS_DATA" : f"{data_home}/logs",
        "WWW_DATA" : f"{data_home}/www",
        "DB_DATA" : f"{data_home}/db"}

        print("Environment file does not exist at:\n\t{}".format(env_file))
        print("Sourcing from default dict:")
        for key, value in env_vars.items():
            print("\t{}={}".format(key, value))
            os.environ[key] = value

    # get files for modification
    file_search = os.path.join(cwd, '*')
    files = glob(file_search)
    # get date from files to look for ingest command and for cdf comparison later
    for f in files:
        result_date = date_regex.search(f.split('/')[-1])
        if result_date:
            result_date = result_date.group()
            break

    # get ingest command
    ingest_search = os.path.join("/data/archive/", site, datastream[:-3]+"*")
    print("Searching for output directories:\n\t{}".format(ingest_search))
    cmd = 'ls -d {}'.format(ingest_search)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out, err = proc.communicate()
    out_2string_striped = str(out)[2:-3]
    out_split = out_2string_striped.split('\\n')
    print("Found the following directories:\n\t{}".format(out_split))
    for element in out_split:
        if element[-2:] != '00':
            search_dir = os.path.join("/data/archive", site, element, "*"+result_date+"*")
            print('Searching for netcdf file:\n\t{}'.format(search_dir))
            ingested_file = glob(search_dir)[0]
            cmd = "ncdump -h {} | grep command".format(ingested_file)
            print(cmd)
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            out, err = proc.communicate()
            ingest_command = str(out).split('"')[1]
            if ingest_command:
                break


    # copy input files to backup directory
    autotest_dir = os.path.join(cwd, ".autotest")
    for f in files:
        try:
            shutil.copyfile(os.path.join(cwd, f), os.path.join(autotest_dir, f))
        except shutil.SameFileError:
            break

    ### TODO This is where the loop for each column will occur ###
    
    # cleanup post processing of old ncreview files
    rm_path = os.path.join(post_processing, dqr, 'ncr*')
    cmd = ['rm', '-v', rm_path]
    proc = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    print("Remove files:")
    for line in proc.stdout:
        print("\t{}".format(line))

    # run modification procedure
    # set header rows
    if args.header:
        rows_to_skip = [x for x in range(args.header)]
    else:
        rows_to_skip = []
    # set columns to modify or default
    columns_to_skip = args.skip_column

    for i, input_file in enumerate(files):
        output_list = []
        with open(input_file) as open_input_file:
            csv_reader = csv.reader(open_input_file, delimiter=args.delimiter)
            for j, line in enumerate(csv_reader):
                if j not in rows_to_skip:
                    for column_number in range(len(line)):
                        line[args.modify] = eval(line[args.modify]) + 1000
                        output_list.append(line)
                    else:
                        output_list.append(line)
                else: output_list.append(line)
        output_file = input_file.split("/")[-1]
        with open(output_file, 'w') as open_output_file:
            csv_writer = csv.writer(open_output_file)
            csv_writer.writerows(output_list)

    # run ingest
    print(ingest_command)

    # setup for ncreview *** TODO setup for cdf comparison ***

    # run ncreveiw *** TODO evaluate cdf comparison ***

    # print contents of log file to console *** TODO add results to json file ***

    # cleanup datastream direcotry 

    # restage raw files from backup directory

    # repeat

if __name__ == "__main__":
    main()
