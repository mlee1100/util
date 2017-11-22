import fileinput
import sys
from glob import glob
import os
import traceback
import argparse

parser = argparse.ArgumentParser(description='Generate Full Contact export files')
parser.add_argument('input_files', type=str, nargs='+', help='path to input files (supports wildcards)')
parser.add_argument('-o','--output_file', required=True, type=str, help='output file path')
args = parser.parse_args()

all_files = [f for files in args.input_files for f in glob(files) if os.path.isfile(f) and os.path.abspath(f) != os.path.abspath(args.output_file)]

with open(args.output_file,'wb') as ofile:
    for i, file in enumerate(all_files):
        try:
            if os.stat(file).st_size > 0:
                with open(file,'rb') as ifile:
                    if i != 0:
                        _ = ifile.next()
                    for line in ifile:
                        ofile.write(line)
        except:
            print file
            raise