import random
import unicodecsv as csv
import math
# import csv
import sys
import argparse
import time
from psutil import virtual_memory
import itertools
import string
import datetime
import os
import concurrent.futures
from multiprocessing import cpu_count
reload(sys)
sys.setdefaultencoding('UTF8')

parser = argparse.ArgumentParser(description='Generate Full Contact export files')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')
parser.add_argument('-v','--values',required=True, type=str, help='column/s to dedup on')
parser.add_argument('-r','--has_header',required=False, type=int, help='file has header')
parser.add_argument('-x','--maxmem', required=False, type=int, default=95, help='percent system mem usage to pause at')
parser.add_argument('-d','--distribution', required=False, type=int, default=1, help='number of files to distribute to')
parser.add_argument('-l','--delimiter', required=False, type=str, default=',', help='field delimiter')
parser.add_argument('-u','--quoting', required=False, type=str, default='QUOTE_MINIMAL', help='field delimiter')
parser.add_argument('-e','--escapechar', required=False, type=str, default='\\', help='escape character')
parser.add_argument('-b','--doublequote',required=False, type=int, default=0, help='set escaping of quote character to the quote character itself')
parser.add_argument('-c','--encoding',required=False, type=str, default='utf-8', help='set escaping of quote character to the quote character itself')
args = parser.parse_args()

start = time.time()

filename, fileext = os.path.splitext(args.filepath)
args.outfile = ''.join([filename,'.dedup',fileext])
exec('args.quoting = csv.{quoting}'.format(quoting=args.quoting))
use_double_quote = (False if args.escapechar and not (True if args.doublequote else False) else True)

args.values = args.values.split(',')
memory_check_interval = 100
memory_check_counter = 0
system_memory = virtual_memory().total * 0.8
# system_memory = 10000

csv_settings = dict(
    delimiter=args.delimiter,
    quoting=args.quoting,
    escapechar=args.escapechar,
    doublequote=use_double_quote
    )

characters_set = set()

def get_letter_size_in_file(file_path,csv_settings,indexes,num_letters,**kwargs):
    starting_letters = kwargs.get('starting_letters','')
    letter_dict = dict()
    with open(args.filepath,'rb') as ifile:
        icsv = csv.reader(ifile,**csv_settings)
        if args.has_header:
            _ = icsv.next()
        for line in icsv:
            if line[indexes[0]][:num_letters].startswith(starting_letters):
                letter_dict[line[indexes[0]][:num_letters]] = letter_dict.get(line[indexes[0]][:num_letters],0) + sys.getsizeof(tuple([line[i] for i in indexes]))

    return letter_dict


def get_dedup_indexes(file_path,values,csv_settings):
    if args.has_header:
        with open(file_path,'rb') as ifile:
            icsv = csv.reader(ifile,**csv_settings)
            header = icsv.next()
            dedup_indexes = [header.index(c) for c in header if c in values]
    else:
        dedup_indexes = [int(v) for v in values]

    return dedup_indexes


def combine_letters(letter_dict,max_size):

    output_dict = dict()

    total_space = 0
    file_num = 0
    letter_list = list()
    file = 'dedupfile.{i}.txt'.format(i=file_num)
    for letter, space in letter_dict.iteritems():
        if total_space + space >= max_size:
            output_dict[file] = set(output_dict[file])
            file_num += 1
            total_space = 0
            file = file = 'dedupfile.{i}.txt'.format(i=file_num)

        total_space += space
        output_dict[file] = output_dict.get(file,list()) + [letter,]

    output_dict[file] = set(output_dict[file])

    return output_dict



def check_memory(max_memory):

    if virtual_memory().percent > max_memory:
        print 'Killed due to excessive memory consumption'
        sys.exit(1)

    return


dedup_indexes = get_dedup_indexes(args.filepath,args.values,csv_settings)

letter_size_dict = dict()

letter_size_dict = dict(letter_size_dict,**get_letter_size_in_file(args.filepath,csv_settings,dedup_indexes,1))
# print letter_size_dict
distribute = [k for k, v in letter_size_dict.iteritems() if v >= system_memory]
while distribute:
    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        mp_results = list()
        for letters in distribute:
            _ = letter_size_dict.pop(letters)

            mp_results.append(executor.submit(get_letter_size_in_file,args.filepath,csv_settings,dedup_indexes,len(letters)+1,starting_letters=letters))

            for result in concurrent.futures.as_completed(mp_results):
                letter_size_dict = dict(letter_size_dict,**result.result())


        distribute = [k for k, v in letter_size_dict.iteritems() if v >= system_memory]

char_sets = combine_letters(letter_size_dict,system_memory)
# print char_sets
# sys.exit(0)

record_file_path_in = args.filepath

for staging_file, starting_characters in char_sets.iteritems():
    dedup_set = set()
    record_file_path_out = args.filepath + '.' + datetime.datetime.strftime(datetime.datetime.today(),'%Y%m%d%H%M%S%f')

    with open(staging_file,'wb') as ofile, open(record_file_path_out,'wb') as bfile, open(record_file_path_in,'rb') as ifile:
        icsv = csv.reader(ifile,**csv_settings)
        ocsv = csv.writer(ofile,**csv_settings)
        bcsv = csv.writer(bfile,**csv_settings)
        for line in icsv:
            # print line[dedup_indexes[0]]
            # print starting_characters
            if [t for t in starting_characters if line[dedup_indexes[0]].startswith(t)]:
                current_value = tuple([line[i].strip().lower() for i in dedup_indexes])
                if current_value not in dedup_set:
                    memory_check_counter += 1

                    if memory_check_counter == memory_check_interval:
                        memory_check_counter = 0
                        check_memory(args.maxmem)
                        
                    dedup_set.update([current_value,])
                    ocsv.writerow(line)

                else:
                    # print current_value
                    pass

            else:
                bcsv.writerow(line)

    record_file_path_in = str(record_file_path_out)


with open(args.outfile,'wb') as ofile:
    if args.has_header:
        ocsv = csv.writer(ofile,**csv_settings)
        ocsv.writerow(header)
    for filename in char_sets:
        with open(filename,'rb') as ifile:
            for line in ifile:
                ofile.write(line)

        os.remove(filename)


print 'finished in {seconds}s'.format(seconds=int(time.time()-start))

