# import csv
import unicodecsv as csv
import argparse
from collections import OrderedDict
import traceback
import time
import os
import shutil
import sys
from operator import add
from pprint import pprint
import unicodecsv as csv
import cStringIO
import multiprocessing

parser = argparse.ArgumentParser(description='Generate Full Contact export files')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')
parser.add_argument('-o','--output',required=True, type=str, help='path of output file')
parser.add_argument('-m','--manual',required=False, type=int, default=0, help='manual input')
parser.add_argument('-c','--encoding',required=False, type=str, default='utf-8', help='set character encoding')
parser.add_argument('-r','--removebackup',required=False, type=int, default=0, help='toggle whether to remove the original file')
parser.add_argument('-i','--ignoreencodingerrors',required=False, type=int, default=0, help='toggle whether to ignore encoding errors and just skip them')
args = parser.parse_args()

reload(sys)
sys.setdefaultencoding('UTF8')

start = time.time()
pool_size = multiprocessing.cpu_count() * 2
pool_limit = pool_size * 2

# parser = dict(
#     delimiter = raw_input('Enter original column delimiter: '),
#     quoting = getattr(csv,raw_input('Enter original quoting: ').upper()),
#     escapechar = raw_input('Enter original escape character: '),
#     doublequote = (True if raw_input('Using quotes to escape quotes?: ') == '1' else False),
#     # encoding = raw_input('Enter new character encoding: '),
#     )

# converter = dict(
#     delimiter = raw_input('Enter new column delimiter: '),
#     quoting = getattr(csv,raw_input('Enter new quoting: ').upper()),
#     escapechar = raw_input('Enter new escape character: '),
#     doublequote = (True if raw_input('Use quotes to escape quotes?: ') == '1' else False),
#     # encoding = raw_input('Enter new character encoding: '),
#     )

parser = dict(
    delimiter = ',',
    quoting = csv.QUOTE_ALL,
    escapechar = '\\',
    doublequote = False,
    # encoding = raw_input('Enter new character encoding: '),
    )

converter = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    escapechar = '\\',
    doublequote = False,
    # encoding = raw_input('Enter new character encoding: '),
    )

for key, value in [(key,value) for key, value in converter.iteritems()]:
    if not value:
        converter.pop(key)

escape_escapes = (converter['escapechar'] and converter['quoting'] in [csv.QUOTE_MINIMAL, csv.QUOTE_ALL, csv.QUOTE_NONNUMERIC])


class FileParser(object):

    bytes_per_chunk = 10 * 1024**2

    def __init__(self, path, bytes_per_chunk=None):
        self.path = path
        if bytes_per_chunk is not None:
            self.bytes_per_chunk = bytes_per_chunk

    def get_size_of(self):
        return os.path.getsize(self.path)

    def yield_chunk(self):
        with open(self.path, 'rb') as ifile:
            while True:
                output = ifile.readlines(self.bytes_per_chunk)
                if not output:
                    break
                yield output

def reparse(iterable, inparser, outconverter):
    io = cStringIO.StringIO()
    icsv = csv.reader(iterable, **inparser)
    ocsv = csv.writer(io, **outconverter)
    if escape_escapes:
        escaped_lines = ([v.replace(converter['escapechar'], converter['escapechar']*2) for v in line] for line in icsv)
    else:
        escaped_lines = (line for line in icsv)
    ocsv.writerows(escaped_lines)
    output = io.getvalue()
    io.close()
    return output

def consume_mp_results(mp_result_list):
    if mp_result_list[0].ready():
        return mp_result_list.pop(0)

if __name__ == '__main__':
    file_parser = FileParser(args.filepath)
    pool = multiprocessing.Pool(pool_size)
    results = list()
    with open(args.output, 'wb') as ofile:
        for i, chunk in enumerate(file_parser.yield_chunk()):
            results.append(pool.apply_async(reparse, [chunk, parser, converter,]))
            if len(results) >= pool_limit:
                result = results.pop(0)
                result.wait()
                ofile.writelines(result.get())

        for result in results:
            result.wait()
            ofile.writelines(result.get())

