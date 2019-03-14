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

converter = dict(
    delimiter = raw_input('Enter new column delimiter: '),
    quoting = getattr(csv,raw_input('Enter new quoting: ').upper()),
    escapechar = raw_input('Enter new escape character: '),
    doublequote = (True if raw_input('Use quotes to escape quotes?: ') == '1' else False),
    # encoding = raw_input('Enter new character encoding: '),
    )

print converter

# for key, value in [(key,value) for key, value in converter.iteritems()]:
#     if not value:
#         converter.pop(key)


def get_dialect(file_path,**kwargs):

    sample = kwargs.get('sample',None)
    dialect_config = kwargs.get('dialect_config',dict())

    if not sample:
        with open(file_path) as ifile:
            sample = ifile.read(1000000)

    if args.manual:
        csv.register_dialect('input_dialect',**dict(
            delimiter = raw_input('Enter column delimiter: ') or ',',
            quoting = getattr(csv,raw_input('Enter quoting: ').upper()) or csv.QUOTE_MINIMAL,
            escapechar = raw_input('Enter escape character: ') or '\\',
            doublequote = (True if raw_input('Using quotes to escape quotes?: ') == '1' else False) or False,
            # encoding = raw_input('Enter character encoding: ') or 'utf-8',
            ))
        return dict(
            dialect = csv.get_dialect('input_dialect'),
            has_header = dialect_config.get('has_header',True),
            )

    else:
        if sample:
            return dict(
                dialect = csv.Sniffer().sniff(sample),
                has_header = dialect_config.get('has_header',True),
                )

        else:
            with open(file_path) as ifile:
                sample = ifile.read(1000000)
            return dict(
                dialect = csv.Sniffer().sniff(sample),
                has_header = csv.Sniffer().has_header(sample),
                )



def check_file(file_path):

    if os.path.exists(file_path):
        if os.path.isfile(file_path):
            if int(os.stat(file_path).st_size) > 100:
                return True

    return False


def replace_iterate(value,replace_list):

    if not replace_list:
        return value

    if type(value) in (str,unicode):
        output = value[:]
        for to_replace, replace_with in replace_list:
            output = output.replace(to_replace,replace_with)
                
        return output

    elif type(value) in (list,tuple):
        output = list(value)
        output = [replace_iterate(v,replace_list) for v in output]

    else:
        output = value

    return output



def main(dialect_config,converter):

    converter['dialect'] = dialect_config['dialect']
    converter['escapechar'] = converter.get('escapechar',(getattr(converter['dialect'],'escapechar',None) or '\\'))

    with open(args.filepath,'rb') as ifile, open(args.output,'wb') as ofile:
        icsv = csv.reader((l.decode(args.encoding) for l in ifile),dialect=dialect_config['dialect'],encoding=args.encoding)

        ocsv = csv.writer(ofile,**converter)

        if dialect_config['has_header']:
            ocsv.writerow(icsv.next())

        for i, line in enumerate(icsv):
            try:
                if converter['quoting'] in [csv.QUOTE_MINIMAL,csv.QUOTE_ALL,csv.QUOTE_NONNUMERIC] and converter.get('escapechar',dialect_config['dialect'].escapechar):

                    line = replace_iterate(line,[(converter.get('escapechar',dialect_config['dialect'].escapechar), converter.get('escapechar',dialect_config['dialect'].escapechar)*2),])
                ocsv.writerow([l.encode(converter.get('encoding',args.encoding), errors=('ignore' if args.ignoreencodingerrors else 'strict')) for l in line])

            except:
                if check_file(args.output):
                    os.remove(args.output)
                traceback.print_exc()
                return line



if __name__ == '__main__':

    dialect_config = get_dialect(args.filepath)
    error_count = 0

    while True:

        sample = main(dialect_config,converter)

        if error_count >= 10:
            print 'ERRORED OUT'
            break

        if sample:
            error_count += 1
            dialect_config = get_dialect(args.filepath,sample=sample,dialect_config=dialect_config)
            continue

        break


    attr_to_print = [
        'delimiter',
        'quoting',
        'escapechar',
        'doublequote',
        'encoding',
        ]
    
    for attr in attr_to_print:
        print '{a}: {v}'.format(
            a = attr,
            v = converter.get(attr,getattr(dialect_config['dialect'],attr,None)),
            )

    runtime = int(time.time()-start)