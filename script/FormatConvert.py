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
parser.add_argument('-l','--delimiter',required=False, type=str, default=',', help='file delimiter')
parser.add_argument('-q','--quoting',required=False, type=str, default='QUOTE_MINIMAL', help='set quoting option')
parser.add_argument('-e','--escapechar',required=False, type=str, default=None, help='set escape character')
parser.add_argument('-b','--doublequote',required=False, type=int, default=1, help='set escaping of quote character to the quote character itself')
parser.add_argument('-c','--encoding',required=False, type=str, default='utf-8', help='set character encoding')
parser.add_argument('-r','--removebackup',required=False, type=int, default=0, help='toggle whether to remove the original file')
parser.add_argument('-i','--ignoreencodingerrors',required=False, type=int, default=0, help='toggle whether to ignore encoding errors and just skip them')
args = parser.parse_args()

reload(sys)
sys.setdefaultencoding('UTF8')

start = time.time()

delimiters = [args.delimiter] + [
    ',',
    '|',
    '\t',
    ]

converter = dict(
    bak = dict(
        filepath = '.'.join([
            args.filepath,
            'bak',
            ]),
        ),
    old = dict(
        filepath = args.filepath,
        delimiter = args.delimiter,
        quoting = args.quoting.upper(),
        escapechar = args.escapechar,
        doublequote = (True if args.doublequote else False),
        encoding = args.encoding,
        ),
    new = dict(
        filepath = '.'.join([
            args.filepath,
            'new',
            ]),
        delimiter = raw_input('Enter new column delimiter: '),
        quoting = raw_input('Enter new quoting: ').upper(),
        escapechar = raw_input('Enter new escape character: '),
        doublequote = (True if raw_input('Use quotes to escape quotes?: ') == '1' else False),
        encoding = raw_input('Enter new character encoding: '),
        ),
    )

for key, value in converter['new'].iteritems():
    if not value:
        converter['new'][key] = converter['old'][key]

exec("converter['old']['quoting_csv'] = csv.{quoting}".format(quoting=converter['old']['quoting']))
exec("converter['new']['quoting_csv'] = csv.{quoting}".format(quoting=converter['new']['quoting']))


def shuffle_files(**kwargs):
    delete = kwargs.get('delete',False)

    shutil.move(converter['old']['filepath'],converter['bak']['filepath'])
    shutil.move(converter['new']['filepath'],converter['old']['filepath'])

    if check_file(converter['old']['filepath']) and delete and converter['old']['filepath'] != converter['bak']['filepath']:
        os.remove(converter['bak']['filepath'])

    return


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


def check_for_changes(converter_dict):
    keys_to_check = [
        'delimiter',
        'quoting',
        'escapechar',
        ]

    for key in keys_to_check:
        if converter_dict['old'][key] != converter_dict['new'][key]:
            return True

    return False


def test_delimiters(file_path,delimiters):

    # return ','

    lines_to_test = 10

    for delimiter in delimiters:
        with open(file_path,'rb') as ifile:
            icsv = csv.reader(ifile,delimiter=delimiter,quoting=converter['old']['quoting_csv'],doublequote=converter['old']['doublequote'],encoding=converter['old']['encoding'])
            # icsv = csv.reader(ifile,delimiter=delimiter)
            matched = True
            for i, line in enumerate(icsv):
                if i == 0:
                    column_count = len(line)
                    if column_count < 2:
                        matched = False
                        break
                if len(line) != column_count:
                    matched = False
                    break
                if i + 1 == lines_to_test:
                    matched = True
                    break

            if matched:
                return delimiter

    return False



def main(converter):

    try:
        if check_file(converter['old']['filepath']):
            determined_delimiter = test_delimiters(converter['old']['filepath'],delimiters)
            if determined_delimiter == converter['old']['delimiter']:
                with open(converter['old']['filepath'],'rb') as ifile, open(converter['new']['filepath'],'wb') as ofile:
                    icsv = csv.reader((l.decode(converter['old']['encoding']) for l in ifile),delimiter=converter['old']['delimiter'],quoting=converter['old']['quoting_csv'],escapechar=converter['old']['escapechar'],doublequote=converter['old']['doublequote'],encoding=converter['old']['encoding'])

                    ocsv = csv.writer(ofile,delimiter=converter['new']['delimiter'],quoting=converter['new']['quoting_csv'],escapechar=converter['new']['escapechar'],doublequote=converter['new']['doublequote'],encoding=converter['new']['encoding'])

                    for i, line in enumerate(icsv):
                        if converter['new']['quoting_csv'] in [csv.QUOTE_MINIMAL,csv.QUOTE_ALL,csv.QUOTE_NONNUMERIC]:
                            line = replace_iterate(line,[(converter['new']['escapechar'], converter['new']['escapechar']*2),])
                        ocsv.writerow([l.encode(converter['new']['encoding'],errors=('ignore' if args.ignoreencodingerrors else 'strict')) for l in line])

            elif determined_delimiter:
                print 'Error - original delimiter determined to be {d}'.format(d=repr(determined_delimiter),i=repr(converter['old']['delimiter']))
                sys.exit(1)

            else:
                print 'Error - could not determine the input delimiter'
                sys.exit(1)

        if check_file(converter['new']['filepath']):
            shuffle_files(delete=(True if args.removebackup else False))

    except:
        if check_file(converter['new']['filepath']):
            os.remove(converter['new']['filepath'])

        traceback.print_exc()
        sys.exit(1)

    print '\r\nREFORMATTED {lines} lines in {filepath}'.format(lines=i+1,filepath=converter['old']['filepath'])
    format_changes = ['{k}: {o} to {n}'.format(k=key,o=value,n=converter['new'][key]) for key, value in converter['old'].iteritems() if value != converter['new'][key] and key not in ['quoting_csv','filepath']]
    print '\r\n'.join(format_changes)


if __name__ == '__main__':

    if check_for_changes(converter):
        main(converter)
        # print converter
    else:
        print 'No Changes Issued...'

    runtime = int(time.time()-start)