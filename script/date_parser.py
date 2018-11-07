import datetime
import re
'''
import date_parser
reload(date_parser)
dp = date_parser.DateParser()
dp.parse('2014', partial=True)
d = dp.parse('2014', partial=True)
d.str_format
d.to_db()
d._build_db_format()
d._set_formats_used()

'''

'''
import csv
import date_parser
from tqdm import tqdm
import os

reload(date_parser)
f = '/home/ec2-user/temp/intelldata/400M_2018_09_24.psv'
o = '/home/ec2-user/temp/intelldata/400M_2018_09_24.badopt.psv'
og = '/home/ec2-user/temp/intelldata/400M_2018_09_24.goodopt.psv'
settings = dict(
    delimiter   = '|',
    quoting = csv.QUOTE_NONE,
    doublequote = False,
    )
dp = date_parser.DateParser()
tell = 0

counter = 0
with tqdm(total=os.path.getsize(f)) as t:
    with open(f, 'rb') as ifile, open(o, 'wb') as ofile, open(og, 'wb') as ogfile:
        icsv = csv.DictReader(ifile, **settings)
        for line in icsv:
            counter += 1
            if counter == 10000:
                counter = 0
                t.update(ifile.tell()-tell)
                tell = ifile.tell()
                db_opt = dp.to_db_format(line['optdate'])
                if not db_opt:
                    ofile.write(line['optdate'] + '\n')
                else:
                    ogfile.write(db_opt + '\n')



'''
class DateParser(object):

    default_formats = [
        '%Y-%m-%d',
        '%Y%m%d',
        '%Y%m%d%H%M%S',
        '%Y%m%d%I%M%S%p',
        '%Y%m%d %H%M%S',
        '%Y%m%d %I%M%S%p',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %I:%M:%S%p',
        '%Y%m%d%H%M',
        '%Y%m',
        '%Y-%m',
        '%Y',
    ]

    def _get_db_format(f):
        sformat = '%Y-%m-%d %H:%M:%S'
        if '%Y' not in f:
            sformat = sformat.replace('%Y', '0'*4)
        if '%m' not in f:
            sformat = sformat.replace('%m', '0'*2)
        if '%d' not in f:
            sformat = sformat.replace('%d', '0'*2)
        if '%H' not in f and '%I' not in f:
            sformat = sformat.replace('%H', '0'*2)
        if '%M' not in f:
            sformat = sformat.replace('%M', '0'*2)
        if '%S' not in f:
            sformat = sformat.replace('%S', '0'*2)
        return sformat



    db_format = '%Y-%m-%d %H:%M:%S'

    def __init__(self, format_list=None):
        self._build_format_conversions(format_list)

    def _parse(self, date_string):
        dt = None
        for i, try_format in enumerate(self.formats):
            for pieces in try_format:
                try:
                    dt = DateTime.strptime(date_string, pieces)
                    dt.str_format = pieces
                except ValueError:
                    continue
                else:
                    self._prioritize_formats(i)
                    break
            if dt:
                break
        return dt

    def _prioritize_formats(self, index):
        self.formats.insert(0, self.formats.pop(index))

    def _build_format_conversions(self, formats):
        if formats is None:
            formats = self.default_formats
        self.to_formats = dict()
        self.formats = list()
        for f in formats:
            self.to_formats[f] = _get_db_format(f)
            partial_formats = list(set([f.replace('%m', '00'), f.replace('%d', '00'), f.replace('%m', '00').replace('%d', '00')]))
            for p in partial_formats:
                self.to_formats[p] = _get_db_format(p)
            self.formats.append(tuple([f,] + partial_formats))


    def to_db_format(self, date_string):
        dt = self._parse(date_string)
        if dt:
            try:
                return datetime.datetime.strftime(dt, self.to_formats[dt.str_format])
            except ValueError:
                pass
        return None



class DateTime(datetime.datetime):

    str_format = None
