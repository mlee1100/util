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
class DateParser(object):

    full_formats = [
        '%Y-%m-%d',
        '%Y%m%d',
        '%Y%m%d%H%M%S',
        '%Y%m%d%H%M',
    ]

    partial_formats = list(set([
        '%Y',
        '%Y%m',
        '%Y-%m',
    ] + [f.replace('%m', '00') for f in full_formats] + [f.replace('%d', '00') for f in full_formats] + [f.replace('%m', '00').replace('%d', '00') for f in full_formats]))

    first_format = full_formats[0]

    def parse(self, date_string, partial=False):
        dt = None
        try_formats = self.full_formats
        if partial:
            try_formats += self.partial_formats

        try:
            dt = DateTime.strptime(date_string, self.first_format)
            # dt.str_format = self.first_format
        except ValueError:
            for try_format in try_formats:
                try:
                    dt = DateTime.strptime(date_string, try_format)
                    # dt.str_format = try_format
                    self.first_format = try_format
                except ValueError:
                    continue
                else:
                    break
        return dt


class DateTime(datetime.datetime):

    str_format = None

    _db_mapping = (
        ('%Y', 4, set(['%Y', '%y']),),
        ('%m', 2, set(['%m', ]),),
        ('%d', 2, set(['%d', ]),),
        ('%H', 2, set(['%H', '%I']),),
        ('%M', 2, set(['%M', ]),),
        ('%S', 2, set(['%S', ]),),
        )

    def _build_db_format(self):

        o = '%Y-%m-%d %H:%M:%S'
        formats_used = self._set_formats_used()

        for output_symbol, length, input_symbols in self._db_mapping:
            if not formats_used.intersection(input_symbols):
                o = o.replace(output_symbol, '0'*length)
        return o

    def _set_formats_used(self):
        stripped = re.sub(r'[^a-zA-Z\%]', '', self.str_format)
        return set([stripped[i:i+2] for i in range(len(stripped)) if i % 2 == 0])
    
    def to_db(self):
        return self.strftime(self._build_db_format())

    @classmethod
    def strptime(cls, date_string, dformat):
        r = super(DateTime, cls).strptime(date_string, dformat)
        cls.str_format = dformat
        return r