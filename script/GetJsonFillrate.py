import collections
import json
import sys
import logging
import gzip
import multiprocessing

logger = logging.getLogger(__name__)


class Fillrate(object):

    sep = '>>'

    @property
    def counter(
        self,
    ):
        return dict(self._counter)

    @property
    def typer(
        self,
    ):
        return {k: dict(v) for k, v in self._typer.items()}
    

    def __init__(
        self,
        d = {},
    ):
        self._counter = collections.Counter(d)
        self._typer = collections.defaultdict(collections.Counter)

    def add(
        self,
        *data,
    ):
        for d in data:
            self._counter.update(self._gen_recursive_fill_rate(d))

    def merge(
        self,
        d,
    ):
        self._counter.update(d)

    def _type_add(
        self,
        path,
        obj,
    ):
        self._typer[path][obj.__class__.__name__] += 1

    def _non_none_keys(
        self,
        d,
    ):
        return (len(d) - ('NoneType' in d))

    # get fill rates from a flat or nested dict
    def _gen_recursive_fill_rate(
        self,
        d,
        path=(),
    ):
        self._type_add(path, d)

        # create fill rate counting dict
        result = collections.Counter()

        if isinstance(d, dict):
            result[path] += bool(d)
            for k, v in d.items():
                result.update(self._gen_recursive_fill_rate(v, path + (k,)))

        elif isinstance(d, (list, tuple)):
            result[path] += bool(d)
            for i, v in enumerate(d):
                result.update(self._gen_recursive_fill_rate(v, path + ('[{}]'.format(i),)))

        else:
            result[path] += self._not_null(d)

        return result

    def _not_null(self, value):
        if isinstance(value, str):
            value = value.strip()
            if value.isnumeric():
                try:
                    value = int(value)
                except ValueError:
                    pass
        # elif isinstance(value, (int, float)):
        #     return True
        return bool(value)
    
    # convert tuple keys from fillrate to standard format
    def standardize_keys(
        self,
        d=None,
    ):
        d = d or self.counter
        sorted_fillrate = sorted(d.items(), key=self._standard_sort)
        return [(self._join_key_tuple(k), v) for k, v in sorted_fillrate]

    def _join_key_tuple(
        self,
        key_tuple,
    ):
        return self.sep.join([str(i) for i in key_tuple])

    def _standard_sort(
        self,
        tup,
    ):
        tup = list(tup)
        tup[0] = list(tup[0])
        for i, k in enumerate(tup[0]):
            if k.startswith('[') and k.endswith(']'):
                k = k[1:-1]
                if k.isdigit():
                    tup[0][i] = int(k)
        return (len(tup[0])>0, len(tup[0])>1, tup[0])


class TestFillrate(object):

    maxDiff = None

    def setup_method(self):
        varied_dict = {
            'this': '',
            'is': [],
            'a': [1,2,3,],
            'test': {},
        }
        self.d = {
            'this': varied_dict,
            'is': [varied_dict for i in range(2)],
            'a': {i: varied_dict for i in ['for', 'real']},
            'test': 'seriously',
        }
        self.truth = {
            (): 1,
            ('this',): 1,
            ('this', 'this'): 0,
            ('this', 'a'): 1,
            ('this', 'a', '[0]'): 1,
            ('this', 'a', '[1]'): 1,
            ('this', 'a', '[2]'): 1,
            ('this', 'is'): 0,
            ('this', 'test'): 0,
            ('a',): 1,
            ('a', 'for'): 1,
            ('a', 'for', 'this'): 0,
            ('a', 'for', 'a'): 1,
            ('a', 'for', 'a', '[0]'): 1,
            ('a', 'for', 'a', '[1]'): 1,
            ('a', 'for', 'a', '[2]'): 1,
            ('a', 'for', 'is'): 0,
            ('a', 'for', 'test'): 0,
            ('a', 'real'): 1,
            ('a', 'real', 'this'): 0,
            ('a', 'real', 'a'): 1,
            ('a', 'real', 'a', '[0]'): 1,
            ('a', 'real', 'a', '[1]'): 1,
            ('a', 'real', 'a', '[2]'): 1,
            ('a', 'real', 'is'): 0,
            ('a', 'real', 'test'): 0,
            ('is',): 1,
            ('is', '[0]',): 1,
            ('is', '[0]', 'this'): 0,
            ('is', '[0]', 'a'): 1,
            ('is', '[0]', 'a', '[0]'): 1,
            ('is', '[0]', 'a', '[1]'): 1,
            ('is', '[0]', 'a', '[2]'): 1,
            ('is', '[0]', 'is'): 0,
            ('is', '[0]', 'test'): 0,
            ('is', '[1]',): 1,
            ('is', '[1]', 'this'): 0,
            ('is', '[1]', 'a'): 1,
            ('is', '[1]', 'a', '[0]'): 1,
            ('is', '[1]', 'a', '[1]'): 1,
            ('is', '[1]', 'a', '[2]'): 1,
            ('is', '[1]', 'is'): 0,
            ('is', '[1]', 'test'): 0,
            ('test',): 1
        }

        self.truth_typer = {
            (): {'dict': 1},
            ('a',): {'dict': 1},
            ('a', 'for'): {'dict': 1},
            ('a', 'for', 'a'): {'list': 1},
            ('a', 'for', 'a', '[0]'): {'int': 1},
            ('a', 'for', 'a', '[1]'): {'int': 1},
            ('a', 'for', 'a', '[2]'): {'int': 1},
            ('a', 'for', 'is'): {'list': 1},
            ('a', 'for', 'test'): {'dict': 1},
            ('a', 'for', 'this'): {'str': 1},
            ('a', 'real'): {'dict': 1},
            ('a', 'real', 'a'): {'list': 1},
            ('a', 'real', 'a', '[0]'): {'int': 1},
            ('a', 'real', 'a', '[1]'): {'int': 1},
            ('a', 'real', 'a', '[2]'): {'int': 1},
            ('a', 'real', 'is'): {'list': 1},
            ('a', 'real', 'test'): {'dict': 1},
            ('a', 'real', 'this'): {'str': 1},
            ('is',): {'list': 1},
            ('is', '[0]'): {'dict': 1},
            ('is', '[0]', 'a'): {'list': 1},
            ('is', '[0]', 'a', '[0]'): {'int': 1},
            ('is', '[0]', 'a', '[1]'): {'int': 1},
            ('is', '[0]', 'a', '[2]'): {'int': 1},
            ('is', '[0]', 'is'): {'list': 1},
            ('is', '[0]', 'test'): {'dict': 1},
            ('is', '[0]', 'this'): {'str': 1},
            ('is', '[1]'): {'dict': 1},
            ('is', '[1]', 'a'): {'list': 1},
            ('is', '[1]', 'a', '[0]'): {'int': 1},
            ('is', '[1]', 'a', '[1]'): {'int': 1},
            ('is', '[1]', 'a', '[2]'): {'int': 1},
            ('is', '[1]', 'is'): {'list': 1},
            ('is', '[1]', 'test'): {'dict': 1},
            ('is', '[1]', 'this'): {'str': 1},
            ('test',): {'str': 1},
            ('this',): {'dict': 1},
            ('this', 'a'): {'list': 1},
            ('this', 'a', '[0]'): {'int': 1},
            ('this', 'a', '[1]'): {'int': 1},
            ('this', 'a', '[2]'): {'int': 1},
            ('this', 'is'): {'list': 1},
            ('this', 'test'): {'dict': 1},
            ('this', 'this'): {'str': 1},
        }

    def test_verify_counts(self):
        for i in range(5):
            fr = Fillrate()
            list(map(fr.add, [self.d]*(i+1)))
            assert fr.counter == {k: v*(i+1) for k, v in self.truth.items()}

    def test_verify_typer(self):
        for i in range(5):
            fr = Fillrate()
            list(map(fr.add, [self.d]*(i+1)))
            assert fr.typer == {k: {t: c*(i+1) for t, c in v.items()} for k, v in self.truth_typer.items()}


def fillrate_file(filename):
    fr = Fillrate()
    bad_json = 0
    first_bad_json = 0
    with gzip.open(filename, 'rt', encoding='utf8') as ifile:
        for i, line in enumerate(ifile):
            try:
                fr.add(json.loads(line))
            except json.decoder.JSONDecodeError:
                # print(line)
                bad_json += 1
                if not first_bad_json:
                    first_bad_json = i+1
            except:
                raise
            # break
    return (fr.counter, filename, bad_json, first_bad_json)

if __name__ == '__main__':
    files = sys.argv[1:]
    fr_global = Fillrate()
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    res = pool.map(fillrate_file, files, 1)
    total_errors = 0
    for r, filename, bad_json_count, first_bad_json_line in res:
        if bad_json_count:
            total_errors += bad_json_count
            print(f'ERRORS in {filename}: {bad_json_count:,}: see line {first_bad_json_line}')
        fr_global.merge(r)
    if total_errors:
        print(f'ERRORS total: {total_errors:,}')
    for k, v in fr_global.standardize_keys():
        if k == '':
            k = 'TOTAL'
        print('{}: {:,}'.format(
            k,
            v,
        ))