# -*- coding: utf-8 -*-
import json
import collections
import sys
import gzip
import bz2
import multiprocessing
import traceback
import random
import argparse
import splitstream

reload(sys)
sys.setdefaultencoding('utf8')

argparser = argparse.ArgumentParser()
argparser.add_argument('-s', '--sample_frequency', required=False, type=int, default=0)
argparser.add_argument('-j', '--is_json', action='store_true')
argparser.add_argument('-e', '--empty_values', action='store_true')
argparser.add_argument('file_paths', nargs='+')
args = argparser.parse_args()


class Wrapper(object):

    def __init__(self, f):
        self.__f = f

    def read(self, *n):
        return self.__f.read(*n)


def convert_json_file(open_file):
    return splitstream.splitfile(Wrapper(open_file), format='json', startdepth=1)


def get_open_handler(path):
    if path.endswith('.gz'):
        return gzip.open
    # elif path.endswith('.bz2'):
    #     return bz2.open
    else:
        return open

class JsonScheme(object):

    null_set = set([None,])

    def __init__(self):
        self.scheme = dict()
        self.type_conflicts = set()

    def _recursive_add(
        self,
        d,
        path=(),
    ):
        # create fill rate counting dict

        result = dict()

        if isinstance(d, dict):
            for k, v in d.items():
                result.update(self._recursive_add(v, path + (k,)))

        elif isinstance(d, (list, tuple)):
            for i, v in enumerate(d):
                result.update(self._recursive_add(v, path + (0,)))

        # elif d is None:
        #     result[path] = None

        elif args.empty_values:
            if d is None:
                result[path] = None
            elif isinstance(d, basestring):
                result[path] = ''
            elif isinstance(d, (int, long)):
                result[path] = 0
            elif isinstance(d, float):
                result[path] = 0.0
            else:
                raise NotImplementedError(type(d))

        elif isinstance(d, basestring):
            result[path] = d.strip()

        else:
            result[path] = d

        return result

    def add(self, d):
        try:
            for path, end_type in self._recursive_add(d).items():
                if path in self.scheme:
                    if type(self.scheme[path]) != type(end_type) and self.scheme[path] is not None and end_type is not None:
                        self.type_conflicts.add('path {} has multiple types: {}, {}'.format(path, *sorted([type(self.scheme[path]), type(end_type)])))
                    if self.scheme[path] in self.null_set and end_type not in self.null_set:
                        self.scheme[path] = end_type
                    elif isinstance(self.scheme[path], basestring) and isinstance(end_type, basestring) and len(self.scheme[path]) < 10 and len(end_type) > len(self.scheme[path]):
                        self.scheme[path] = end_type
                else:
                    self.scheme[path] = end_type
        except:
            # print json.dumps(d)
            # print path
            traceback.print_exc()
            raise

    def display(self):
        for path, end_type in sorted(self.scheme.items(), key=lambda k: (len(k[0]), k[0])):
            print '{}: {}'.format(path, end_type)

    def _recursive_get_from_path(self, item, path):
        for elem in path:
            item = item[elem]
        if isinstance(item, basestring):
            return item[:100]
        else:
            return item

    def _recursive_exists_from_path(self, item, path):
        try:
            self._recursive_get_from_path(item, path)
            return True
        except:
            return False

    def _recursive_set_from_path(self, item, path, value):
        plen = len(path)
        self._recursive_get_from_path(item, path[:plen-1])[path[-1]] = value

    def _trim_conflicts(self):
        for path, end_type in dict(self.scheme).items():
            possible_conflicts = {tuple(p[:len(path)]): (p, self.scheme[p]) for p in self.scheme.keys() if path != p}
            if end_type is None:
                if path in possible_conflicts:
                    self.scheme.pop(path)
            else:
                if path in possible_conflicts:
                    print 'conflict: {} with {}'.format((path, end_type), possible_conflicts[path])
                    self.scheme.pop(path)


    def consolidate(self):
        self._trim_conflicts()
        consolidated = dict()
        max_index = max([len(k) for k in self.scheme])
        try:
            for index in range(max_index):
                for path, end_type in sorted(self.scheme.items(), key=lambda k: (len(k[0]), k[0])):
                    if len(path) == index + 1:
                        self._recursive_set_from_path(consolidated, path, end_type)
                        continue
                    elif self._recursive_exists_from_path(consolidated, path[:index+1]):
                        continue
                    else:
                        if isinstance(path[index+1], basestring):
                            self._recursive_set_from_path(consolidated, path[:index+1], dict())
                        else:
                            if len(path) > index + 1:
                                self._recursive_set_from_path(consolidated, path[:index+1], [dict(),])
                            else:
                                self._recursive_set_from_path(consolidated, path[:index+1], [None,])

        except:
            # print consolidated
            # print index
            # print path
            # print self.scheme
            raise

        else:
            return consolidated


def mp_function(path):
    scheme = JsonScheme()
    with get_open_handler(path)(path, 'rb') as ifile:
        if args.is_json:
            ifile = convert_json_file(ifile)
        for i, line in enumerate(ifile):
            try:
                if args.sample_frequency == 0 or random.randint(0, args.sample_frequency) == 0:
                    scheme.add(json.loads(line))
            except:
                pass
                raise
            # if i == 1000:
            #     break

    # print scheme.scheme
    return scheme.scheme, scheme.type_conflicts



if __name__ == '__main__':

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    scheme = JsonScheme()
    type_conflicts = set()
    for scheme_result, scheme_type_conflicts in pool.map_async(mp_function, args.file_paths).get(99999999):
        type_conflicts.update(scheme_type_conflicts)
        for path, end_type in scheme_result.items():
            if scheme.scheme.get(path, None) in scheme.null_set:
                scheme.scheme[path] = end_type

    for type_conflict in type_conflicts:
        print type_conflict
    print json.dumps(scheme.consolidate(), ensure_ascii=False, indent=4).encode('utf8')

