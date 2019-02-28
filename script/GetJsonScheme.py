# -*- coding: utf-8 -*-
import json
import collections
import sys
import gzip
import multiprocessing

reload(sys)
sys.setdefaultencoding('utf8')


def get_open_handler(path):
    if path.endswith('.gz'):
        return gzip.open
    else:
        return open

class JsonScheme(object):

    def __init__(self):
        self.scheme = dict()

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

        elif d is None:
            result[path] = None

        else:
            result[path] = type(d)()

        return result

    def add(self, d):
        for path, end_type in self._recursive_add(d).items():
            if path in self.scheme:
                if self.scheme[path] != end_type and self.scheme[path] is not None and end_type is not None:
                    print 'path {} has multiple types: {}, {}'.format(path, self.scheme[path], end_type)
                if end_type is not None:
                    self.scheme[path] = end_type
                    continue
            self.scheme[path] = end_type

    def display(self):
        for path, end_type in sorted(self.scheme.items(), key=lambda k: (len(k[0]), k[0])):
            print '{}: {}'.format(path, end_type)

    def _recursive_get_from_path(self, item, path):
        for elem in path:
            item = item[elem]
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


    def consolidate(self):
        for path, end_type in dict(self.scheme).items():
            if end_type is None:
                if path in [tuple(p[:len(path)]) for p in self.scheme if tuple(p[:len(path)]) != p]:
                    self.scheme.pop(path)

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
            print consolidated
            print index
            print path
            print self.scheme
            raise

        else:
            return consolidated


def mp_function(path):
    scheme = JsonScheme()
    with get_open_handler(path)(path, 'rb') as ifile:
        for line in ifile:
            scheme.add(json.loads(line))

    return scheme.scheme



if __name__ == '__main__':

    file_paths = sys.argv[1:]

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    scheme = JsonScheme()
    for scheme_result in pool.map_async(mp_function, file_paths).get(9999999):
        scheme.scheme.update(scheme_result)

    print json.dumps(scheme.consolidate())

