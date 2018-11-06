import urlparse
import os

'''
import utilities
utilities.URL_test()
'''
class URL(object):

    def __init__(self, url=''):
        self.url = url

    def _has_protocal(self, raw_url):
        return ('//' in raw_url)

    @property
    def url(self):
        return ''.join([
            (self.scheme + '://' if self.scheme else ''),
            self.netloc,
            self.path,
            (';' + self.params if self.params else ''),
            ('?' + self.query if self.query else ''),
            ('#' + self.fragment if self.fragment else ''),
            ])

    @url.setter
    def url(self, value):
        assert isinstance(value, basestring), 'url must be of type basestring'
        self._url = value
        if not self._has_protocal(value):
            value = '//' + value
        parsed_url = urlparse.urlparse(value)
        self.scheme = parsed_url.scheme
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.params = parsed_url.params
        self.query = parsed_url.query
        self.fragment = parsed_url.fragment
        self.username = parsed_url.username
        self.password = parsed_url.password
        self.hostname = parsed_url.hostname
        self.port = parsed_url.port

    @property
    def scheme(self):
        return self._scheme

    @scheme.setter
    def scheme(self, value):
        assert isinstance(value, basestring), 'scheme must be of type basestring'
        self._scheme = value

    @property
    def netloc(self):
        return self._netloc

    @netloc.setter
    def netloc(self, value):
        assert isinstance(value, basestring), 'netloc must be of type basestring'
        self._netloc = value

    @property
    def domain(self):
        return self.netloc.lstrip('www.')

    @domain.setter
    def domain(self, value):
        assert isinstance(value, basestring), 'domain must be of type basestring'
        value = value.lstrip('www.')
        self.netloc = 'www.' + value

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        assert isinstance(value, basestring), 'path must be of type basestring'
        self._path = value

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, value):
        assert isinstance(value, basestring), 'params must be of type basestring'
        self._params = value

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, value):
        assert isinstance(value, basestring), 'query must be of type basestring'
        self._query = value

    @property
    def fragment(self):
        return self._fragment

    @fragment.setter
    def fragment(self, value):
        assert isinstance(value, basestring), 'fragment must be of type basestring'
        self._fragment = value

    def to_dict(self):
        return dict(
            url = self.url,
            scheme = self.scheme,
            domain = self.domain,
            netloc = self.netloc,
            path = self.path,
            params = self.params,
            query = self.query,
            fragment = self.fragment,
            )

def URL_test():
    input_url = 'https://www.google.com/search;parse1;parse2?q=what+is+the+www+called+in+url&oq=what+is+the+www+called+in+url&aqs=chrome..69i57j0.3767j1j7&sourceid=chrome&ie=UTF-8#yeaaa'
    u = URL(input_url)
    assert u.scheme == 'https'
    assert u.netloc == 'www.google.com'
    assert u.path == '/search'
    assert u.params == 'parse1;parse2'
    assert u.query == 'q=what+is+the+www+called+in+url&oq=what+is+the+www+called+in+url&aqs=chrome..69i57j0.3767j1j7&sourceid=chrome&ie=UTF-8'
    assert u.fragment == 'yeaaa'
    assert u.url == input_url
    print u.to_dict()


class FileSliceGenerator(object):

    def __init__(self, path):
        self.path = path
        self.file_size = os.path.getsize(self.path)

    def yield_file_location(self, chunks):
        start = 0
        raw_chunk_size = self.file_size / chunks
        with open(self.path, 'rb', 0) as ifile:
            while True:
                end = min(start + raw_chunk_size, self.file_size)
                ifile.seek(end)
                if end < self.file_size:
                    ifile.readline()
                yield (start, ifile.tell())
                start = ifile.tell()
                if start >= self.file_size:
                    break

    def read_chunk_lines(self, locations):
        start, end = locations
        with open(self.path, 'rb', 0) as ifile:
            ifile.seek(start)
            while ifile.tell() < end:
                yield ifile.readline()

def Slice_test():
    slicer = FileSliceGenerator('/home/ec2-user/temp/intelldata/400M_2018_09_24.badopt.psv')
    output = '/home/ec2-user/temp/intelldata/400M_2018_09_24.badopt.clone.psv'
    counter = 0
    with open(output, 'wb') as ofile:
        for location in slicer.yield_file_location(10):
            for line in slicer.read_chunk_lines(location):
                counter += 1
                ofile.write(line)
    print counter


if __name__ == '__main__':
    # URL_test()
    Slice_test()


