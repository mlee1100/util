import models
import csv

"""
import file_chunker
import csv
reload(file_chunker)
f = '/home/ec2-user/temp/DOC_Netwise_20180212.txt'
o = '/home/ec2-user/temp/DOC_Netwise_20180212.txt.chunk'
chunker = file_chunker.FileChunker(f, 5, delimiter='|', quoting=csv.QUOTE_ALL)
locations = []
for i, seek_location in enumerate(chunker.yield_seekbyte()):
    locations.append(seek_location)
    if i == 6:
        break


with open(o, 'wb') as ofile:
    for location in locations:
        for line in chunker.yield_chunk(location, True):
            ofile.write(line['DateAdded'])


"""
class FileChunker(object):

    def __init__(self, csv_object, lines_per_chunk=1000):
        self.file = csv_object
        self.lines_per_chunk = lines_per_chunk
        self._assign_header()

    def _assign_header(self):
        self.file.header = self.file.get_header()

    def yield_seekbyte(self, has_header=False):
        lines = 0
        with self.file.open_for_reading() as ifile:
            file_iterator = iter(ifile.readline, '')
            if has_header:
                file_iterator.next()
            yield ifile.tell()
            for line in file_iterator:
                lines += 1
                if lines == self.lines_per_chunk:
                    lines = 0
                    yield ifile.tell()

    def yield_chunk(self, seekbyte, **kwargs):
        parse_read = kwargs.get('parse_read', False)
        if parse_read:
            openner = self.file.parse_read
        else:
            openner = self.file.open_for_reading
        with openner(seekbyte) as reader:
            for i, line in enumerate(reader):
                if i == self.lines_per_chunk:
                    break
                else:
                    yield line
