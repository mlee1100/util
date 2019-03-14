import gzip
import os
import json
import sys
import multiprocessing
import splitstream

directory_in, directory_out = sys.argv[1:]

class Wrapper(object):

    def __init__(self, f):
        self.__f = f

    def read(self, *n):
        return self.__f.read(*n)

class JsonLiner(object):

    def __init__(self):
        pass

    def json_to_jl(self, input_path, output_path):
        assert not os.path.exists(output_path), '{} already exists'.format(output_path)
        directory, basename = os.path.split(output_path)
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except OSError:
                pass
            except:
                raise
        input_handler = self.open_handler(input_path)
        output_handler = self.open_handler(output_path)

        with input_handler(input_path, 'rb') as ifile, output_handler(output_path, 'wb') as ofile:
            for line in splitstream.splitfile(Wrapper(ifile), format='json', startdepth=1):
                ofile.write(line + '\n')

        return output_path

    def open_handler(self, path):
        if path.endswith('.gz'):
            return gzip.open
        else:
            return open

def get_all_files_in_dir(directory):
    directory = os.path.abspath(directory)
    file_paths = list()
    for dirpath, dirnames, filenames in os.walk(directory):
        file_paths.extend([f for f in [os.path.join(dirpath, filename) for filename in filenames] if os.path.isfile(f)])
    return file_paths

def convert_file(input_path):
    parser = JsonLiner()
    input_path_relative = input_path.partition(directory_in)[-1].lstrip('/')
    output_path = os.path.join(directory_out, input_path_relative)
    return parser.json_to_jl(input_path, output_path)

if __name__ == '__main__':
    processes = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes)
    pool.map(convert_file, get_all_files_in_dir(directory_in), chunksize=1)
    # for file in get_all_files_in_dir(directory_in):
    #     convert_file(file)
    #     break
