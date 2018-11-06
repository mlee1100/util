
import codecs
import chardet
import argparse
import os
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Generate Fillrates for flat file')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')
parser.add_argument('-o','--output_path',required=False, type=str, default=None, help='output file path')
parser.add_argument('-e','--encoding',required=False, type=str, default='utf8', help='new file encoding')
parser.add_argument('-a','--action',required=False, type=str, default='strict', help='action on encode errors')
args = parser.parse_args()

args.filepath = os.path.abspath(args.filepath)


def detect_encoding(file_path):
    encoding = chardet.detect('a')['encoding']
    try:
        with tqdm(total=get_file_size(file_path)) as t:
            for chunk in read_in_chunks(file(file_path, 'rb'), 1024**2):
                detector_results = chardet.detect(chunk)
                t.update(len(chunk))
                if detector_results['encoding'] != encoding:
                    break

    except KeyboardInterrupt:
        print detector_results
        raise

    except:
        raise

    return detector_results['encoding']


def get_file_size(file_path):
    return os.path.getsize(file_path)

def read_in_chunks(file_object, chunk_size=4*1024**2):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


if __name__ == '__main__':
    encoding = detect_encoding(args.filepath)
    print encoding
    if args.output_path is not None:
        args.output_path = os.path.abspath(args.output_path)
        if args.filepath == args.output_path:
            raise Exception('input and output paths cannot match')
        with tqdm(total=get_file_size(args.filepath)) as t:
            with codecs.open(args.filepath, 'rb', encoding) as ifile, codecs.open(args.output_path, 'wb', args.encoding, args.action) as ofile:
                try:
                    for chunk in read_in_chunks(ifile):
                        ofile.write(chunk)
                        t.update(len(chunk))

                except:
                    print chunk
                    raise
