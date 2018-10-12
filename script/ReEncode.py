
import codecs
from chardet.universaldetector import UniversalDetector
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
    detector = UniversalDetector()
    increments = 10*1000
    counter = 0
    encoding = 'ascii'
    try:
        with tqdm(total=get_file_size(file_path)) as t:
            for line in file(file_path, 'rb'):
                detector.feed(line)
                counter += 1
                t.update(len(line))
                if detector.done:
                    break
                if counter == increments:
                    counter = 0
                    detector_results = detector.close()
                    if detector_results['encoding'] != encoding:
                        break
                    else:
                        detector.reset()

        detector_results = detector.close()

    except KeyboardInterrupt:
        detector_results = detector.close()
        print detector_results
        raise

    except:
        raise

    return detector_results['encoding']


def get_file_size(file_path):
    return os.path.getsize(file_path)


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
                    for line in ifile:
                        ofile.write(line)
                        t.update(len(line))

                except:
                    print line
                    raise
