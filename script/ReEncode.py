
import codecs
from chardet.universaldetector import UniversalDetector
import argparse
import os

parser = argparse.ArgumentParser(description='Generate Fillrates for flat file')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')
parser.add_argument('-o','--output_path',required=True, type=str, help='output file path')
parser.add_argument('-e','--encoding',required=True, type=str, help='new file encoding')
args = parser.parse_args()

args.filepath = os.path.abspath(args.filepath)
args.output_path = os.path.abspath(args.output_path)


def detect_encoding(file_path):
    detector = UniversalDetector()
    for line in file(file_path, 'rb'):
        detector.feed(line)
        if detector.done:
            break

    detector_results = detector.close()
    if detector_results['confidence'] == 1:
        return detector_results['encoding']
    else:
        raise Exception('could not detect encoding of {} confidently'.format(file_path))


if __name__ == '__main__':
    if args.filepath == args.output_path:
        raise Exception('input and output paths cannot match')
    encoding = detect_encoding(args.filepath)
    with codecs.open(args.filepath, 'rb', encoding) as ifile, codecs.open(args.output_path, 'wb', args.encoding) as ofile:
        for line in ifile:
            ofile.write(line)
