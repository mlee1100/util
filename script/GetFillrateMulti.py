import pandas
import argparse
import multiprocessing
from collections import Counter
import time

parser = argparse.ArgumentParser(description='Generate Fillrates for flat file')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')

args = parser.parse_args()

not_filled = set([0, '', u'', None])
processors = multiprocessing.cpu_count()

start = time.time()


def is_filled(val):
    return val not in not_filled

def filled(x):
    return sum(x.apply(is_filled))

def get_frame_fillrate(frame):
    return Counter(frame.apply(filled, axis=0).to_dict())

if __name__ == '__main__':

    counter = Counter()
    pool = multiprocessing.Pool(processors)

    dframe = pandas.io.parsers.read_csv(args.filepath, sep='|', chunksize=10000, compression=('gzip' if args.filepath.endswith('.gz') else None))
    counter = sum(pool.map(get_frame_fillrate, dframe), counter)
    # for frame in dframe:
    #     counter = sum([Counter(frame.apply(filled, axis=0).to_dict()),], counter)
    pool.close()
    pool.join()
    print counter
    print time.time() - start