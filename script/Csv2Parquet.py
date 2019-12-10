import pandas as pd
import sys
import os
import csv
import gzip
from pyarrow.parquet import ParquetFile

args = sys.argv[1:]
infile = args[0]
if len(args) > 1:
    outfile = args[1]
else:
    outfile = os.path.splitext(infile)[0] + '.parquet'
    if outfile == infile:
        outfile = outfile + '_1'

if infile.endswith('.gz'):
    opener = gzip.open
else:
    opener = open

with opener(infile, 'rb') as ifile:
    fline = next(ifile)
    if fline.count(b'|') >= 2:
        delimiter = '|'
    elif fline.count(b'\t') >= 2:
        delimiter = '\t'
    else:
        delimiter = ','


try:
    pd.read_csv(infile, converters={i: str for i in range(0, 1000)}).to_parquet(outfile)
except:
    print(infile)
    raise
# print(outfile)
# print(ParquetFile(outfile).schema)