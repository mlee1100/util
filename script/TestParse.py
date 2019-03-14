import csv
import argparse
from collections import OrderedDict
import traceback
import math
import time
import sys
from operator import add
from tqdm import tqdm
import os


parser = argparse.ArgumentParser(description='Generate Fillrates for flat file')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')
parser.add_argument('-l','--delimiter',required=True, type=str, help='file delimiter')
parser.add_argument('-u','--quoting',required=False, type=str, default='QUOTE_MINIMAL', help='set quoting option')
parser.add_argument('-e','--escapechar',required=False, type=str, default=None, help='set escape character')
parser.add_argument('-b','--doublequote', required=False, type=int, default=1, help='set escaping of quote character to the quote character itself')
parser.add_argument('-s','--stop',required=False, type=int, default=None, help='number of lines to scan')
args = parser.parse_args()

start = time.time()

exec('args.quoting = csv.{quoting}'.format(quoting=args.quoting))
use_double_quote = (False if args.escapechar and not (True if args.doublequote else False) else True)

def convert_time(seconds):

  minutes, seconds = divmod(seconds,60)
  hours, minutes = divmod(minutes,60)

  if hours:
    output = '{h} hours {m} minutes {s} seconds'.format(
      h = hours,
      m = minutes,
      s = seconds,
      )

  elif minutes:
    output = '{m} minutes {s} seconds'.format(
      m = minutes,
      s = seconds,
      )

  else:
    output = '{s} seconds'.format(
      s = seconds,
      )

  return output


def append_to_set(inset,value):

  if value not in null_values:
    if value not in inset:
      inset.add(value)
    else:
      # print value
      pass

  return inset


if __name__ == '__main__':

  # sys.exit(0)
  
  lines = 0
  issues = 0
  tell = 0
  first_issue = None

  with open(args.filepath,'rU') as ifile, tqdm(total=os.path.getsize(args.filepath)) as t:
    icsv = csv.reader((line.replace('\0','').replace('\r','') for line in ifile),delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)
    # icsv = csv.reader(ifile,delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)
    header = icsv.next()
    header_len = len(header)

    try:
      for i, line in enumerate(icsv):
        if i == args.stop:
          break
        lines += 1
        if len(line) != header_len:
          issues += 1
          if not first_issue:
            first_issue = (i+1, line)
          print (i+1, line)
        t.update(ifile.tell()-tell)
        tell = ifile.tell()
    except:
      print lines
      raise

  print '{issues} issues'.format(issues=str(issues))
  print 'records: {t:,}'.format(
    t = lines,
    )
  if first_issue:
    print first_issue



  runtime = int(time.time()-start)

  print convert_time(runtime)