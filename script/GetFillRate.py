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
parser.add_argument('-z','--excludezeros',required=False, type=int, default=1, help='count zeros as not filled')
parser.add_argument('-d','--distinct',required=False, type=int, default=0, help='get distinct counts for each field')
parser.add_argument('-n','--nullvalues',required=False, type=str, default='', help='which values ot count as not filled')
parser.add_argument('-s','--stop',required=False, type=int, default=None, help='number of lines to scan')
parser.add_argument('-r','--header',required=False, nargs="*", default=None, help='header')
args = parser.parse_args()

start = time.time()

exec('args.quoting = csv.{quoting}'.format(quoting=args.quoting))
use_double_quote = (False if args.escapechar and not (True if args.doublequote else False) else True)
args.nullvalues = args.nullvalues.split(',')
if args.excludezeros:
  args.nullvalues.append('0')
null_values = set(args.nullvalues)

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


def return_int_list(inlist):

  return [(0 if v in null_values else 1) for v in inlist]
  # return [(1 if '"' in v else 0) for v in inlist]


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

  if args.filepath == '-':
    ifile = sys.stdin
    t = None
  else:
    ifile = open(args.filepath, 'rU')
    t = tqdm(total=os.path.getsize(args.filepath))

  rargs = dict(delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)
  icsv = csv.reader((line.replace('\0','').replace('\r','') for line in ifile),**rargs)
  # icsv = csv.reader(ifile,delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)
  if args.header:
    header = args.header
  else:
    header = icsv.next()

  previous_line = list(header)
  count_list = [0 for v in header]
  count_set = [set() for v in header]

  if args.distinct:
    for i, line in enumerate(icsv):
      if i == args.stop:
        break
      lines += 1
      if t:
        t.update(ifile.tell()-tell)
        tell = ifile.tell()
      try:
        count_set = [append_to_set(s,line[i].lower()) for i, s in enumerate(count_set)]
      except:
        print line
        issues += 1
        traceback.print_exc()
        # sys.exit(1)

  else:
    try:
      for i, line in enumerate(icsv):
        if i == args.stop:
          break
        lines += 1
        if t:
          t.update(ifile.tell()-tell)
          tell = ifile.tell()
        try:
          count_list = map(add,count_list,return_int_list(line))
          previous_line = line
        except:
          print previous_line
          print line
          issues += 1
          traceback.print_exc()
          # raise
          # sys.exit(1)
    except:
      print lines
      raise


  output_values = ([len(s) for s in count_set] if args.distinct else count_list)
  field_dict = [(field,value) for field, value in zip(header,output_values)]

  print '{issues} issues'.format(issues=str(issues))
  print 'records: {t:,}'.format(
    t = lines,
    )
  print 'COLUMN|FILLED|PERCENT'
  for key, value in field_dict:
    print '{key}|{value:,}|{percent}%'.format(
      key = key,
      value = value,
      percent = round(math.floor((float(value)/float(lines))*1000)/10,1),
      lines = lines,
      ).replace('.0%','%')



  runtime = int(time.time()-start)

  print convert_time(runtime)