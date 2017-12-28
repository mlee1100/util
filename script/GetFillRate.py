import csv
import argparse
from collections import OrderedDict
import traceback
import math
import time
import sys
from operator import add


parser = argparse.ArgumentParser(description='Generate Fillrates for flat file')
parser.add_argument('-f','--filepath',required=True, type=str, help='path of file to get fill rate on')
parser.add_argument('-l','--delimiter',required=True, type=str, help='file delimiter')
parser.add_argument('-u','--quoting',required=False, type=str, default='QUOTE_MINIMAL', help='set quoting option')
parser.add_argument('-e','--escapechar',required=False, type=str, default=None, help='set escape character')
parser.add_argument('-b','--doublequote', required=False, type=int, default=1, help='set escaping of quote character to the quote character itself')
parser.add_argument('-z','--excludezeros',required=False, type=int, default=1, help='count zeros as not filled')
parser.add_argument('-d','--distinct',required=False, type=int, default=0, help='get distinct counts for each field')
parser.add_argument('-n','--nullvalues',required=False, type=str, default='', help='which values ot count as not filled')
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


def append_to_set(inset,value):

  if value not in null_values:
    inset.update([value,])

  return inset


if __name__ == '__main__':

  # sys.exit(0)
  
  lines = 0
  issues = 0

  with open(args.filepath,'rU') as ifile:
    icsv = csv.reader(ifile,delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)
    header = icsv.next()

    count_list = [0 for v in header]
    count_set = [set() for v in header]

    if args.distinct:
      for line in icsv:
        lines += 1
        try:
          count_set = [append_to_set(s,line[i].lower()) for i, s in enumerate(count_set)]
        except:
          print line
          issues += 1
          traceback.print_exc()
          # sys.exit(1)

    else:
      for line in icsv:
        lines += 1
        try:
          count_list = map(add,count_list,return_int_list(line))
        except:
          print line
          issues += 1
          traceback.print_exc()
          # sys.exit(1)

  output_values = ([len(s) for s in count_set] if args.distinct else count_list)
  field_dict = [(field,value) for field, value in zip(header,output_values)]

  print '{issues} issues'.format(issues=str(issues))
  print 'records: {t:,}'.format(
    t = lines,
    )

  for key, value in field_dict:
    print '{percent:0.1f}%: {key}: {value:,}'.format(
      key = key,
      value = value,
      percent = round(math.floor((float(value)/float(lines))*1000)/10,1),
      lines = lines,
      ).replace('.0%','%')



  runtime = int(time.time()-start)

  print convert_time(runtime)