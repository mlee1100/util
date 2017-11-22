import sys
import os
import random
# import unicodecsv as csv
import csv
import concurrent.futures
import MySQLdb.cursors
import gc
import redis
from hotqueue import HotQueue
from threading import Thread
import time
import argparse
import threading
import multiprocessing
import hashlib
from decimal import Decimal
from psutil import virtual_memory
from Queue import Queue
from psutil import virtual_memory
from Functions import *
import datetime
import traceback
import re

reload(sys)
sys.setdefaultencoding('UTF8')

parser = argparse.ArgumentParser(description='Generate Full Contact export files')
parser.add_argument('-d','--database',required=True, type=str, help='database alias')
parser.add_argument('-w','--workers',required=True, type=int, help='maximum thread pool workers')
parser.add_argument('-x','--maxmem', required=True, type=int, help='percent system mem usage to pause at')
parser.add_argument('-c','--conmem', required=True, type=int, help='percent system mem usage to continue process at')
parser.add_argument('-q','--queryfile', required=True, type=str, help='path to file containing SQL query')
parser.add_argument('-o','--outfile', required=True, type=str, help='path to output file')
parser.add_argument('-l','--delimiter', required=True, type=str, help='field delimiter')
parser.add_argument('-t','--looptable', required=True, type=str, help='table name to loop on')
parser.add_argument('-z','--samplesize', required=True, type=int, help='number of records to sample')
parser.add_argument('-k','--chunksize', required=False, type=int, default=2000, help='number of id columns to get for each query')
parser.add_argument('-u','--quoting', required=False, type=str, default='QUOTE_MINIMAL', help='field delimiter')
parser.add_argument('-e','--escapechar', required=False, type=str, default='\\', help='escape character')
parser.add_argument('-b','--doublequote', required=False, type=int, default=0, help='set escaping of quote character to the quote character itself')
parser.add_argument('-f','--fileexact', required=False, type=int, default=0, help='force output to use the exact file name')
args = parser.parse_args()

datestring = datetime.datetime.strftime(datetime.datetime.today(),'%Y%m%d-%H%M')

exec('args.quoting = csv.{quoting}'.format(quoting=args.quoting))

with open(args.queryfile) as ifile:
  loaded_query = ifile.read()
loaded_query = loaded_query + ';'

if args.fileexact:
  args.outfile = os.path.abspath(args.outfile)
else:
  args.outfile = ''.join([
    os.path.splitext(os.path.abspath(args.outfile))[0],
    '_',
    datestring,
    os.path.splitext(os.path.abspath(args.outfile))[1]
    ])

start = time.time()

query_chunks = args.chunksize

start_count = 0

sql_conn = dict(
  host = os.getenv('SQL_{db}_HOST'.format(db=args.database.upper())),
  user = os.getenv('SQL_{db}_USER'.format(db=args.database.upper())),
  password = os.getenv('SQL_{db}_PASSWORD'.format(db=args.database.upper())),
  database = os.getenv('SQL_{db}_DATABASE_DEFAULT'.format(db=args.database.upper())),
  )

m = multiprocessing.Manager()
q = m.Queue()

records_found = 0

def write_queue_to_file(q,path):

  global records_found

  with open(path,'wb') as ofile:

    while True:
      if not q.empty():
        q_value = q.get()
        field_count = len(q_value['fields'])
        q_value['values'] = [line for line in q_value['values'] if len(line) == field_count]
        if q_value['values']:
          break
      else:
        time.sleep(.1)
        
    # ocsv = csv.writer(ofile,delimiter=args.delimiter,quoting=args.quoting,encoding='utf-8')
    use_double_quote = (False if args.escapechar and not (True if args.doublequote else False) else True)
    ocsv = csv.writer(ofile,delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)

    ocsv.writerow(q_value['fields'])
    new_records = len(q_value['values'])
    records_found += new_records
    if records_found >= args.samplesize:
      ocsv.writerows(q_value['values'][:new_records-(records_found-args.samplesize)])
      q.task_done()
      return
    else:
      ocsv.writerows(q_value['values'])

    while True:
      if not q.empty():
        try:
            q_value = q.get()
            q_value['values'] = [line for line in q_value['values'] if len(line) == field_count]
            if q_value['values']:
              new_records = len(q_value['values'])
              records_found += new_records
              if records_found >= args.samplesize:
                ocsv.writerows(q_value['values'][:new_records-(records_found-args.samplesize)])
                break
              else:
                ocsv.writerows(q_value['values'])
        except:
            q.task_done()
            break
      else:
        time.sleep(.01)

  print 'QUEUE OUT'


def get_query(id_list):

  id_string = ','.join([str(n) for n in id_list])

  query = loaded_query.replace('$ID_LIST',id_string)

  return query


def parse_results(results):

  try:

    if results['values']:
      if args.escapechar and args.quoting != csv.QUOTE_NONE:
        return dict(
          fields = results['fields'],
          values = [[(str(v).replace(args.escapechar,args.escapechar*2) if v else v) for v in l] for l in results['values']], #esccape escape characters
          )
      else:
        return results
    else:
      return None

  except:
    traceback.print_exc()


def query_to_queue(id_list):

  try:

    pause_for_memory()

    results = parse_results(execute_query(sql_conn,query=get_query(id_list),execute_type='get',cursor_class=MySQLdb.cursors.Cursor,return_fields=True))

    # try:
    #   print get_time_left(current_id=inid*query_chunks-(start_count*query_chunks),max_id=max_id-(start_count*query_chunks),start_time=start)
    # except:
    #   print ''

    if results:
      q.put(results)

  except:
    traceback.print_exc()
    raise


def pause_for_memory():

  global mem_pause

  if virtual_memory().percent > args.maxmem or mem_pause:
    mem_pause = True
    while virtual_memory().percent > args.conmem and mem_pause:
      time.sleep(.1)
    mem_pause = False

  return


if __name__ == '__main__':

  try:
    
    os.system('setterm -cursor off')

    mem_pause = False

    max_id = int(execute_query(sql_conn,query='SELECT MAX(id) as `id` from `{table}`;'.format(table=args.looptable),execute_type='get')[0]['id'])
    t = Thread(target=write_queue_to_file,args=(q,args.outfile,))
    t.daemon = True
    t.start()

    counter_all = start_count
    rand_set = set()

    while records_found < args.samplesize:
      percent_done = int((float(records_found)/float(args.samplesize))*100)
      print '{p}%\r'.format(p=str(percent_done)),
      with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        concurrent_count = 0
        while concurrent_count < 10:
          concurrent_count += 1
          query_list = []
          while len(query_list) < args.chunksize:
            new_rand = random.randint(0,max_id)
            if new_rand not in rand_set:
              rand_set.update([new_rand,])
              query_list.append(new_rand)

          executor.submit(query_to_queue,query_list)


    q.put('break')
    t.join()

    print 'finished in {s}s'.format(s=int(time.time()-start))

  except:
    pass

  finally:
    os.system('setterm -cursor on')


