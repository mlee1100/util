import sys
import os
import random
from warnings import filterwarnings
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
from Queue import Queue
from psutil import virtual_memory
from Functions import execute_query, get_time_left, redis_action
import datetime
import traceback
import requests
import re

reload(sys)
sys.setdefaultencoding('UTF8')
filterwarnings('ignore', category = MySQLdb.Warning)

parser = argparse.ArgumentParser(description='Generate Full Contact export files')
parser.add_argument('-d','--database',required=False, type=str, default='contactsv2prod', help='database alias')
parser.add_argument('-w','--workers',required=False, type=int, default=20, help='maximum thread pool workers')
parser.add_argument('-x','--maxmem', required=False, type=int, default=80, help='percent system mem usage to pause at')
parser.add_argument('-c','--conmem', required=False, type=int, default=50, help='percent system mem usage to continue process at')
parser.add_argument('-q','--queryfile', required=True, type=str, help='path to file containing SQL query')
parser.add_argument('-o','--outfile', required=True, type=str, help='path to output file')
parser.add_argument('-l','--delimiter', required=False, type=str, default=',', help='field delimiter')
parser.add_argument('-t','--looptable', required=True, type=str, help='table name to loop on')
parser.add_argument('-k','--chunksize', required=False, type=int, default=2000, help='number of id columns to get for each query')
parser.add_argument('-u','--quoting', required=False, type=str, default='QUOTE_MINIMAL', help='field delimiter')
parser.add_argument('-e','--escapechar', required=False, type=str, default='\\', help='escape character')
parser.add_argument('-b','--doublequote', required=False, type=int, default=0, help='set escaping of quote character to the quote character itself')
parser.add_argument('-f','--fileexact', required=False, type=int, default=0, help='force output to use the exact file name')
args = parser.parse_args()

mailgun_api_key = os.getenv('MAILGUN_API_KEY',None)
mailgun_api_url = os.getenv('MAILGUN_API_URL',None)

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

def write_queue_to_file(q,path):

  with open(path,'wb') as ofile:

    while True:
      if not q.empty():
        q_value = q.get()
        if q_value['values']:
          break
      else:
        time.sleep(.1)
        
    # ocsv = csv.writer(ofile,delimiter=args.delimiter,quoting=args.quoting,encoding='utf-8')
    use_double_quote = (False if args.escapechar and not (True if args.doublequote else False) else True)
    ocsv = csv.writer(ofile,delimiter=args.delimiter,quoting=args.quoting,escapechar=args.escapechar,doublequote=use_double_quote)

    ocsv.writerow(q_value['fields'])
    ocsv.writerows(q_value['values'])

    while True:
      if not q.empty():
        try:
            q_value = q.get()
            if q_value['values']:
              ocsv.writerows(q_value['values'])
        except:
            q.task_done()
            break
      else:
        gc.collect()
        time.sleep(.01)

  print 'QUEUE OUT'


def get_query(inid):

  inid_min = inid * query_chunks
  inid_max = inid_min + query_chunks - 1

  query = loaded_query.replace('$START_ID',str(inid_min)).replace('$END_ID',str(inid_max))

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


def query_to_queue(intup):

  tries = 10
  try_count = 0

  try:

    inid = intup

    pause_for_memory()

    while try_count < tries:
      try:
        results = parse_results(execute_query(sql_conn,query=get_query(inid),execute_type='get',cursor_class=MySQLdb.cursors.Cursor,return_fields=True))
        break
      except:
        try_count += 1
        if try_count == tries:
          raise

    if results:
      q.put(results)
      results = []
      gc.collect()

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


def send_email(runtime):

  email_from = 'alert@ec2-notifier'
  email_to = 'michael.lee@netwisedata.com'

  email_subject = ' '.join([
    sys.argv[0],
    'finished in',
    str(runtime),
    'seconds',
    ])

  email_body = 'python ' + ' '.join(sys.argv) + '\r\n\r\n'

  email_body += '\r\n'.join(sorted([
    '{k}: {v}'.format(k=k,v=v) for k, v in args.__dict__.iteritems()
    ]))

  r = requests.post(
    mailgun_api_url,
    auth=("api", mailgun_api_key),
    # files=[('attachment',open(f)) for f in args.attachments if os.path.exists(f)],
    data={
      "from": email_from,
      "to": email_to,
      "subject": email_subject,
      "text": email_body,
      }
    )

  if int(r.status_code) == 200:
    print 'Successfully sent {subject}'.format(subject=email_subject)
  else:
    print 'there was some error sending the email'
    print r
    print r.content


if __name__ == '__main__':

  mem_pause = False

  max_id = int(execute_query(sql_conn,query='SELECT MAX(id) as `id` from `{table}`;'.format(table=args.looptable),execute_type='get')[0]['id'])
  t = multiprocessing.Process(target=write_queue_to_file,args=(q,args.outfile,))
  t.daemon = True
  t.start()

  counter_all = start_count
  futures_per_break = 300

  while True:
    futures_count = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
    # with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
      while counter_all <= int(max_id/query_chunks)+1:
        futures_count += 1
        pause_for_memory()
        executor.submit(query_to_queue,counter_all)
        counter_all += 1
        if futures_count == futures_per_break:
          # print 'breaking for futures'
          break

    while q.qsize() > 0:
      time.sleep(.1)

    try:
      print '\tETA ' + get_time_left(current_id=counter_all*query_chunks-(start_count*query_chunks),max_id=max_id-(start_count*query_chunks),start_time=start) + '\r',
    except:
      print ''

    if futures_count < futures_per_break:
      break

  q.put('break')
  t.join()

  runtime = int(time.time()-start)

  send_email(runtime)

  print 'finished in {s}s'.format(s=runtime)


