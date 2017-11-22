import datetime
import redis
import os
import subprocess
import time
import MySQLdb
import MySQLdb.cursors
import contextlib
import unicodecsv as csv
import resource
import collections
import traceback
from psutil import virtual_memory
from subprocess import call


def get_date_format(date_string,**kwargs): # flexible date parsing used to cleanse files
  
  if date_string is None:
    return None

  has_time = kwargs.get('has_time',None)
  first_try_format = kwargs.get('first_try_format',None)

  if has_time is None:
    if ' ' in date_string:
      has_time = True
    else:
      has_time = False

  formats_date = [
    '%m/%d/%Y',
    '%Y-%m-%d',
    '%Y%m%d',
    '%m%d%Y',
    ]

  if has_time:
    formats_time = [
      '%H:%M:%S',
      '%H:%M',
      '%H',
      '%I:%M:%S %p',
      '%I:%M %p',
      '%I %p',
      ]
  else:
    formats_time = ['']

  if first_try_format:
    try:
      datetime.datetime.strptime(date_string,first_try_format)
      return first_try_format
    except:
      pass

  for dform in formats_date:
    for tform in formats_time:
      try_date = ' '.join([dform,tform]).strip()

      try:
        datetime.datetime.strptime(date_string,try_date)
        return try_date
      except:
        continue
      
  return None




def get_datetime_string(instring,date_format):

  dt_obj = datetime.datetime.strptime(instring,date_format)

  return dt_obj.isoformat().replace('T',' ')[:19]



def redis_action(action,port,**kwargs):

  redis_properties = kwargs.get('redis_properties',None)
  if redis_properties:
    redis_location = redis_properties['location']
    redis_cli_path = redis_properties['cli']
    redis_server_path = redis_properties['server']
  else:
    redis_location = os.getenv('REDIS')
    redis_cli_path = os.getenv('REDISCLI')
    redis_server_path = os.getenv('REDISSERVER')

  def check_redis_started():
    check_command = '{redis_cli_path} ping'.format(redis_cli_path=redis_cli_path)
    try:
      return_value = subprocess.check_output([check_command],shell=True)
      if return_value.strip() == 'PONG':
        return True
    except:
      pass
    return False

  with open(os.devnull,'wb') as devnull:

    if action == 'start':
      try:
        ping_command = '{redis_cli_path} -p {port} ping'.format(redis_cli_path=redis_cli_path,port=port)
        return_value = subprocess.check_output([ping_command],shell=True) # this will print a line saying it can't connect, but it won't be logged
        if return_value.strip() == 'PONG':
          print 'redis server already running on {port}'.format(port=port)
      except:
        print 'booting up redis server on {port}'.format(port=port)
        start_command = '{redis_server_path} --port {port}'.format(redis_server_path=redis_server_path,port=port)
        return_value = subprocess.Popen([start_command],shell=True,stdout=devnull)
        check = False
        while not check: # wait until redis is actually running
          check = check_redis_started()
          time.sleep(.01)


    elif action == 'stop':
      try:
        stop_command = '{redis_cli_path} -p {port} shutdown NOSAVE'.format(redis_cli_path=redis_cli_path,port=port)
        subprocess.Popen([stop_command],shell=True,stdout=devnull)
        print 'shutting down redis server on {port}'.format(port=port)
      except Exception as e:
        print 'redis server could not be shutdown: {0}'.format(e)

  return



def execute_query(sql_conn,**kwargs):

  query = kwargs.get('query',None)
  execute_type = kwargs.get('execute_type','get')
  cursor_class = kwargs.get('cursor_class',MySQLdb.cursors.DictCursor)
  return_fields = kwargs.get('return_fields',False)
  autocommit = kwargs.get('autocommit',False)
  values = kwargs.get('values',None)
  procedure = kwargs.get('procedure',None)
  args = kwargs.get('args',[])
  read_uncommitted = kwargs.get('read_uncommitted',False)
  parameter_query = kwargs.get('parameter_query',None)

  if type(read_uncommitted) is not bool:
    read_uncommitted = False
  if type(autocommit) is not bool:
    autocommit = False

  try:
    with contextlib.closing(MySQLdb.connect(host=sql_conn['host'],user=sql_conn['user'],passwd=sql_conn['password'],db=sql_conn['database'],local_infile=1,charset='utf8',cursorclass=cursor_class,connect_timeout=600)) as con:

      con.autocommit(True)

      try:
        with contextlib.closing(con.cursor()) as cur:

          if execute_type == 'get':
            if read_uncommitted:
              isolation_query = 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'.format(isolation_level=isolation_level)
              cur.execute(isolation_query)
            if parameter_query:
              cur.execute(parameter_query)
            cur.execute(query)
            if return_fields:
              results = dict(
                values = cur.fetchall(),
                fields = [i[0] for i in cur.description],
                )
            else:
              results = cur.fetchall()
            return results
            
          elif execute_type == 'put':
            if values is None:
              cur.execute(query)
            else:
              cur.executemany(query, values)
            if not autocommit:
              try:
                print 'committing'
                con.commit()
              except:
                con.rollback()
                traceback.print_exc()
                raise
            return dict(
              row_count = int(cur.rowcount),
              last_id = int(cur.lastrowid)
              )

          elif execute_type == 'procedure':
            print 'executing {procedure}'.format(procedure=procedure)
            cur.callproc(procedure,args)
            results = cur.fetchall()
            cur.close()
            if not autocommit:
              try:
                print 'committing'
                con.commit()
                print 'finished executing {procedure}'.format(procedure=procedure)
              except:
                con.rollback()
                traceback.print_exc()
                raise
            return results

      except KeyboardInterrupt:
        con.close()
        raise
      except Exception:
        con.close()
        traceback.print_exc()
        raise

  except:
    exception_string = 'error running query: \n\r {query}'.format(query=query)
    raise

  return



def kill_process(q,main_pid,mem_percent_threshold):

  print 'main process is {pid}'.format(pid=main_pid)

  while True:
    if virtual_memory().percent > mem_percent_threshold:
      print 'Exceeded Memory Usage'
      for new_pid in q.consume():
        if new_pid == 'break':
          break
        print new_pid
        try:
          print 'killing {pid}'.format(pid=new_pid)
          call(['sudo','kill','-9',str(new_pid)])
          print 'killed {pid}'.format(pid=new_pid)
        except:
          pass
        finally:
          time.sleep(2)
        q.put('break')

      print 'killing {pid}'.format(pid=main_pid)
      call(['sudo','kill','-9',str(main_pid)])
      # sys.exit(1)
      break
    else:
      time.sleep(.1)



def splitFile(**kwargs):
  source_dir = kwargs.get('source_dir')
  out_dir = kwargs.get('out_dir')
  source_file = kwargs.get('source_file')
  has_header = kwargs.get('has_header')
  file_bit_size = kwargs.get('file_bit_size')

  source_path = '{0}{1}'.format(source_dir,source_file)
  return_paths = []

  file_part_name = out_dir + str(source_file).split(".")[0] + '_part_'
  number_of_parts = os.stat(source_path).st_size/file_bit_size+1
  with open(source_path, 'r') as f:
    if has_header:
      header = f.readline()
    for i in range(0, int(number_of_parts)):
      new_file_name = file_part_name + str(i) + '.txt'
      with open(new_file_name, 'w') as o:
        segment = f.readlines(int(file_bit_size))
        if has_header:
          o.write(header)
        for c in range(0, int(len(segment))):
          o.write(segment[c])
        return_paths.append(new_file_name)

  return return_paths


def load_properties(path,**kwargs):

  ordered = kwargs.get('ordered',False)

  with open(path,'rb') as ifile:
    icsv = csv.reader((l.decode('utf-8') for l in ifile),delimiter=':',quoting=csv.QUOTE_NONE,encoding='utf-8')
    
    if ordered is True:
      odict = collections.OrderedDict()
    elif ordered is False:
      odict = {}

    function_set = set()

    for line in icsv:
      if line:
        if len(line) == 1:
          odict[line[0]] = None
        elif line[1] == 'FUNCTION':
          function_set.update([line[0],])
          odict[line[0]] = line[2]
        else:
          odict[line[0]] = line[1]

    return (odict, function_set)


def get_time_left(**kwargs):

  def append_zero(input_int):
    return ('0'+str(input_int))[-2:]

  try:

    current_id = kwargs.get('current_id')
    max_id = kwargs.get('max_id')
    start_time = kwargs.get('start_time')

    p_done = float(current_id)/float(max_id)

    s = int(((1-p_done)/p_done)*(time.time()-start_time))

    m, s = divmod(s,60)
    h, m = divmod(m,60)

    h = append_zero(h)
    m = append_zero(m)
    s = append_zero(s)

    return '{h}:{m}:{s}\r'.format(h=h,m=m,s=s)

  except:
    return ''



def get_bijection(integer_list): # best way is to have the list of numbers in ascending order as much as possible; this will create smaller outputs

  def apply_bijection(n1,n2):
    return (2**(n1-1))*((2*n2)-1)

  try:
    integer_list = [int(i) for i in integer_list]

    while len(integer_list) > 1:
      n1 = integer_list[0]
      n2 = integer_list[1]
      list_remainder = integer_list[2:]
      integer_list = [apply_bijection(n1,n2),] + list_remainder
    return integer_list[0]

  except:
    print 'bijection can only be implemented with integers'
    raise