import subprocess
import sys
import os

script_to_kill = sys.argv[1]


def get_process_ids(script):

  stdout = subprocess.check_output([
    'ps',
    'ax',
    ])

  stdout = [l.strip().split(' python ') for l in stdout.split('\n') if ' python ' in l]

  return [l[0].split(' ')[0] for l in stdout if os.path.split(l[1].split(' ')[0])[1] == script]


def kill_processes(process_id_list):

  with open(os.devnull,'wb') as devnull:
    for process_id in process_id_list:
      subprocess.call([
          'kill',
          '-9',
          process_id
          ],
        stdout=devnull,
        stderr=subprocess.STDOUT
        )

  return



if __name__ == '__main__':

  ids_to_kill = True

  while ids_to_kill:
    ids_to_kill = get_process_ids(script_to_kill)
    kill_processes(ids_to_kill)

  os.system('setterm -cursor on')
