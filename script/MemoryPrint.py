import psutil
import time

max_mem = 0
while True:
    try:
        mem = psutil.virtual_memory().percent
        print mem
        if mem > max_mem:
            max_mem = mem
        time.sleep(1)
    except KeyboardInterrupt:
        print '\nmax memory of session: {}%'.format(max_mem)
        break
    except:
        raise