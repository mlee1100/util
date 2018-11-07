import psutil
import time

while True:
    print psutil.virtual_memory().percent
    time.sleep(1)