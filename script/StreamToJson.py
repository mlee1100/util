import sys
import json
import csv

icsv = csv.DictReader(sys.stdin, delimiter=',', quoting=csv.QUOTE_MINIMAL, doublequote=False, escapechar='\\')
for line in icsv:
    sys.stdout.write(json.dumps(line, ensure_ascii=False) + '\n')