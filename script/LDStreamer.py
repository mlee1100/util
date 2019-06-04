import csv
import sys
import errno

input_settings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    )

icsv = csv.DictReader(sys.stdin, **input_settings)
ocsv = csv.DictWriter(sys.stdout, fieldnames=icsv.fieldnames, **input_settings)

try:
    ocsv.writeheader()
    for line in icsv:
        if line['b2b_email']:
            ocsv.writerow(line)

except IOError as e:
    if e.errno == errno.EPIPE:
        pass