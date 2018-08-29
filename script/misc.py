import sys
reload(sys)
sys.setdefaultencoding('utf8')
infile = 'DSG_AutoFin_FEB2018fx.csv'
outfile = 'DSG_AutoFin_FEB2018fx2.csv'
with open(infile, 'rb') as ifile, open(outfile, 'wb') as ofile:
    for line in ifile:
        ofile.write(line.rstrip().rstrip(',,,') + '\n')




import sys
reload(sys)
sys.setdefaultencoding('utf8')
infile = 'DSG_AutoFin_FEB2018fx2.csv'
with open(infile, 'rb') as ifile:
    for line in ifile:
        if line.count(',') > 23:
            print line




import csv
f = 'email_hippo_initial_file.csv'
o = 'email_hippo_initial_file_dedup.csv'
email_set = set()
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.DictReader(ifile, delimiter=',', fieldnames=['person_id', 'email'])
    ocsv = csv.DictWriter(ofile, delimiter=',', fieldnames=['person_id', 'email'])
    for line in icsv:
        if line['email'] not in email_set:
            email_set.add(line['email'])
            ocsv.writerow(line)



import csv
f = 'email_hippo_initial_file.csv'
email_set = set()
with open(f, 'rb') as ifile:
    icsv = csv.DictReader(ifile, delimiter=',', fieldnames=['person_id', 'email'])
    for line in icsv:
        domain = line['email'].split('@')[-1]
        if domain not in email_set:
            email_set.add(domain)
        else:
            print domain

print len(email_set)



import json
f = 'zillow_profiles_complete.jl'
counts = dict()

with open(f, 'rb') as ifile:
    for line in ifile:
        if 'linkedin.com' in line.lower():
            counts['linkedin_found'] = counts.get('linkedin_found', 0) + 1
        jline = json.loads(line)
        if jline.get('links', dict()).get('linkedin', ''):
            counts['linkedin_parsed'] = counts.get('linkedin_parsed', 0) + 1


print counts



import json
f = 'zillow_profiles_complete.jl'
keys = set()
with open(f, 'rb') as ifile:
    for i, line in enumerate(ifile):
        j = json.loads(line)
        key = j.get('_key', None)
        if key:
            keys.add(key)


print '{} records'.format(i+1)
print '{} unique keys'.format(len(keys))



import csv
f = 'DOC_Netwise_20180507_ascii.txt'
source_dict = dict()
counter = 1
with open(f, 'rb') as ifile:
    icsv = csv.DictReader(ifile, quoting=csv.QUOTE_ALL, delimiter='|')
    for i, line in enumerate(icsv):
        counter += 1
        if counter == 1000000:
            print i
            counter = 0
        if line['Source'] in source_dict:
            source_dict[line['Source']]['count'] += 1
            if line['DOBNew']:
                source_dict[line['Source']]['dobs'] += 1
        else:
            source_dict[line['Source']] = dict(count=1)
            if line['DOBNew']:
                source_dict[line['Source']]['dobs'] = 1
            else:
                source_dict[line['Source']]['dobs'] = 0


for sc in sorted(source_dict.keys()):
    print '{}|{:,}|{:,}'.format(sc, source_dict[sc]['count'], source_dict[sc]['dobs'])





# ----------

import json
import os
import hashlib
files = [
    'people_with_email_TN_additional_verified_20180613.json',
    'people_with_email_TN_unverified_20180613.json',
    'people_with_email_TN_verified_20180613.json',
    ]

for file in files:
    outfile = os.path.splitext(file)[0] + '.csv'
    with open(file, 'rb') as ifile, open(outfile, 'wb') as ofile:
        for i, line in enumerate(ifile):
            emails = set()
            j = json.loads(line)
            if 'mainEmail' in j:
                emails.add(j['mainEmail'])
            if 'otherEmails' in j:
                emails.update(j['otherEmails'])
            for email in emails:
                ofile.write(hashlib.md5(email.strip().lower()).hexdigest() + '\n')
            # if i % 1000000 == 0:
            #     print i


# ------

import json
import os
import hashlib
files = [
    'people_with_email_TN_additional_verified_20180613.json',
    # 'people_with_email_TN_unverified_20180613.json',
    # 'people_with_email_TN_verified_20180613.json',
    ]

for file in files:
    with open(file, 'rb') as ifile:
        for i, line in enumerate(ifile):
            j = json.loads(line)
            if 'mainEmail' not in j and 'otherEmails' not in j:
                print line
                break




# ------
f = 'liveramp_matchback_20180621.csv'
o = 'liveramp_matchback_20180621.formatted.csv'
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    ofile.write('email_MD5|file_wide_field\n')
    for line in ifile:
        ofile.write(line.strip() + '|1\n')



# -----

import csv
import os
import hashlib
f = '53860_netwise_uscon_test.txt'
with open(f, 'rb') as ifile, open(os.path.splitext(f)[0] + '.csv', 'wb') as ofile:
    icsv = csv.DictReader(ifile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    for line in icsv:
        for i in range(3):
            if line['email_address_{}'.format(i+1)].strip():
                ofile.write(hashlib.md5(line['email_address_{}'.format(i+1)].strip().lower()).hexdigest() + '\n')


# -----

import csv
infile = 'premover-v4-20180717.tab'
outfile = 'premover-v4-20180717.csv'

insettings = dict(
    delimiter = '\t',
    quoting = csv.QUOTE_NONE,
    escapechar = '\\',
    doublequote = False,
    )

outsettings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_ALL,
    escapechar = '\\',
    doublequote = False,
    )

zips = set([
    '30309',
    '45414',
    '55443',
    '35487',
    '95476',
    '33427',
    '07040',
    '89106',
    '85034',
    '90001',
    '60606',
    '10128',
    '75201',
    '48201',
    '98101',
    ])

zip_counts = dict()
status_counts = dict()

with open(infile, 'rb') as ifile, open(outfile, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **insettings)
    ocsv = csv.DictWriter(ofile, fieldnames=icsv.fieldnames, **outsettings)
    ocsv.writeheader()
    for i, line in enumerate(icsv):
        if i % 100000 == 0:
            print i
        if line['zip'] in zips:
            # record_counts[line['zip']] = record_counts.get(line['zip'], 0) + 1
            # status_counts[line['status']] = status_counts.get(line['status'], 0) + 1
            ocsv.writerow(line)


print record_counts
print status_counts







import json

files = [
    'nw_li_comp_01.json',
    'nw_li_comp_02.json',
    'nw_li_comp_03.json',
    'nw_li_comp_04.json',
    'nw_li_comp_05.json',
    'nw_li_comp_06.json',
    'nw_li_comp_07.json',
    'nw_li_comp_08.json',
    'nw_li_comp_09.json',
    'nw_li_comp_10.json',
    'nw_li_comp_10.json',
    'nw_li_comp_11.json',
    'nw_li_comp_12.json',
    'nw_li_comp_13.json',
    'nw_li_comp_14.json',
    'nw_li_comp_15.json',
    'nw_li_comp_16.json',
    ]


records = 0
founded = 0

for file in files:
    with open(file, 'rb') as ifile:
        for line in ifile:
            j = json.loads(line)
            records += 1
            # if int(j.get('founded', 0)):
            if 1500 <= int(j.get('founded', 0)) <= 2018:
                founded += 1
    print '{:,} / {:,}'.format(founded, records)


print founded
print records






import csv
import tldextract

def get_domain(string):
    if not string:
        return None
    parsed_url = tldextract.extract(string.strip().lower())
    if parsed_url.domain and parsed_url.suffix:
        return '.'.join([parsed_url.domain, parsed_url.suffix])
    print string
    return None

isp_file = 'bombora_ip_domain_append_v2.csv'
isp_outfile = 'db_bombora_ip_domain_append_v2.csv'

fieldnames = (
    'isISP',
    'ipAddress',
    'probability',
    'companyName',
    'websiteURL',
    'hqAddress1',
    'hqAddress2',
    'hqCity',
    'hqStateProvReg',
    'hqZip',
    'hqCountry',
    'parentCompany',
    'parentAddress',
    'parentCity',
    'parentStateProvReg',
    'parentZIP',
    'parentCountry',
    'phone',
    'industry',
    'orgWatch',
    'revenue',
    'employees',
    'stockExchange',
    'tickerSymbol',
    'domain',
    'provider_record_id',
    )



icsv_settings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_MINIMAL,
    )

ocsv_settings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # escapechar = '\\',
    escapechar = None,
    doublequote = True,
    fieldnames = fieldnames,
    restval = None,
    lineterminator = '\n'
    )

def escape(dict_):
    return {k: v.replace('\\', '\\\\') for k, v in dict_.iteritems()}

with open(isp_file, 'rb') as ifile, open(isp_outfile, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **icsv_settings)
    ocsv = csv.DictWriter(ofile, **ocsv_settings)
    ocsv.writeheader()
    for line in icsv:
        # line = escape(line)
        try:
            line['domain'] = get_domain(line['websiteURL'])
            line['isISP'] = (1 if line['isISP'] == 'TRUE' else 0)
            if line['probability']:
                line['probability'] = float(line['probability'])
            else:
                line['probability'] = None
            ocsv.writerow(line)
        except:
            print line
            raise


