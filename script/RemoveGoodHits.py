import csv
import argparse


parser = argparse.ArgumentParser(description='Generate Full Contact export files')
parser.add_argument('-i','--input_file_path', required=True, type=str, help='path to incoming file')
parser.add_argument('-g','--good_hits_file_path', required=True, type=str, help='path to file with good hits to remove from incoming')
parser.add_argument('-o','--output_file_path', required=True, type=str, help='path to output file')
parser.add_argument('-c','--id_column', required=False, type=str, default='searchId', help='name of column that has the primary id\'s')
args = parser.parse_args()

csv_settings = dict(
    delimiter = ',',
    )


def get_good_hit_ids(file_path,csv_settings):

    output_set = set()

    with open(file_path,'rb') as gfile:
        gcsv = csv.reader(gfile,**csv_settings)
        for line in gcsv:
            try:
                if line:
                    output_set.update([int(line[0].strip())])
            except:
                print line
                raise
            # output_set = set([int(l[0].strip()) for l in gcsv])

    return output_set


if __name__ == '__main__':

    records = dict()

    good_hits = get_good_hit_ids(args.good_hits_file_path,csv_settings)

    records['good'] = len(good_hits)

    with open(args.input_file_path,'rb') as ifile, open(args.output_file_path,'wb') as ofile:
        icsv = csv.DictReader(ifile,**csv_settings)
        ocsv = csv.DictWriter(ofile,icsv.fieldnames,**csv_settings)
        ocsv.writeheader()
        for line in icsv:
            records['input'] = records.get('input',0) + 1
            if int(line[args.id_column].strip()) not in good_hits:
                records['output'] = records.get('output',0) + 1
                ocsv.writerow(line)


    records['removed'] = records['input'] - records['output']
    
    for key, value in records.iteritems():
        print key + ': ' + str(value)