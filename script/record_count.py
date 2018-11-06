import smart_open
import json
import os
import gzip
import multiprocessing

files = [
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20171208_massformat_part9_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20180106_massformat_part10_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20180522_massformat_part12_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20171108_massformat_part8_from_new_source_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people09_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people07_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people01_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people05_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people08_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people06_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people12_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people03_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people14_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people11_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people10_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people04_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people02_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people13_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people15_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people16_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people17_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20180416_massformat_part11_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/Recrawled_20m_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/final_people_data_2017_05_26_48m_notfoundsource_v15_out_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/final_people_data_2017_05_26_48m_foundsource_v15_out_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20170717_massformat_part7_v15_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20170717_massformat_part6_v20170914_oth_v15_with_400m_ids.json.gz_custom.gz',
    's3://nwd-imports/localblox/consumer/2018-10-30/people_83.5m_d_v15_with_400m_ids.json.gz_custom.gz',
    ]


def analyze(file_path):
    print 'downloading {}'.format(file_path)
    os.system('aws s3 cp {} ./'.format(file_path))
    basename = os.path.basename(file_path)
    print 'scanning file {}'.format(basename)
    counter_records = 0
    counter_lines = 0
    with gzip.open(basename, 'rb') as ifile:
        jiter = (json.loads(line) for line in ifile)
        for d in jiter:
            counter_lines += 1
            counter_records += len(d.get('mergedIdentities', []))
            # break

    os.remove(basename)
    print '{:,} lines in {}'.format(counter_lines, basename)
    print '{:,} records in {}'.format(counter_records, basename)
    return counter_lines, counter_records


if __name__ == '__main__':
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    files.reverse()
    counts = pool.map(analyze, files, chunksize=1)
    counts_lines, counts_records = zip(*counts)
    print '{:,} total lines'.format(sum(counts_lines))
    print '{:,} total records'.format(sum(counts_records))


