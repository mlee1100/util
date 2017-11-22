from random import randint
import sys
import os
import math
import time

reload(sys)
sys.setdefaultencoding('utf-8')

Args = sys.argv[1:]
if len(Args) == 2:
	infile, sample_size = Args
elif len(Args) == 3:
	infile, sample_size, cap = Args
else:
	print 'argument error, expecting: <file path> <sample size> [<max line cap>]'
	sys.exit(1)

sample_size = int(sample_size)

try:
	cap = int(cap)
except ValueError:
	print 'argument error: <max line cap> argument must be an integer'
	sys.exit(1)
except NameError:
	cap = float('inf')
	print 'sampling all lines...'

infile_path = os.path.abspath(infile)

start = time.time()

def get_line_estimate(path,line_sample):

	line_sample = float(line_sample)
	file_size = float(os.stat(path).st_size)
	count = 0
	total_size = float(0)

	with open(path,'rb') as ifile:
		header_size = len(ifile.next())
		while count < line_sample:
			total_size += len(ifile.next())
			count += 1

	line_size = total_size/line_sample
	percent_through_file = total_size/(file_size - header_size)
	adjust_hard = .90
	adjust = adjust_hard + (percent_through_file/(100 - adjust_hard))

	return int(((file_size - header_size)/line_size) * adjust) + 1


def convert_size(records):

	if records >= 1000000000:
		return str(int(math.floor(float(records)/1000000000))) + 'B'

	if records >= 1000000:
		return str(int(math.floor(float(records)/1000000))) + 'M'

	if records >= 1000:
		return str(int(math.floor(float(records)/1000))) + 'K'

	else:
		return str(int(math.floor(float(records)/1)))


if __name__ == '__main__':

	file_dir, file_name = os.path.split(infile_path)
	file_name_only, file_ext = os.path.splitext(file_name)

	outfile = os.path.join(
		file_dir,
		'_'.join([
			file_name_only,
			'sample',
			convert_size(sample_size)
			])
		) + file_ext
	
	lines = get_line_estimate(infile,1000)

	lines = min(lines,cap)

	sample = set()
	while len(sample) <= sample_size:
		sample.update([randint(0,lines)])

	with open(infile,'rb') as ifile, open(outfile,'wb') as ofile:
		counter = 0
		ofile.writelines([ifile.next()])
		for i, line in enumerate(ifile):
			if i in sample:
				counter += 1
				ofile.writelines([line])
				if counter >= sample_size:
					break

	print 'finished in {seconds}s'.format(seconds=int(time.time() - start))