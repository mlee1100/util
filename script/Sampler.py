from random import randint
import sys
import os
import math
import time
import glob
import smart_open

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

infile_paths = [os.path.abspath(p) for p in glob.glob(infile)]

start = time.time()

def get_line_estimate(path,line_sample):

	line_sample = float(line_sample)
	file_size = float(os.stat(path).st_size)
	count = 0
	total_size = float(0)

	with smart_open.smart_open(path,'rb') as ifile:
		header_size = len(ifile.next())
		while count < line_sample:
			try:
				total_size += len(ifile.next())
				count += 1
			except StopIteration:
				break

	line_size = total_size/line_sample
	percent_through_file = total_size/(file_size - header_size)
	adjust_hard = .90
	adjust = adjust_hard + (percent_through_file/(100 - adjust_hard))

	if line_size == 0.0:
		line_size = header_size

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

	for infile_path in infile_paths:
		print 'sampling {}...'.format(infile_path)
		file_dir, file_name = os.path.split(infile_path)
		if file_name.endswith('.gz'):
			file_name = file_name.rpartition('.gz')[0]
		file_name_only, file_ext = os.path.splitext(file_name)

		outfile = os.path.join(
			file_dir,
			'_'.join([
				file_name_only,
				'sample',
				convert_size(sample_size)
				])
			) + file_ext
		
		lines = get_line_estimate(infile_path,1000)

		lines = max(min(lines,cap), 1)

		sample = set()
		while len(sample) <= sample_size:
			sample.add(randint(0,lines-1))
			if len(sample) == lines:
				sample.add(lines)
				lines += 1

		with smart_open.smart_open(infile_path,'rb') as ifile, open(outfile,'wb') as ofile:
			counter = 0
			ofile.writelines([ifile.next()])
			for i, line in enumerate(ifile):
				if i in sample:
					counter += 1
					ofile.writelines([line])
					if counter >= sample_size:
						break

	print 'finished in {seconds}s'.format(seconds=int(time.time() - start))