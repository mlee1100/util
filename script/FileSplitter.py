import argparse
import gzip
import os

parser = argparse.ArgumentParser()
parser.add_argument("files", nargs='*', help="files to split")
parser.add_argument("-o", "--output_prefix", type=str, help='output prefix')
parser.add_argument("-s", "--output_suffix", type=str, help='output suffix')
parser.add_argument("-b", "--byte_limit", type=int, default=None, help='bytes to split at')
parser.add_argument("-l", "--line_limit", type=int, default=None, help='lines to split at')
parser.add_argument("-z", "--zfill", type=int, default=1, help='enumerator zero fill length')
parser.add_argument("-r", "--header", action='store_true', help='retain header')
parser.add_argument("-c", "--compress", action='store_true', help='gzip compress result')
args = parser.parse_args()

assert args.byte_limit or args.line_limit, 'must choose a byte or line limit'

fileno = 0

def get_new_output(openfile=None):
    if openfile:
        openfile.close()
    global fileno
    fileno += 1
    # file_path = f'{args.output_prefix}{str(fileno).zfill(args.zfill)}{args.output_suffix}'
    file_path = '{}{}{}'.format(args.output_prefix, str(fileno).zfill(args.zfill), args.output_suffix)
    if args.compress:
        return gzip.open(file_path, 'wb')
    else:
        return open(file_path, 'wb')

ofile = get_new_output()
header = None
current_line_count = 0
for filename in args.files:
    if filename.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open
    with opener(filename, 'rb') as ifile:
        if args.header:
            new_header = next(ifile)
            if header and new_header != header:
                ofile = get_new_output()
                header = new_header
                ofile.write(header)
            elif new_header != header:
                header = new_header
                ofile.write(header)
        for line in ifile:
            ofile.write(line)
            current_line_count += 1
            if (args.byte_limit and ofile.tell() >= args.byte_limit) or (args.line_limit and current_line_count >= args.line_limit):
                ofile = get_new_output(ofile)
                current_line_count = 0
                if args.header:
                    ofile.write(header)

ofile.flush()
final_file_name = ofile.name
ofile.close()
if not current_line_count:
    os.remove(final_file_name)