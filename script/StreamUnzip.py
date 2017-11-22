import smart_open
import sys

args = sys.argv[1:]
if len(args) != 1:
    raise Exception('Expected one argument (file path)')
    
input_file = args[0]


if __name__ == '__main__':

    try:
        with smart_open.smart_open(input_file,'rb') as ifile:
            for line in ifile:
                sys.stdout.write(line)

    except IOError:
        pass