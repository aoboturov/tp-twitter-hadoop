import fileinput
import sys

for line in fileinput.input():
    if fileinput.lineno() == 1:
        continue
    if not line.isspace():
        line_split = line.split('\t')
        sys.stdout.write(line_split[1].rstrip('\n'))
        if line_split[0] == 'W':
            sys.stdout.write('\n')
        else:
            sys.stdout.write('\t')
