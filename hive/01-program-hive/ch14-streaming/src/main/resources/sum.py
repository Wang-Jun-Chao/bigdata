#!/usr/bin/python3
import sys

total = 0
for line in sys.stdin.readlines():
    line = line.strip()
    if line != '':
        total += int(line)

sys.stdout.write(str(total))
