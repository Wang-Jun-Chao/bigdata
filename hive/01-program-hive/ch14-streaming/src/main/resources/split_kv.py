#!/usr/bin/python3

import sys

for line in sys.stdin.readlines():
    line = line.strip()
    if line != '':
        kvs = line.split(",")
        for p in kvs:
            kv = p.split("=")
            sys.stdout.write(kv[0] + "\t" + kv[1] + "\n")
