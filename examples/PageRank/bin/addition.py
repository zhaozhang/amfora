#!/usr/bin/env python3

import os
import sys
import subprocess

if __name__=="__main__":
    if len(sys.argv) < 2:
        print("wrong format\n")
        print("Format: "+sys.argv[0]+" key-value.txt")
        sys.exit(1)
    fname = sys.argv[1] 
    if not os.path.isfile(fname):
        print("Key-value pair file "+fname+" does not exist")
        sys.exit(1)
    
    kvdict = dict()    
    fd = open(fname, 'r')
    lines = fd.readlines()
    for line in lines:
        pageid, score = line.strip('\n').split('\t')
        if pageid not in kvdict:
            kvdict[pageid] = []
        kvdict[pageid].append(float(score))    

    for k in kvdict:
        print("%s\t%.8f" % ( k, sum(kvdict[k])))

        
