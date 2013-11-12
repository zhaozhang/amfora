#!/usr/bin/env python3

import os
import sys
import subprocess

if __name__=="__main__":
    if len(sys.argv) < 3:
        print("wrong format\n")
        print("Format: "+sys.argv[0]+" link.txt score.txt")
        sys.exit(1)
    lname = sys.argv[1] 
    sname = sys.argv[2]
    if not os.path.isfile(lname):
        print("Link file "+lname+" does not exist")
        sys.exit(1)
    if not os.path.isfile(sname):    
        print("Score file "+sname+" does not exist")
        sys.exit(1)
    
    sdict = dict()    
    sfd = open(sname, 'r')
    lines = sfd.readlines()
    for line in lines:
        pageid, score = line.strip('\n').split('\t')
        sdict[pageid] = score

    fd = open(lname, 'r')
    lines = fd.readlines()
    for line in lines:
        words = line.strip('\n').split(' ')
        pageid = words[0][:len(words[0])-1]
        for page in words[1:]:
            print("%s\t%.8f" % (page, float(sdict[pageid])/(len(words)-1)))
            #print(page+'\t'+str(float(sdict[pageid])/(len(words)-1)))

        
