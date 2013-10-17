#!/usr/bin/env python3

import os
import sys

images = dict()
center = dict()
parameters = dict()
imagetbl=sys.argv[1]
corrtbl =sys.argv[2]
binary = "/home/zhaozhang/Montage/Montage_v3.3/bin/mBackground"
mount = "/tmp/amfora"

f = open(imagetbl, "r")
line = f.readline()
line = f.readline()
line = f.readline()
while True:
    line = f.readline()
    if not line:
        break

    parselist = line.split()
    images[parselist[0]]=parselist[23]

g = open(sys.argv[2], "r")
line = g.readline()
while True:
    line = g.readline()
    if not line:
        break
    parselist = line.split()
    par = parselist[1] + " " +parselist[2] + " " + parselist[3]
    parameters[parselist[0]]=par
    inf = images[parselist[0]]
    basename = os.path.basename(inf)
    outf = mount+"/projdir/"+basename
    out = mount+"/corrdir/"+basename
    print("%s %s %s %s" % (binary, outf, out, par))

