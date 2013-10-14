#!/bin/bash

#This is simplied version of PageRank. This example source code did not apply the dumping factor of 0.85 to the results. This example source code assumes all pages are cited in other pages.

cp -r examples/PageRank/data /tmp/amfora/
src/amc.py scatter /tmp/amfora/data

cp examples/PageRank/score.txt /tmp/amfora/
src/amc.py multicast /tmp/amfora/score.txt

mkdir /tmp/amfora/temp
for file in `ls /tmp/amfora/data`
do
   src/amc.py queue "examples/PageRank/bin/distribution.py /tmp/amfora/data/${file} /tmp/amfora/data/score.txt > /tmp/amfora/temp/${file}"
done
src/amc.py execute

mkdir /tmp/amfora/target
src/amc.py shuffle /tmp/amfora/temp /tmp/amfora/target

mkdir /tmp/amfora/result
for file in `ls /tmp/amfora/target`
do
   src/amc.py queue "examples/PageRank/bin/addition.py /tmp/amfora/target/${file} > /tmp/amfora/result/${file}"
done
src/amc.py execute

cat /tmp/amfora/result/* | sort -nrk 1 > /tmp/amfora/new-score.txt
