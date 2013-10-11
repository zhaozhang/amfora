#!/bin/bash

cp -r examples/WordCount/data /tmp/amfora/
src/amc.py scatter /tmp/amfora/data

mkdir /tmp/amfora/temp
for file in `ls /tmp/amfora/data`
do
   src/amc.py queue "wc -w /tmp/amfora/data/${file} | -d ' ' -f 1> /tmp/amfora/temp/${file}"
done

cat /tmp/amfora/temp/* | awk '{sum+=$1} END{print sum}'
