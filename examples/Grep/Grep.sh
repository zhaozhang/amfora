#!/bin/bash
cp -r examples/Grep/data /tmp/amfora
amc.py scatter /tmp/amfora/data

mkdir /tmp/amfora/temp
for file in `ls /tmp/amfora/data`
do
    src/amc.py queue "grep granted /tmp/amfora/data/${file} > /tmp/amfora/temp/${file}
done"
src/amc.py execute

cat /tmp/amfora/temp/* > /tmp/amfora/result.txt
cat /tmp/amfora/result.txt
