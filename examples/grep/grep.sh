#!/bin/bash
cp -r example/grep/data /tmp/amfora
amc.py scatter /tmp/amfora/data

touch /tmp/amfora/temp
for file in `ls /tmp/amfora/data`
do
    amc.py queue grep hello /tmp/amfora/data/${file} > /tmp/amfora/temp/${file}
done
amc.py execute

cat /tmp/amfora/temp/* > /tmp/amfora/result.txt

cp /tmp/amfora/result.txt ./result.txt