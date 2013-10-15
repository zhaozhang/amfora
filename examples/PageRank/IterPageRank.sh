#!/bin/bash

#This is simplied version of PageRank. This example source code did not apply the dumping factor of 0.85 to the results. This example source code assumes all pages are cited in other pages.

cp -r examples/PageRank/data /tmp/amfora/
src/amc.py scatter /tmp/amfora/data
cp examples/PageRank/score.txt /tmp/amfora/score-0.txt

ID=0
Converge=1

while [ ${Converge} == 1 ]
do
    src/amc.py multicast /tmp/amfora/score-${ID}.txt

    mkdir /tmp/amfora/temp-${ID}
    for file in `ls /tmp/amfora/data`
    do
	src/amc.py queue "examples/PageRank/bin/distribution.py /tmp/amfora/data/${file} /tmp/amfora/score-${ID}.txt > /tmp/amfora/temp-${ID}/${file}"
    done
    src/amc.py execute

    mkdir /tmp/amfora/target-${ID}
    src/amc.py shuffle /tmp/amfora/temp-${ID} /tmp/amfora/target-${ID}

    mkdir /tmp/amfora/result-${ID}
    for file in `ls /tmp/amfora/target-${ID}`
    do
	src/amc.py queue "examples/PageRank/bin/addition.py /tmp/amfora/target-${ID}/${file} > /tmp/amfora/result-${ID}/${file}"
    done
    src/amc.py execute

    newID=`expr ${ID} + 1`	
    cat /tmp/amfora/result-${ID}/* | sort -nk 1 > /tmp/amfora/score-${newID}.txt
    
    diff /tmp/amfora/score-${ID}.txt /tmp/amfora/score-${newID}.txt
    Converge=$?
    ID=`expr ${ID} + 1`
done
