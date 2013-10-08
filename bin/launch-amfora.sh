#!/bin/bash
### launch-amfora.sh will launch amfora on one host
### in background, pid is stored in file ./pid
### 
### 

if [ $# -lt 4 ]; then
    echo "wrong format"
    echo "format: ${0} </mount/point> <amfora.conf> <hostip> <AMFORA_HOME>"
    echo "example: ${0} /tmp/amfora etc/amfora.conf 192.168.0.1 ${AMFORA_HOME}"
    exit
fi


mount=${1}
file=${2}
ip=${3}
export AMFORA_HOME=${4}

if [ "${AMFORA_HOME}" = "" ];then
    echo "Environment variable AMFORA_HOME is not defined"
    exit
fi

pwd=`pwd`
cd ${AMFORA_HOME}
src/amfora.py ${mount} ${file} ${ip} >> /tmp/amfora.log 2>&1 &
echo $! > pid

cd $pwd

