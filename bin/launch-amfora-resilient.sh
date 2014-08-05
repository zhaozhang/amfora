#!/bin/bash
### launch-amfora.sh will launch amfora on one host
### in background, pid is stored in file ./pid
### 
### 

if [ $# -lt 9 ]; then
    echo "wrong format"
    echo "format: ${0} </mount/point> <amfora.conf> <hostip> <AMFORA_HOME> <replication_factor> <MTTF> <resilience_option> <bandwidth> <latency>"
    echo "example: ${0} /tmp/amfora etc/amfora.conf 192.168.0.1 ${AMFORA_HOME} 3 3600 3 10000000 0.00006"
    exit
fi


mount=${1}
file=${2}
ip=${3}
export AMFORA_HOME=${4}
rf=${5}
mttf=${6}
ro=${7}
bw=${8}
lt=${9}

if [ "${AMFORA_HOME}" = "" ];then
    echo "Environment variable AMFORA_HOME is not defined"
    exit
fi

pwd=`pwd`
cd ${AMFORA_HOME}/
src/amfora-resilient.py ${mount} ${file} ${ip} ${rf} ${mttf} ${ro} ${bw} ${lt} > /tmp/amfora.log 2>&1 &
echo $! > pid

cd ${pwd}

