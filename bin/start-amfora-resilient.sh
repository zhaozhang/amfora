#!/bin/bash
### start-amfora.sh will start amfora on all hosts
### listed in amfora.conf on the path of ${AMFORA_HOME}
### the shared file system is mounted as the second 
### parameter specifies

if [ $# -lt 7 ]; then
    echo "wrong format"
    echo "format: ${0} <amfora.conf> </mount/point> <replication_factor> <MTTF> <resilience_option> <bandwidth> <latency>"
    echo "example: ${0} etc/amfora.conf /tmp/amfora 3 3600 3 10000000 0.00006"
    exit
fi

file=${1}
mount=${2}
rf=${3}
mttf=${4}
ro=${5}
bw=${6}
lt=${7}

if [ "${AMFORA_HOME}" = "" ];then
    echo "Environment variable AMFORA_HOME is not defined"
    exit
fi

for line in `grep -v '^\#' ${1}` 
do
    host=`echo ${line} | cut -d ':' -f 1`
    echo "starting amfora on ${host}"
    ssh ${host} "cd ${AMFORA_HOME}; mkdir -p ${mount}; bin/launch-amfora-resilient.sh ${mount} ${file} ${host} ${AMFORA_HOME} ${rf} ${mttf} ${ro} ${bw} ${lt}"
done

sleep 10
