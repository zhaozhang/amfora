#!/bin/bash
### start-amfora.sh will start amfora on all hosts
### listed in amfora.conf on the path of ${AMFORA_HOME}
### the shared file system is mounted as the second 
### parameter specifies

if [ $# -lt 2 ]; then
    echo "wrong format"
    echo "format: ${0} <amfora.conf> </mount/point>"
    echo "example: ${0} etc/amfora.conf./tmp/amfora"
    exit
fi

file=${1}
mount=${2}

if [ "${AMFORA_HOME}" = "" ];then
    echo "Environment variable AMFORA_HOME is not defined"
    exit
fi

for line in `grep -v '^\#' ${1}` 
do
    host=`echo ${line} | cut -d ':' -f 1`
    echo "starting amfora on ${host}"
    echo ssh ${host} "cd ${AMFORA_HOME}; bin/launch-amfora.sh ${mount} etc/amfora.conf ${host} ${AMFORA_HOME}"
done


