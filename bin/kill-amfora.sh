#!/bin/bash
### kill-amfora.sh will kill the amfora processes on all hosts
### listed in amfora.conf on the path of ${AMFORA_HOME},
### the pid is recorded on each host in a file, a kill runs
### Linux kill cmd with -9 option

if [ $# -lt 2 ]; then
    echo "wrong format"
    echo "format: ${0} <amfora.conf> <mountpoint>"
    echo "example: ${0} etc/amfora.conf /tmp/amfora"
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
    echo "killing amfora on ${host}"
    pid=`ssh ${host} "cd ${AMFORA_HOME};cat pid"`
    ssh ${host} "kill -9 ${pid}; fusermount -u ${mount}"
done


