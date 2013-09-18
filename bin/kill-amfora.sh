#!/bin/bash
### kill-amfora.sh will kill the amfora processes on all hosts
### listed in amfora.conf on the path of ${AMFORA_HOME},
### the pid is recorded on each host in a file, a kill runs
### Linux kill cmd with -9 option

if [ $# -lt 1 ]; then
    echo "wrong format"
    echo "format: ${0} <amfora.conf>"
    echo "example: ${0} etc/amfora.conf"
    exit
fi

file=${1}

if [ "${AMFORA_HOME}" = "" ];then
    echo "Environment variable AMFORA_HOME is not defined"
    exit
fi

for line in `grep -v '^\#' ${1}` 
do
    host=`echo ${line} | cut -d ':' -f 1`
    echo "killing amfora on ${host}"
    ssh ${host} "cd ${AMFORA_HOME};kill -9 `cat pid`"
done


