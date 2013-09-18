#!/bin/bash
### remove-amfora.sh will remove amfora installation on all hosts
### listed in amfora.conf on the path of ${AMFORA_HOME}, except
### localhost

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

localhost=`hostname`
localip=`nslookup ${localhost} | tail -n 2 | head -n 1 | cut -d ' ' -f 2`
for line in `grep -v '^\#' ${1}` 
do
    host=`echo ${line}| cut -d ':' -f 1`
    port=`echo ${line}| cut -d ':' -f 2`
    if [ ${host} = ${localip} ]; then
	continue
    fi
    echo "removing amfora on ${host}"
    ssh ${host} "rm -rf ${AMFORA_HOME}; unset AMFORA_HOME"
done


