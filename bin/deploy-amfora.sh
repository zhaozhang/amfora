#!/bin/bash
### deploy-amfora.sh will deploy amfora source code on all hosts
### listed in sites.list on the path of ${AMFORA_HOME}
###

if [ $# -lt 1 ]; then
    echo "wrong format"
    echo "format: ${0} <sites.list>"
    echo "example: ${0} etc/sites.list"
    exit
fi

file=${1}
path=`echo ${AMFORA_HOME}`
if [ "${path}" = "" ];then
    echo "Environment variable AMFORA_HOME is not defined"
    exit
fi

localhost=`hostname -i`
grep -v '^\#' ${1} | while read host
do
    if [ ${host} = ${localhost} ]; then
	continue
    fi
    echo ssh ${host} 'mkdir -p ${path}'
    echo scp amfora.tar ${host}:${path}/
    echo ssh ${host} 'cd ${path}; tar xf amfora.tar'
done

#export AMFORA_HOME=${path}
#export PATH=${PATH}:${AMFORA_HOME}/bin

