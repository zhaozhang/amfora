#!/bin/bash
### deploy-amfora.sh will deploy amfora source code on all hosts
### listed in amfora.conf on the path of ${AMFORA_HOME}
###

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

tar cf amfora.tar bin etc examples LICENSE README.md src

localhost=`hostname`
localip=`nslookup ${localhost} | tail -n 2 | head -n 1 | cut -d ' ' -f 2`
for line in `grep -v '^\#' ${1}` 
do
    host=`echo ${line}| cut -d ':' -f 1`
    port=`echo ${line}| cut -d ':' -f 2`
    if [ ${host} = ${localip} ]; then
	continue
    fi
    echo "deploying amfora on ${host}"
    ssh ${host} "mkdir -p ${AMFORA_HOME}"
    scp amfora.tar ${host}:${AMFORA_HOME}/
    ssh ${host} "cd ${AMFORA_HOME}; tar xf amfora.tar"
done


