#!/bin/bash
set -e
armadactl submit examples/spark-pi-driver.yaml >& /tmp/jobid.txt
export JOB_ID=`cat /tmp/jobid.txt | awk  '{print $5}'`
cat /tmp/jobid.txt
echo waiting for SparkPi driver to start: `date`
sleep 20
export IP_ADDR=`kubectl get pod "armada-$JOB_ID-0" -o jsonpath='{.status.podIP}'`
echo driver has ip address: $IP_ADDR
envsubst < examples/spark-pi-executor.yaml > /tmp/ex.yaml
echo starting executor
armadactl submit /tmp/ex.yaml
echo SparkPi executor started