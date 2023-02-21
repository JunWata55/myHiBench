#!/bin/bash
#
#pids=(`ps -e -o pid,cmd | grep taskexecutor | grep -v grep | awk '{print $1}'`)
#
#echo the fetched taskmanager pids are:
#echo -e "\t${pids[@]}"
#
#for pid in ${pids[@]}
#do
#	echo $pid
#done
../stop.sh 3
../start.sh 3
