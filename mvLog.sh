#!/bin/bash

file=`ls /home/junwata/flink-1.15.0/log/*taskexecutor*.out`

mv $file $1/task$2.out