#!/bin/bash

IFS=$'\n'
for line in `ps -efo pid,cmd | grep DataGen | grep -v grep | awk '{ print $1 }'`
do
echo $line
kill $line
done
