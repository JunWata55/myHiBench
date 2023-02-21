#!/bin/bash

killpstree(){
    local children=`ps --ppid $1 --no-heading | awk '{ print $1 }'`
    for child in $children
    do
        killpstree $child
    done
    # kill $1
    echo $1
}

killpstree $1