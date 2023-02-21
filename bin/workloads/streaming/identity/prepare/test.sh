#!/bin/bash
echo dirname ${BASH_SOURCE:-$0}
echo $(cd $(dirname ${BASH_SOURCE:-$0}); pwd)


this="${BASH_SOURCE-$0}"
echo $this
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
echo $bin
script="$(basename -- "$this")"
echo $script
