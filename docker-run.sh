#!/usr/bin/env bash
image="$1"
# get rest parameters
shift

docker run --rm\
  -v "`echo $HOME`/.aws:/home/notroot/.aws"\
  "$image"\
  "$@"
