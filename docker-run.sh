#!/usr/bin/env bash
image="$1"
elasticsearch="$2"
# get rest parameters
shift
shift

docker run --rm --link "$elasticsearch":elasticsearch\ 
  -v "`echo $HOME`/.aws:/home/notroot/.aws"\
  "$image"\
  "$@"
