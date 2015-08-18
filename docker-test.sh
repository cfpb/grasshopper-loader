#!/usr/bin/env bash
docker run --rm --entrypoint='npm'\
  -v "`echo $HOME`/.aws:/home/notroot/.aws"\
  $1\
  test
