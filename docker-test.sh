#!/usr/bin/env bash
docker run --rm --link $2:elasticsearch --entrypoint='npm'\
  -v "`echo $HOME`/.aws:/home/notroot/.aws"\
  $1\
  test
