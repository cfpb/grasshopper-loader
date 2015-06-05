#!/usr/bin/env bash
find $1 -type f -iname "tl_2014_$2*.zip" -print0 | while IFS= read -r -d $'\0' file;
do
  ./grasshopper-loader.js -d "${file}" -t tiger --index census --type addrfeat -h awsdevhmdal08 -l error
done
