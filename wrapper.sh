#!/bin/sh
# An example of worker used with the wrapper
set -xue
# obj="URL='http://site.com/video1.avi';AVI='video1.avi';MP4='video1.mp4'"
obj=$1
eval $obj

wget -c -O $AVI $URL

