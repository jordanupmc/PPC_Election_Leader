#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage $0 <number of term to launch>"
    exit 1
fi

i=0
while [ $i -lt $1 ];do
    gnome-terminal -e sbt &
    ((i+=1))
done

exit 0
