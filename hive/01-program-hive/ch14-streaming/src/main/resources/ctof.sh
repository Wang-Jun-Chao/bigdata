#!/bin/bash

while read LINE
do
    result=$(echo "scale=2;((9/5) * ${LINE}) + 32" | bc)
    echo "${result}"
done
