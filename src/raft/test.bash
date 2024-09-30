#!/bin/bash
for i in $(seq 100)
do
    go test -run 3B >> 3B2-100.txt
done