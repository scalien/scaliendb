#!/bin/bash

export LD_LIBRARY_PATH=.

for i in {1..10}
do
        ./runforever ConcurrentWords 192.168.39.17:7080 &
done
