#!/bin/bash

export LD_LIBRARY_PATH=.

for i in {1..10}
do
        ./runforever $* &
done
