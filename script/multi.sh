#!/bin/bash

COUNTER=$1
shift
while [ $COUNTER -gt 0 ];
do
	$* &
	let COUNTER=COUNTER-1
done
