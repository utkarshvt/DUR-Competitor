#!/bin/bash

cli=0
step=3

file=$1

while [ $cli -le 60 ]
do
	grep "ClientId $cli " $file > Tid_$cli
	cli=$(($cli + $step))
done
