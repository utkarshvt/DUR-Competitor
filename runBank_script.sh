#!/bin/bash

#cd /proj/HyflowTM/
cd /proj/HyflowTM/utkarsh/tw_code/competitor/DUR_with_locks/
screen -dm -S dup_bank ./bankServer.sh $*
#screen -dm -S testing ./busy_script.sh $1
#echo "Replica: $1" | tee -a $1.txt
exit
