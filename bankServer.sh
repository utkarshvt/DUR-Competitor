#!/bin/bash

echo "Params : $*"
#killall -9 java
#java -Xms42048m -Xmx84096m -ea -Djava.util.logging.config.file=logging.properties -cp lib/kryo-2.24.0.jar:lib/minlog-1.2.jar:lib/objenesis-1.2.jar:lib/reflectasm-1.09-shaded.jar:ppaxos.jar stm.benchmark.bank.BankServer $*

java -Xms102400m -Xmx116122m -XX:+UseParallelOldGC -ea -Djava.util.logging.config.file=logging.properties -cp lib/kryo-3.0.1.jar:lib/minlog-1.2.jar:lib/objenesis-1.2.jar:lib/reflectasm-1.09-shaded.jar:/lib/mockito-all-1.8.5.jar:ppaxos.jar stm.benchmark.bank.BankServer $1 $2 $3 $4 $5 $6 | tee -a testlogs/$1_$5_1.txt

