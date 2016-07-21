#!/bin/sh

for i in 1 2 3
do
  python run_query.py --spark-master spark://$MASTER:7077 -q 3c --num-trial 1 
  sleep 10
done

