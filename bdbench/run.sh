#!/bin/sh

#for i in $(seq 1 1 10) 
#do
#  echo "Quert-1c"
#  python run_query.py --spark-master spark://$MASTER:7077 -q 1c --num-trial 1 
#  sleep 10
#done

#for i in $(seq 1 1 15) 
#do
# echo "Quert-1a"
# python run_query.py --spark-master spark://$MASTER:7077 -q 1a --num-trial 1 
# sleep 10
#done

#for i in $(seq 1 1 15) 
#do
# echo "Quert-1b"
# python run_query.py --spark-master spark://$MASTER:7077 -q 1b --num-trial 1 
# sleep 10
#done

#for i in $(seq 1 1 15) 
#do
# python run_query.py --spark-master spark://$MASTER:7077 -q 2a --num-trial 1 
# sleep 10
#done

#for i in $(seq 1 1 15) 
#do
# python run_query.py --spark-master spark://$MASTER:7077 -q 2b --num-trial 1 
# sleep 10
#done

#for i in $(seq 1 1 15) 
#do
# python run_query.py --spark-master spark://$MASTER:7077 -q 2c --num-trial 1 
# sleep 10
#done

for i in $(seq 1 1 30) 
do
 python run_query.py --spark-master spark://10.2.3.5:7077 -q 3a
 sleep 20
done
