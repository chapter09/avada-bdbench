# bdbench_spark_sql
Scripts for running Big Data Benchmark on existing Spark SQL cluster

### Prepare the dataset from Amazon S3
    python prepare_benchmark.py --spark-master spark://SPARK_HOME:7077 -n 1
  
### Run the benchmark scripts and collect monitoring data
    
