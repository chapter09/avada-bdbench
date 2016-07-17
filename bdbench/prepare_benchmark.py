#!/bin/python

import subprocess
import sys
from sys import stderr
from argparse import ArgumentParser
import os
import time


SCALE_FACTOR_MAP = {
		0: "tiny",
		1: "1node",
		5: "5nodes",
		10: "10nodes"
		}


def parse_args():
	parser = ArgumentParser(usage="prepare_benchmark.py [options]")

	parser.add_argument("--hadoop", dest="hdfs",
			default=os.environ.get("HADOOP_HOME"), help="Path of HADOOP_HOME")
	parser.add_argument("--spark",
			default=os.environ.get("SPARK_HOME"), help="Path of SPARK_HOME")
	parser.add_argument("--spark-master",
			default=os.environ.get("SPARK_MASTER"), help="Address of Sparl master")

	parser.add_argument("-n", "--scale-factor", type=int, default=5,
			help="Number of database nodes (dataset is scaled accordingly)")
	parser.add_argument("-f", "--file-format", default="text",
			help="File format to copy (text, text-deflate, "
			"sequence, or sequence-snappy)")

	parser.add_argument("-d", "--aws-key-id",
			help="Access key ID for AWS")
	parser.add_argument("-k", "--aws-key",
			help="Access key for AWS")

	parser.add_argument("--skip-s3-import", action="store_true", default=False,
			help="Assumes s3 data is already loaded")

	opts = parser.parse_args()

	if opts.scale_factor not in SCALE_FACTOR_MAP.keys():
		print >> stderr, "Unsupported cluster size: %s" % opts.scale_factor
		sys.exit(1)

	opts.data_prefix = SCALE_FACTOR_MAP[opts.scale_factor]

	# checking parameters
	if opts.hdfs is None or opts.spark is None:
		print >> stderr, \
				"The script requires HADOOP_HOME and SPARK_HOME"
		sys.exit(1)

	return opts


def cmd(command):
	subprocess.check_call(command, shell=True)


def prepare_spark_sql(opts):

	if not opts.skip_s3_import:
		print "=== IMPORTING BENCHMARK DATA FROM S3 ==="
		try:
			cmd("%s/bin/hdfs dfs -mkdir /user/shark/benchmark", opts.hdfs)
		except Exception:
			pass  # Folder may already exist        
		
		cmd("%s/bin/hadoop distcp "
			"s3n://big-data-benchmark/pavlo/%s/%s/rankings/ "
				"/user/shark/benchmark/" % (
					opts.hdfs, opts.file_format, opts.data_prefix))

				cmd("%s/bin/hadoop distcp "
						"s3n://big-data-benchmark/pavlo/%s/%s/crawl/ "
						"/user/shark/benchmark/" % (
							opts.hdfs, opts.file_format, opts.data_prefix))

						cmd("%s/bin/hadoop distcp "
								"s3n://big-data-benchmark/pavlo/%s/%s/uservisits/ "
								"/user/shark/benchmark/" % (
									opts.hdfs, opts.file_format, opts.data_prefix))

								# Scratch table used for JVM warmup
		cmd("%s/bin/hadoop distcp /user/shark/benchmark/rankings "
				"/user/shark/benchmark/" % opts.hdfs)

		print "=== CREATING HIVE TABLES FOR BENCHMARK ==="

	cmd("%s/bin/spark-sql --master %s -e \"DROP TABLE IF EXISTS rankings; "
			"CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, "
			"avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" "
			"STORED AS TEXTFILE LOCATION \\\"/user/shark/benchmark/rankings\\\";\"" %
			(opts.spark, opts.spark_master))

	cmd("%s/bin/spark-sql --master %s -e \"DROP TABLE IF EXISTS scratch; "
			"CREATE EXTERNAL TABLE scratch (pageURL STRING, pageRank INT, "
			"avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" "
			"STORED AS TEXTFILE LOCATION \\\"/user/shark/benchmark/scratch\\\";\"" %
			(opts.spark, opts.spark_master))

	cmd("%s/bin/spark-sql --master %s -e \"DROP TABLE IF EXISTS uservisits; "
			"CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,"
			"visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,"
			"languageCode STRING,searchWord STRING,duration INT ) "
			"ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" "
			"STORED AS TEXTFILE LOCATION \\\"/user/shark/benchmark/uservisits\\\";\"" %
			(opts.spark, opts.spark_master))

	cmd("%s/bin/spark-sql --master %s -e \"DROP TABLE IF EXISTS documents; "
			"CREATE EXTERNAL TABLE documents (line STRING) STORED AS TEXTFILE "
			"LOCATION \\\"/user/shark/benchmark/crawl\\\";\"" %
			(opts.spark, opts.spark_master))

	print "=== FINISHED CREATING BENCHMARK DATA ==="


def main():
	opts = parse_args()
	prepare_spark_sql(opts)


if __name__ == "__main__":
	main()
