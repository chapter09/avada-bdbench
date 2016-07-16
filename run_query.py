#!/bin/python

import subprocess
import sys
from sys import stderr
from argparse import ArgumentParser
import os
import time
import datetime
import re
import multiprocessing
from StringIO import StringIO

# A scratch directory on your filesystem
LOCAL_TMP_DIR = "./tmp"

### Benchmark Queries ###
TMP_TABLE = "result"
TMP_TABLE_CACHED = "result_cached"
CLEAN_QUERY = "DROP TABLE %s;" % TMP_TABLE

# TODO: Factor this out into a separate file
QUERY_1a_HQL = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000"
QUERY_1b_HQL = QUERY_1a_HQL.replace("1000", "100")
QUERY_1c_HQL = QUERY_1a_HQL.replace("1000", "10")

QUERY_2a_HQL = "SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) FROM " \
    "uservisits GROUP BY SUBSTR(sourceIP, 1, 8)"
QUERY_2b_HQL = QUERY_2a_HQL.replace("8", "10")
QUERY_2c_HQL = QUERY_2a_HQL.replace("8", "12")

QUERY_3a_HQL = """SELECT sourceIP,
                          sum(adRevenue) as totalRevenue,
                          avg(pageRank) as pageRank
                   FROM
                     rankings R JOIN
                     (SELECT sourceIP, destURL, adRevenue
                      FROM uservisits UV
                      WHERE UV.visitDate > "1980-01-01"
                      AND UV.visitDate < "1980-04-01")
                      NUV ON (R.pageURL = NUV.destURL)
                   GROUP BY sourceIP
                   ORDER BY totalRevenue DESC
                   LIMIT 1"""
QUERY_3a_HQL = " ".join(QUERY_3a_HQL.replace("\n", "").split())
QUERY_3b_HQL = QUERY_3a_HQL.replace("1980-04-01", "1983-01-01")
QUERY_3c_HQL = QUERY_3a_HQL.replace("1980-04-01", "2010-01-01")

QUERY_4_HQL = """DROP TABLE IF EXISTS url_counts_partial;
                 CREATE TABLE url_counts_partial AS
                   SELECT TRANSFORM (line)
                   USING "python /root/url_count.py" as (sourcePage,
                     destPage, count) from documents;
                 DROP TABLE IF EXISTS url_counts_total;
                 CREATE TABLE url_counts_total AS
                   SELECT SUM(count) AS totalCount, destpage
                   FROM url_counts_partial GROUP BY destpage;"""
QUERY_4_HQL = " ".join(QUERY_4_HQL.replace("\n", "").split())

QUERY_4_HQL_HIVE_UDF = """DROP TABLE IF EXISTS url_counts_partial;
                 CREATE TABLE url_counts_partial AS
                   SELECT TRANSFORM (line)
                   USING "python /tmp/url_count.py" as (sourcePage,
                     destPage, count) from documents;
                 DROP TABLE IF EXISTS url_counts_total;
                 CREATE TABLE url_counts_total AS
                   SELECT SUM(count) AS totalCount, destpage
                   FROM url_counts_partial GROUP BY destpage;"""
QUERY_4_HQL_HIVE_UDF = " ".join(QUERY_4_HQL_HIVE_UDF.replace("\n", "").split())

QUERY_1_PRE = "CREATE TABLE %s (pageURL STRING, pageRank INT);" % TMP_TABLE
QUERY_2_PRE = "CREATE TABLE %s (sourceIP STRING, adRevenue DOUBLE);" % TMP_TABLE
QUERY_3_PRE = "CREATE TABLE %s (sourceIP STRING, " \
    "adRevenue DOUBLE, pageRank DOUBLE);" % TMP_TABLE

QUERY_1a_SQL = QUERY_1a_HQL
QUERY_1b_SQL = QUERY_1b_HQL
QUERY_1c_SQL = QUERY_1c_HQL

QUERY_2a_SQL = QUERY_2a_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_2b_SQL = QUERY_2b_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_2c_SQL = QUERY_2c_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_3a_SQL = """SELECT sourceIP, totalRevenue, avgPageRank
                    FROM
                      (SELECT sourceIP,
                              AVG(pageRank) as avgPageRank,
                              SUM(adRevenue) as totalRevenue
                      FROM Rankings AS R, UserVisits AS UV
                      WHERE R.pageURL = UV.destinationURL
                      AND UV.visitDate
                        BETWEEN Date('1980-01-01') AND Date('1980-04-01')
                      GROUP BY UV.sourceIP)
                   ORDER BY totalRevenue DESC LIMIT 1""".replace("\n", "")
QUERY_3a_SQL = " ".join(QUERY_3a_SQL.replace("\n", "").split())
QUERY_3b_SQL = QUERY_3a_SQL.replace("1980-04-01", "1983-01-01")
QUERY_3c_SQL = QUERY_3a_SQL.replace("1980-04-01", "2010-01-01")


def create_as(query):
    return "CREATE TABLE %s AS %s;" % (TMP_TABLE, query)


def insert_into(query):
    return "INSERT INTO TABLE %s %s;" % (TMP_TABLE, query)


def count(query):
    return query
    return "SELECT COUNT(*) FROM (%s) q;" % query

IMPALA_MAP = {'1a': QUERY_1_PRE, '1b': QUERY_1_PRE, '1c': QUERY_1_PRE,
              '2a': QUERY_2_PRE, '2b': QUERY_2_PRE, '2c': QUERY_2_PRE,
              '3a': QUERY_3_PRE, '3b': QUERY_3_PRE, '3c': QUERY_3_PRE}

TEZ_MAP = {'1a': (count(QUERY_1a_HQL),), '1b': (count(QUERY_1b_HQL),),
           '1c': (count(QUERY_1c_HQL),), '2a': (count(QUERY_2a_HQL),),
           '2b': (count(QUERY_2b_HQL),), '2c': (count(QUERY_2c_HQL),),
           '3a': (count(QUERY_3a_HQL),), '3b': (count(QUERY_3b_HQL),),
           '3c': (count(QUERY_3c_HQL),)}

QUERY_MAP = {
    '1a':  (create_as(QUERY_1a_HQL), insert_into(QUERY_1a_HQL),
            create_as(QUERY_1a_SQL)),
    '1b':  (create_as(QUERY_1b_HQL), insert_into(QUERY_1b_HQL),
            create_as(QUERY_1b_SQL)),
    '1c':  (create_as(QUERY_1c_HQL), insert_into(QUERY_1c_HQL),
            create_as(QUERY_1c_SQL)),
    '2a': (create_as(QUERY_2a_HQL), insert_into(QUERY_2a_HQL),
           create_as(QUERY_2a_SQL)),
    '2b': (create_as(QUERY_2b_HQL), insert_into(QUERY_2b_HQL),
           create_as(QUERY_2b_SQL)),
    '2c': (create_as(QUERY_2c_HQL), insert_into(QUERY_2c_HQL),
           create_as(QUERY_2c_SQL)),
    '3a': (create_as(QUERY_3a_HQL), insert_into(QUERY_3a_HQL),
           create_as(QUERY_3a_SQL)),
    '3b': (create_as(QUERY_3b_HQL), insert_into(QUERY_3b_HQL),
           create_as(QUERY_3b_SQL)),
    '3c': (create_as(QUERY_3c_HQL), insert_into(QUERY_3c_HQL),
           create_as(QUERY_3c_SQL)),
    '4':  (QUERY_4_HQL, None, None),
    '4_HIVE':  (QUERY_4_HQL_HIVE_UDF, None, None)}

# Turn a given query into a version using cached tables


def make_input_cached(query):
    return query.replace("uservisits", "uservisits_cached") \
                .replace("rankings", "rankings_cached") \
                .replace("url_counts_partial", "url_counts_partial_cached") \
                .replace("url_counts_total", "url_counts_total_cached") \
                .replace("documents", "documents_cached")

# Turn a given query into one that creats cached tables


def make_output_cached(query):
    return query.replace(TMP_TABLE, TMP_TABLE_CACHED)

### Runner ###


def parse_args():
    parser = ArgumentParser(usage="run_query.py [options]")

    parser.add_argument("--hadoop", dest="hdfs",
                        default=os.environ.get("HADOOP_HOME"),
                        help="Path of HADOOP_HOME")
    parser.add_argument("--spark",
                        default=os.environ.get("SPARK_HOME"),
                        help="Path of SPARK_HOME")
    parser.add_argument("--spark-master",
                        default=os.environ.get("SPARK_MASTER"),
                        help="Address of Sparl master")

    parser.add_argument("-r", "--restart", action="store_true",
                        default=False, help="Restart Spark")

    parser.add_argument("-g", "--no-cache", action="store_true",
                        default=False, help="Disable caching in Spark")
    parser.add_argument("-t", "--reduce-tasks", type=int, default=150,
                        help="Number of reduce tasks in Spark")
    parser.add_argument("-z", "--clear-buffer-cache", action="store_true",
                        default=False,
                        help="Clear disk buffer cache between query runs")

    parser.add_argument("--num-trials", type=int, default=10,
                        help="Number of trials to run for this query")
    parser.add_argument("--prefix", type=str, default="",
                        help="Prefix result files with this string")

    parser.add_argument("-q", "--query-num", default="1a",
                        help="Which query to run in benchmark: "
                        "%s" % ", ".join(QUERY_MAP.keys()))

    opts = parser.parse_args()

    # checking parameters
    if opts.hdfs is None or opts.spark is None:
        print >> stderr, "The script requires HADOOP_HOME and SPARK_HOME"
        sys.exit(1)

    if opts.query_num not in QUERY_MAP:
        print >> stderr, "Unknown query number: %s" % opts.query_num
        sys.exit(1)

    return opts


def cmd(command):
    return subprocess.check_call(command, shell=True)


def run_spark_sql(opts):
    local_clean_query = CLEAN_QUERY
    local_query_map = QUERY_MAP

    prefix = str(time.time()).split(".")[0]
    query_file_name = "%s_workload.sh" % prefix
    local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
    #local_query_file = "./workload/" + query_file_name
    local_slaves_file = "%s/conf/slaves" % opts.spark
    query_file = open(local_query_file, 'w')
    result_file = "./results/%s_results" % prefix
    tmp_file = "./results/%s_out" % prefix

    runner = "%s/bin/spark-sql --master %s" % (opts.spark,
                                               opts.spark_master)

    print "Getting Slave List"
    slaves = map(str.strip, filter(lambda x: not x.strip().startswith("#")
                                   and x.strip(), \
                                   open(local_slaves_file).readline()))

    if opts.restart:
        print "Restarting standalone scheduler..."
        cmd("%s/sbin/stop-all.sh" % opts.spark)
        ensure_spark_stopped_on_slaves(slaves)
        time.sleep(30)
        cmd("%s/sbin/stop-all.sh" % opts.spark)
        cmd("%s/sbin/start-all.sh" % opts.spark)
        time.sleep(10)

    # Two modes here: Shark Mem and Shark Disk. If using Shark disk clear buffer
    # cache in-between each query. If using Shark Mem, used cached tables.

    query_list = "set mapred.reduce.tasks = %s;" % opts.reduce_tasks

    # Throw away query for JVM warmup
    query_list += "SELECT COUNT(*) FROM scratch;"

    # Create cached queries for Shark Mem
    if not opts.no_cache:
        local_clean_query = make_output_cached(CLEAN_QUERY)

        def convert_to_cached(query):
            return (make_output_cached(make_input_cached(query[0])), )

        local_query_map = {k: convert_to_cached(
            v) for k, v in QUERY_MAP.items()}

        # Set up cached tables
        if '4' in opts.query_num:
            # Query 4 uses entirely different tables
            query_list += """
             DROP TABLE IF EXISTS documents_cached;
             CREATE TABLE documents_cached AS SELECT * FROM documents;
             """
        else:
            query_list += """
             DROP TABLE IF EXISTS uservisits_cached;
             DROP TABLE IF EXISTS rankings_cached;
             CREATE TABLE uservisits_cached AS SELECT * FROM uservisits;
             CREATE TABLE rankings_cached AS SELECT * FROM rankings;
             """

    # Warm up for Query 1
    if '1' in opts.query_num:
        query_list += "DROP TABLE IF EXISTS warmup;"
        query_list += "CREATE TABLE warmup AS SELECT pageURL, " \
        + "pageRank FROM scratch WHERE pageRank > 1000;"

    if '4' not in opts.query_num:
        query_list += local_clean_query

    query_list += local_query_map[opts.query_num][0]

    query_list = re.sub("\s\s+", " ", query_list.replace('\n', ' '))

    print "\nQuery:"
    print query_list.replace(';', ";\n")

    if opts.clear_buffer_cache:
        query_file.write("python ./clear-buffer-cache.py %s\n" %
                         local_slaves_file)

    query_file.write("%s -e '%s' > %s 2>&1\n" % (
        runner, query_list, tmp_file))

    query_file.write(
        "cat %s | grep Time | grep -v INFO |grep -v MapReduce >> %s\n" % (
            tmp_file, result_file))

    query_file.close()
    cmd("chmod +x %s" % local_query_file)

    # Run benchmark
    print "Running remote benchmark..."

    # Collect results
    results = []
    contents = []

    for i in range(opts.num_trials):
        print "Stopping Executors on Slaves....."
        # ensure_spark_stopped_on_slaves(slaves)
        print "Query %s : Trial %i" % (opts.query_num, i + 1)
        cmd("sh %s" % local_query_file)
        content = open(result_file).readlines()
        all_times = map(lambda x: float(
            x.split(": ")[1].split(" ")[0]), content)

        if '4' in opts.query_num:
            query_times = all_times[-4:]
            part_a = query_times[1]
            part_b = query_times[3]
            print "Parts: %s, %s" % (part_a, part_b)
            result = float(part_a) + float(part_b)
        else:
            result = all_times[-1]  # Only want time of last query

    print "Result: ", result
    print "Raw Times: ", content

    results.append(result)
    contents.append(content)

    # Clean-up
    #ssh_shark("rm /mnt/%s*" % prefix)
    print "Clean Up...."
    #ssh_shark("rm /mnt/%s_results" % prefix)
    # os.remove(results_file)

    # os.remove(local_slaves_file)
    # os.remove(local_query_file)

    return results, contents


def ssh_ret_code(host, cmd):
    try:
        return ssh(host, cmd)
    except subprocess.CalledProcessError as e:
        return e.returncode


# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, command):
    return subprocess.check_call(
        "ssh -t -o StrictHostKeyChecking=no %s@%s '%s'" %
        ("ubuntu", host, command), shell=True)


def ensure_spark_stopped_on_slaves(slaves):
    stop = False
    while not stop:
        cmd = "jps | grep ExecutorBackend"
        ret_vals = map(lambda s: ssh_ret_code(s, cmd), slaves)
        print ret_vals
        if 0 in ret_vals:
            print "Spark is still running on some slaves... sleeping"
            cmd = "jps | grep ExecutorBackend"
            + " | cut -d \" \" -f 1 | xargs -rn1 kill -9"
            map(lambda s: ssh_ret_code(s, cmd), slaves)
            time.sleep(2)
        else:
            stop = True


def main():
    if not os.path.exists("./tmp/"):
        os.mkdir("./tmp/")

    opts = parse_args()
    run_spark_sql(opts)


if __name__ == "__main__":
    main()
