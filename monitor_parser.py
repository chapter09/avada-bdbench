#!/bin/python
__author__  = "www.haow.ca"

# requirements: ansible

import os, sys
import re
import signal
from sys import stderr
import subprocess
from argparse import ArgumentParser
import time, datetime

MASTER=""
WORKERS=[]

def parse_args():
	parser = ArgumentParser(usage="monitor_parser.py [options]")
	parser.add_argument("-p", dest="path", type=str, help="Input directory")
	parser.add_argument("--hosts", dest="hosts", type=str, help="Node list")
	opts = parser.parse_args()

	if not opts.path:
		print >> stderr, "Please enter the input path"
		parser.print_help()
		sys.exit(1)
	return opts


def to_epoch(date_str):
	p = "%Y-%m-%d %H:%M:%S"
	return int(time.mktime(time.strptime(date_str, p)))


def parse_hosts(opts):
	print "### Parsing hosts ###"
	in_fd = open(opts.hosts)
	flag_master = -1
	flag_worker = 999
	for i, line in enumerate(in_fd):
		if line is None:
			break
		elif "master" in line:
			flag_master = i
		elif i == flag_master+1:
			MASTER = line.split(" ")[0]
		elif "worker" in line:
			flag_worker = i
		elif i > flag_worker:
			WORKERS.append(line.split(" ")[0])
	

def parse_log(opts):
	print "### Parsing log ###"
	in_fd = open(opts.path+"/spark.log")
	out_fds = {} 
	for worker in WORKERS:
		out_fds[worker.strip()] = open(
				opts.path+"/task-%s.log"%worker.split(".")[0], 'w')
  
	visited = False
	for line in in_fd.readlines():
		if "Finished task" in line:
			line_splitted = line.split(" ")
			date = line_splitted[0]
			time = line_splitted[1]
			task_id = line_splitted[8]
			stage_id = line_splitted[11]
			t_cost = int(line_splitted[15])
			host = line_splitted[18]
      
			start_time = to_epoch(date+" "+time) - t_cost/1000

			if not visited:
				base_time = start_time
				start_time = 0
				visited = True
			else:
			  start_time = start_time - base_time

			finish_time = start_time + t_cost/1000
			out_fds[host].write("%d\t%d\t%s\t%s\n"\
					% (start_time, finish_time, task_id, stage_id)) 

	in_fd.close()
	for fd in out_fds.values():
		fd.close()


def parse_disk(in_fd, out_fd): 
	print "### Parsing disk ###"
	for line in in_fd.readlines():
		disk_util = 0
		util_1 = 0
		util_2 = 0
		if line.startswith("xvdb"):
			util_1 = float(line.split(" ")[-1].strip())
		if line.startswith("xvdc"):
			util_2 = float(line.split(" ")[-1].strip())
		out_fd.write("%d\n"%((util_1 + util_2)/2))


def parse_jvm(in_fd, out_fd):
	print "### Parsing jvm ###"
	# output format:
	# exe-mem exe-cpu exe-gc data-mem data-cpu data-gc name-mem name-cpu name-gc
	pt_executor = "ExecutorBackend"
	pt_datanode = "tanode.DataNode"
	pt_namenode = "tanode.NameNode"
	ext_id = dn_id = None

	#header = ("exe-mem\t exe-cpu\t exe-gc\t"
	#    "data-mem\t data-cpu\t data-gc\t"
	#    "name-mem\t name-cpu\t name-gc\n")
	#out_fd.write(header)

	outline = [" "]*9

	for line in in_fd.readlines():
		if pt_executor in line:
			ext_id = line.split()[0].strip()
			exe_mem = line.split()[2].strip()	
			exe_cpu = line.split()[6].strip()	
			exe_gc = line.split()[7].strip()	
			outline[0] = exe_mem[:-1]
			outline[1] = exe_cpu[:-1]
			outline[2] = exe_gc[:-1]
		elif pt_datanode in line:
			dn_id = line.split()[0].strip()
			dn_mem = line.split()[2].strip()	
			dn_cpu = line.split()[6].strip()	
			dn_gc = line.split()[7].strip()	
			outline[3] = dn_mem[:-1]
			outline[4] = dn_cpu[:-1]
			outline[5] = dn_gc[:-1]
		elif pt_namenode in line:
			nd_mem = line.split()[2].strip()	
			nd_cpu = line.split()[6].strip()	
			nd_gc = line.split()[7].strip()	
			outline[6] = nd_mem[:-1]
			outline[7] = nd_cpu[:-1]
			outline[8] = nd_gc[:-1]
		elif not line.strip():
			out_str = "\t".join(map(str, outline))
			if out_str.strip():
				out_fd.write(out_str+"\n")
				outline = [" "]*9
			else:
				continue
	return (ext_id, dn_id)


def parse_net(in_fd, out_fd, ext_id, dn_id):
	print "### Parsing net ###"
	# output format:
	# exe-snd exe-rev dn-snd dn-rev
	# Todo: hardcode pid
	outline = ["0"]*4
	for line in in_fd.readlines():
		if ext_id in line:
			exe_snd = line.split()[-2].strip()
			exe_rev = line.split()[-1].strip()
			outline[0] = exe_snd
			outline[1] = exe_rev
		elif dn_id in line:
			dn_snd = line.split()[-2]
			dn_rev = line.split()[-1]
			outline[2] = dn_snd
			outline[3] = dn_rev
		elif not line.strip():
			out_str = ",".join(map(str, outline))
			if out_str.strip():
				out_fd.write(out_str+"\n")
				outline = ["0"]*4
			else:
				continue
		

def parse(opts):
	# walk through the directory
	# suppose the input directory is /monitor/146359****
	parse_hosts(opts)
	parse_log(opts) # read in spark.log and output task start/finish time

	for d, sub_d, f_list in os.walk(opts.path):
		if f_list:
			# iterate in the order of disk, jvmtop, net
			f_list.sort()

			for f in f_list:
				if f.startswith(".") \
						or "txt" in f \
						or "spark" in f \
						or "task" in f:
					print f
					continue

				in_fd = open(d+"/"+f)
				out_fd = open(d+"/"+os.path.splitext(f)[0]+".txt", "w")

				print "Parsing %s" % (d+"/"+f)
				print "Creating %s" % (d+"/"+os.path.splitext(f)[0]+".txt")

				if "disk" in f:
					parse_disk(in_fd, out_fd)
				elif "jvm" in f:
					ext_id, dn_id = parse_jvm(in_fd, out_fd)
				elif "net" in f and ext_id and dn_id:
					parse_net(in_fd, out_fd, ext_id, dn_id)
				else:
					print "Unkown file: %s" % (d+"/"+f)

				in_fd.close()
				out_fd.close()


def main():
	opts = parse_args()
	#parse_hosts(opts)
	#parse_log(opts)
	parse(opts)


if __name__ == "__main__":
	main()
