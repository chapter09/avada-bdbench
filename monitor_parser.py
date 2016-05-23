#!/bin/python
__author__  = "www.haow.ca"

# requirements: ansible

import os, sys
import signal
from sys import stderr
import subprocess
from argparse import ArgumentParser
import time, datetime


def parse_args():
	parser = ArgumentParser(usage="monitor_parser.py [options]")

	parser.add_argument("-p", dest="path", type=str, help="Input directory")

	opts = parser.parse_args()

	if not opts.path:
		print >> stderr, "Please enter the input path"
		parser.print_help()
		sys.exit(1)
	return opts


def parse_disk(in_fd, out_fd): 
	for line in in_fd.readlines():
		if line.startswith("vda"):
			out_fd.write(line.split(" ")[-1].strip()+"\n")


def parse_jvm(in_fd, out_fd):
	# output format:
	# exe-mem exe-cpu exe-gc data-mem data-cpu data-gc name-mem name-cpu name-gc
	pt_executor = "ExecutorBackend"
	pt_datanode = "tanode.DataNode"
	pt_namenode = "tanode.NameNode"
	
	#header = ("exe-mem\t exe-cpu\t exe-gc\t"
	#    "data-mem\t data-cpu\t data-gc\t"
	#    "name-mem\t name-cpu\t name-gc\n")
	#out_fd.write(header)

	outline = [" "]*9

	for line in in_fd.readlines():
		if pt_executor in line:
			exe_mem = line.split()[2].strip()	
			exe_cpu = line.split()[6].strip()	
			exe_gc = line.split()[7].strip()	
			outline[0] = exe_mem[:-1]
			outline[1] = exe_cpu[:-1]
			outline[2] = exe_gc[:-1]
		elif pt_datanode in line:
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


def parse_net(in_fd, out_fd):
	# output format:
	# exe-snd exe-rev dn-snd dn-rev
	# Todo: hardcode pid
	outline = ["0"]*4
	for line in in_fd.readlines():
		if "27290" in line:
			exe_snd = line.split()[-2].strip()
			exe_rev = line.split()[-1].strip()
			outline[0] = exe_snd
			outline[1] = exe_rev
		elif "19633" in line:
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
	for d, sub_d, f_list in os.walk(opts.path):
		if f_list:
			for f in f_list:
				fn, ext = os.path.splitext(f)
				if "txt" in ext:
					continue

				in_fd = open(d+"/"+f)
				out_fd = open(d+"/"+os.path.splitext(f)[0]+".txt", "w")

				print "Parsing %s" % (d+"/"+f)
				print "Creating %s" % (d+"/"+os.path.splitext(f)[0]+".txt")

				if "disk" in f:
					parse_disk(in_fd, out_fd)
				elif "jvm" in f:
					parse_jvm(in_fd, out_fd)
				elif "net" in f:
					parse_net(in_fd, out_fd)
				else:
					print "Unkown file: %s" % (d+"/"+f)

				in_fd.close()
				out_fd.close()


def main():
	opts = parse_args()
	parse(opts)


if __name__ == "__main__":
	main()
