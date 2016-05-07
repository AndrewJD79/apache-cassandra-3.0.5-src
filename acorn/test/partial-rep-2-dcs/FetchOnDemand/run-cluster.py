#!/usr/bin/env python

# TODO: may want to change the name of the test suite

import datetime
import os
import pprint
import sys
import threading

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util

import AcornUtil


def GetUsWest1PubIp():
	fn = "%s/.run/dc-ip-map" % os.path.dirname(os.path.realpath(__file__))
	with open(fn) as fo:
		for line in fo.readlines():
			t = line.strip().split(" ")
			if len(t) != 2:
				raise RuntimeError("Unexpected format [%s]s" % line)
			dc = t[0]
			ip = t[1]
			if dc == "us-west-1":
				return ip


def RsyncSrcToUsWest1():
	with Cons.MeasureTime("rsync src to us-west-1 ..."):
		ip = GetUsWest1PubIp()
		#Cons.P(ip)
		cmd = "rsync -av -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"' ~/work/acorn/acorn %s:work/acorn/" % ip
		Util.RunSubp(cmd, shell = True)


def main(argv):
	AcornUtil.GenHostfiles()

	RsyncSrcToUsWest1()

	# Need to consider timezone too
	cur_datetime = datetime.datetime.utcnow().strftime("%y%m%d-%H%M%S")
	Cons.P(cur_datetime)

	dn_this = os.path.dirname(os.path.realpath(__file__))
	fn_pssh_hn = "%s/.run/pssh-hostnames" % dn_this

	# Build src and run.
	#   cur_datetime is for identifying each run. The nodes do not rely on the
	#   value for synchronizing. They use Cassandra itself for synchronization.
	with Cons.MeasureTime("Running ..."):
		dn_pssh_out = "%s/.run/pssh-out/%s" % (dn_this, cur_datetime)
		Util.RunSubp("mkdir -p %s" % dn_pssh_out)
		cmd = "parallel-ssh -h %s" \
				" --option=\"StrictHostKeyChecking no\"" \
				" --option=\"UserKnownHostsFile /dev/null\"" \
				" -t 0" \
				" -o %s" \
				" %s/build-src-run-local.py %s 2>&1" \
				% (fn_pssh_hn, dn_pssh_out, dn_this, cur_datetime)
		Util.RunSubp(cmd, shell = True)
		# Check manually if something doesn't go right


if __name__ == "__main__":
	sys.exit(main(sys.argv))
