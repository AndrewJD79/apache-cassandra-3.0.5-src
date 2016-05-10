#!/usr/bin/env python

# TODO: may want to change the name of the test suite

import datetime
import os
import pprint
import subprocess
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
		# Make sure you sync only source files. Syncing build result confuses the
		# build system.
		cmd = "cd ~/work/acorn/acorn/test/partial-rep-2-dcs/FetchOnDemand" \
				" && rsync -av -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"' *.py pom.xml src" \
				" %s:work/acorn/acorn/test/partial-rep-2-dcs/FetchOnDemand/" % ip
		Util.RunSubp(cmd, shell = True)


def main(argv):
	AcornUtil.GenHostfiles()

	RsyncSrcToUsWest1()

	# Current datetime in UTC
	cur_datetime = datetime.datetime.utcnow().strftime("%y%m%d-%H%M%S")
	Cons.P("Exp id: %s" % cur_datetime)

	dn_this = os.path.dirname(os.path.realpath(__file__))
	fn_pssh_hn = "%s/.run/pssh-hostnames" % dn_this

	# Build src and run.
	#   cur_datetime is for identifying each run. The nodes do not rely on the
	#   value for synchronizing. They use Cassandra itself for synchronization.
	with Cons.MeasureTime("Running ..."):
		dn_pssh_out = "%s/.run/pssh-out/%s" % (dn_this, cur_datetime)
		dn_pssh_err = "%s/.run/pssh-err/%s" % (dn_this, cur_datetime)
		Util.RunSubp("mkdir -p %s" % dn_pssh_out)
		Util.RunSubp("mkdir -p %s" % dn_pssh_err)
		cmd = "parallel-ssh -h %s" \
				" --option=\"StrictHostKeyChecking no\"" \
				" --option=\"UserKnownHostsFile /dev/null\"" \
				" -t 0" \
				" -o %s" \
				" -e %s" \
				" %s/build-src-run-local.py %s 2>&1" \
				% (fn_pssh_hn, dn_pssh_out, dn_pssh_err, dn_this, cur_datetime)
		_RunSubp(cmd)

	# Check output with more
	Util.RunSubp("more %s/*" % dn_pssh_out, shell = True)
	Util.RunSubp("more %s/*" % dn_pssh_err, shell = True)


def _RunSubp(cmd):
	Cons.P(cmd)

	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	# communidate() waits for termination
	stdouterr = p.communicate()[0]
	rc = p.returncode
	if rc != 0:
		Cons.P("Error: cmd=[%s] rc=%d" % (cmd, rc))
	if len(stdouterr) > 0:
		Cons.P(stdouterr)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
