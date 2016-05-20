#!/usr/bin/env python

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


def GetRemoteDcPubIps():
	curAz = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_result = False)
	curRegion = curAz[:-1]
	#Cons.P(curAz)
	#Cons.P(curRegion)

	fn = "%s/.run/dc-ip-map" % os.path.dirname(os.path.realpath(__file__))
	ips = []
	with open(fn) as fo:
		for line in fo.readlines():
			t = line.strip().split(" ")
			if len(t) != 2:
				raise RuntimeError("Unexpected format [%s]s" % line)
			dc = t[0]
			ip = t[1]
			if dc != curRegion:
				ips.append(ip)
	return ips


def RsyncSrcToRemoteDcs():
	with Cons.MeasureTime("rsync src to remote DCs ..."):
		remotePubIps = GetRemoteDcPubIps()
		Cons.P(remotePubIps)

		threads = []
		for rIp in remotePubIps:
			t = threading.Thread(target=ThreadRsync, args=[rIp])
			t.start()
			threads.append(t)

		for t in threads:
			t.join()


def ThreadRsync(ip):
	#Cons.P(ip)
	# Make sure you sync only source files. Syncing build result confuses the
	# build system.
	cmd = "cd ~/work/acorn/acorn/clients/youtube" \
			" && rsync -a -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"' *.py pom.xml *.yaml src" \
			" %s:work/acorn/acorn/clients/youtube/" % ip
	Util.RunSubp(cmd, shell = True, print_cmd = False)


def main(argv):
	# Experiment ID: Current datetime in UTC
	# It is a part of the keyspace name.
	exp_id = None

	if len(argv) == 1:
		exp_id = datetime.datetime.utcnow().strftime("%y%m%d-%H%M%S")
	elif len(argv) == 2:
		exp_id = argv[1]
	else:
		print "Usage: %s" % argv[0]
		sys.exit(1)

	AcornUtil.GenHostfiles()

	RsyncSrcToRemoteDcs()

	Cons.P("Exp id: %s" % exp_id)

	dn_this = os.path.dirname(os.path.realpath(__file__))
	fn_pssh_hn = "%s/.run/pssh-hostnames" % dn_this

	# Build src and run.
	#   exp_id is for identifying each run. The nodes do not rely on the
	#   value for synchronizing. They use Cassandra itself for synchronization.
	with Cons.MeasureTime("Running ..."):
		dn_pssh_out = "%s/.run/pssh-out/%s" % (dn_this, exp_id)
		dn_pssh_err = "%s/.run/pssh-err/%s" % (dn_this, exp_id)
		Util.RunSubp("mkdir -p %s" % dn_pssh_out)
		Util.RunSubp("mkdir -p %s" % dn_pssh_err)
		cmd = "parallel-ssh -h %s" \
				" --option=\"StrictHostKeyChecking no\"" \
				" --option=\"UserKnownHostsFile /dev/null\"" \
				" -t 0" \
				" -o %s" \
				" -e %s" \
				" %s/build-src-run-local.py %s 2>&1" \
				% (fn_pssh_hn, dn_pssh_out, dn_pssh_err, dn_this, exp_id)
		[rc, stdouterr] = _RunSubp(cmd)

		# Check output with more
		Util.RunSubp("more %s/*" % dn_pssh_out, shell = True)
		Util.RunSubp("more %s/*" % dn_pssh_err, shell = True)

		if rc == 0:
			Cons.P("Success")
		else:
			Cons.P("Failure. rc=%d" % rc)
		Cons.P(Util.Indent(stdouterr, 2))
	
	# Rsync to mt-s7 for analysis. May want to store to S3 later.
	Util.RunSubp("rsync -a -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"'" \
			" /home/ubuntu/work/acorn/acorn/clients/youtube/.run hobin@130.207.110.229:work/acorn-log", shell = True)

	# Make a quick check tool
	fn = "%s/.run/check-last-run.sh" % os.path.dirname(os.path.realpath(__file__))
	with file(fn, "w") as fo:
		fo.write("cat .run/pssh-out/%s/* | grep -E \"" \
				"# writes              :" \
				"|# reads               :" \
				"|# read misses - DC loc:" \
				"|# read misses - Obj   :" \
				"|# write timeouts      :" \
				"|# read timeouts       :" \
				"|      Local DC:" \
				"|#   avg=\"" \
				% exp_id)

	Util.RunSubp("chmod +x %s" % fn);
	Cons.P("For a quick summary, run .run/check-last-run.sh\n")


def _RunSubp(cmd):
	Cons.P(cmd)

	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	# communidate() waits for termination
	stdouterr = p.communicate()[0]
	rc = p.returncode
	return [rc, stdouterr]


if __name__ == "__main__":
	sys.exit(main(sys.argv))
