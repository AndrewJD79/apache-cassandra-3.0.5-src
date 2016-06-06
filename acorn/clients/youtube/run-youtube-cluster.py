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

sys.path.insert(0, "%s" % os.path.dirname(__file__))
import AcornUtil


def GetRemoteDcPubIps():
	curAz = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_result = False)
	curRegion = curAz[:-1]
	#_Log(curAz)
	#_Log(curRegion)

	fn = "%s/.run/dc-ip-map" % os.path.dirname(__file__)
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
		_Log(remotePubIps)

		threads = []
		for rIp in remotePubIps:
			t = threading.Thread(target=ThreadRsync, args=[rIp])
			t.start()
			threads.append(t)

		for t in threads:
			t.join()


def ThreadRsync(ip):
	#_Log(ip)
	# Make sure you sync only source files. Syncing build result confuses the
	# build system.
	cmd = "cd ~/work/acorn/acorn/clients/youtube" \
			" && rsync -a -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"' *.py pom.xml *.yaml src" \
			" %s:work/acorn/acorn/clients/youtube/" % ip
	Util.RunSubp(cmd, shell = True, print_cmd = False)


def main(argv):
	try:
		# Change cwd to where this file is, so that this can be called from anywhere.
		dn_this = os.path.dirname(__file__)
		os.chdir(dn_this)

		# Experiment ID, made of the current datetime in UTC. It is for identifying
		# each run. The nodes do not rely on the value for synchronizing. They use
		# Cassandra itself for synchronization.
		#
		# It is different from job_id since you can run multiple experiment per job.
		exp_id = None
		if len(argv) == 1:
			exp_id = datetime.datetime.utcnow().strftime("%y%m%d-%H%M%S")
		elif len(argv) == 2:
			exp_id = argv[1]
		else:
			raise RuntimeError("argv=%s" % ",".join(argv))
		_Log("Exp id: %s" % exp_id)

		AcornUtil.GenHostfiles()

		RsyncSrcToRemoteDcs()

		# Delete all records in the experiment tables. The records in
		# acorn_attr_pop are supposed to expire by themselves.
		myPubIp = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4")
		Util.RunSubp("cqlsh -e \"" \
				"truncate acorn_pr.t0;" \
				" truncate acorn_obj_loc.obj_loc;" \
				" truncate acorn_exe_barrier.t0;" \
				" truncate acorn_exp_meta.t0;" \
				" truncate acorn_regular.t0;" \
				"\" %s || true" % myPubIp
				, shell = True)

		fn_pssh_hn = "%s/.run/pssh-hostnames" % dn_this

		# Build src and run.
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
				_Log("Success")
			else:
				_Log("Failure. rc=%d" % rc)
			_Log(Util.Indent(stdouterr, 2))
		
		# Make a quick summary reporting tool and generate one.
		fn = "%s/.run/check-last-run.sh" % os.path.dirname(os.path.realpath(__file__))
		with file(fn, "w") as fo:
			fo.write("cat .run/pssh-out/%s/* | grep -E \"" \
					"# writes              :" \
					"|# reads               :" \
					"|# fetch on demand\\(s\\)  :" \
					"|# read misses - DC loc:" \
					"|# read misses - Obj   :" \
					"|# write timeouts      :" \
					"|# read timeouts       :" \
					"|      Local DC:" \
					"|# Write latency:" \
					"|# Read latency :" \
					"\"" \
					% exp_id)
		Util.RunSubp("chmod +x %s" % fn);
		fn_summary = ".run/summary-%s" % exp_id
		Util.RunSubp(".run/check-last-run.sh > %s" % fn_summary, shell = True)
		_Log("A quick summary file is generated at %s" % fn_summary)
		_Log("You can also run .run/check-last-run.sh")

		# TODO: Upload the result to S3 in us-east-1
		# - All exp parameters and results
		# - May want current time

		# TODO
		# Rsync to mt-s7 for analysis. May want to store to S3 later.
		#Util.RunSubp("rsync -a -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"'" \
		#		" /home/ubuntu/work/acorn/acorn/clients/youtube/.run hobin@130.207.110.229:work/acorn-log", shell = True)

		# TODO: Dequeue the experiment request from SQS
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		sys.exit(1)


def _RunSubp(cmd):
	_Log(cmd)

	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	# communidate() waits for termination
	stdouterr = p.communicate()[0]
	rc = p.returncode
	return [rc, stdouterr]


_fo_log = None

def _Log(msg):
	fn = "/var/log/acorn/ec2-init.log"
	global _fo_log
	if _fo_log is None:
		_fo_log = open(fn, "a")
	_fo_log.write("%s: %s\n" % (datetime.datetime.now().strftime("%y%m%d-%H%M%S"), msg))
	_fo_log.flush()
	Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
