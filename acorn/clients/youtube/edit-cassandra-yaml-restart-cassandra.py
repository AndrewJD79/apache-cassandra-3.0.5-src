#!/usr/bin/env python

import os
import subprocess
import sys

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util

import AcornUtil


_exp_id = "edit-cassandra-yaml-restart-cassandra"


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
	AcornUtil.GenHostfiles()

	RsyncSrcToRemoteDcs()

	RunPssh("\"(/home/ubuntu/work/acorn/acorn/clients/youtube/edit-cassandra-yaml-local.py" \
			" && /home/ubuntu/work/acorn-tools/cass/cass-restart.py" \
			")\"")


def RunPssh(cmd):
	dn_this = os.path.dirname(os.path.realpath(__file__))
	fn_pssh_hn = "%s/.run/pssh-hostnames" % dn_this

	# Stop cassandra. _exp_id is for identifying each run.
	with Cons.MeasureTime("Running ..."):
		dn_pssh_out = "%s/.run/pssh-out/%s" % (dn_this, _exp_id)
		dn_pssh_err = "%s/.run/pssh-err/%s" % (dn_this, _exp_id)
		Util.RunSubp("mkdir -p %s" % dn_pssh_out)
		Util.RunSubp("mkdir -p %s" % dn_pssh_err)
		cmd_pssh = "parallel-ssh -h %s" \
				" --option=\"StrictHostKeyChecking no\"" \
				" --option=\"UserKnownHostsFile /dev/null\"" \
				" -t 0" \
				" -o %s" \
				" -e %s" \
				" %s 2>&1" \
				% (fn_pssh_hn, dn_pssh_out, dn_pssh_err, cmd)
		[rc, stdouterr] = _RunSubp(cmd_pssh)

		# Check output with more
		Util.RunSubp("more %s/*" % dn_pssh_out, shell = True)
		Util.RunSubp("more %s/*" % dn_pssh_err, shell = True)

		if rc == 0:
			Cons.P("Success")
		else:
			Cons.P("Failure. rc=%d" % rc)

		Cons.P(Util.Indent(stdouterr, 2))

		if rc != 0:
			sys.exit(0)


def _RunSubp(cmd):
	Cons.P(cmd)

	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	# communidate() waits for termination
	stdouterr = p.communicate()[0]
	rc = p.returncode
	return [rc, stdouterr]


if __name__ == "__main__":
	sys.exit(main(sys.argv))
