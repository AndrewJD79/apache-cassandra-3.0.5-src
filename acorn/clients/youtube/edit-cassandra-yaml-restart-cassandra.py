#!/usr/bin/env python

import os
import subprocess
import sys

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util

import AcornUtil


_exp_id = "update-src-restart-cassandra-except-local-dc"


def main(argv):
	AcornUtil.GenHostfiles()

	RunPssh("\"(cd /home/ubuntu/work/acorn" \
			" && git checkout -- acorn" \
			" && git pull" \
			" && /home/ubuntu/work/acorn/acorn/clients/youtube/edit-cassandra-yaml-local.py" \
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
