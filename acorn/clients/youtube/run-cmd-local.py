#!/usr/bin/env python

import os
import re
import subprocess
import sys
import threading

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util

import AcornUtil


_exp_id = "run-cmd-cluster"


def main(argv):
	_KillAcornYoutube()

	#Util.RunSubp("/home/ubuntu/work/acorn-tools/cass/cass-restart.py")


def _KillAcornYoutube():
	out = Util.RunSubp("ps -ef | grep target/AcornYoutube-0.1.ja[r] || true"
			, shell = True, print_cmd = False, print_result = False)
	if len(out) == 0:
		print "No process to kill"
		return

	for line in out.split("\n"):
		t = re.split(" +", line)
		#print t
		if len(t) >= 2:
			pid = t[1]
			sys.stdout.write("killing %s " % pid)
			sys.stdout.flush()
			Util.RunSubp("kill %s" % pid)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
