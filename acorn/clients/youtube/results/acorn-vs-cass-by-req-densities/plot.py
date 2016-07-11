#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons
import Util

_dn_out = "%s/.output" % os.path.dirname(__file__)

def main(argv):
	Util.MkDirs(_dn_out)

	Plot()


def Plot():
	_Plot("network", "Total outgoing\nnetwork traffic\n(in GB)", 3, 1.0 / 1024 / 1024 / 1024)
	_Plot("lat-w", "Avg write latency (ms)", 8, 1.0)
	_Plot("lat-r", "Avg read latency (ms)", 10, 1.0)
	_Plot("cpu", "Avg CPU usage (%)", 12, 1.0)
	_Plot("disk-space", "Total disk space used (GB)", 14, 1.0 / 1024)


def _Plot(metric_y, label_y ,y_col, y_alpha):
	env = os.environ.copy()
	fn_out = "%s/acorn-vs-cass-by-req-densities-%s.pdf" % (_dn_out, metric_y)
	env["FN_OUT"] = fn_out
	env["MAX_NUM_REQS"] = str(1571389)
	env["LABEL_Y"] = label_y.replace("\n", "\\n")
	env["Y_COL"] = str(y_col)
	env["Y_ALPHA"] = str(y_alpha)

	cmd = "gnuplot %s/acorn-vs-cass-a-metric-by-req-densities.gnuplot" % os.path.dirname(__file__)
	Util.RunSubp(cmd, print_cmd = False, env = env)
	Cons.P("Created %s %d" % (fn_out, os.path.getsize(fn_out)))


if __name__ == "__main__":
	sys.exit(main(sys.argv))
