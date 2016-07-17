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
	_PlotReqDensityVsMetric()

	_PlotCostLat()


def _PlotReqDensityVsMetric():
	__PlotReqDensityVsMetric("network", "Total outgoing\nnetwork traffic\n(in GB)", 3, 1.0 / 1024 / 1024 / 1024)
	__PlotReqDensityVsMetric("lat-w", "Avg write latency (ms)", 8, 1.0, y_max = 300)
	__PlotReqDensityVsMetric("lat-r", "Avg read latency (ms)", 10, 1.0, y_max = 300)
	__PlotReqDensityVsMetric("cpu", "Avg CPU usage (%)", 12, 1.0)
	__PlotReqDensityVsMetric("disk-space", "Total disk space used (GB)", 14, 1.0 / 1024)

	__PlotReqDensityVsMetric("cost", "Cost ($)", 0, 1.0)


def __PlotReqDensityVsMetric(metric_y, label_y , y_col, y_alpha, y_max = None):
	env = os.environ.copy()
	fn_out = "%s/acorn-vs-cass-by-req-densities-%s.pdf" % (_dn_out, metric_y)
	env["FN_OUT"] = fn_out
	#env["MAX_NUM_REQS"] = str(1571389)
	env["LABEL_Y"] = label_y.replace("\n", "\\n")
	env["Y_COL"] = str(y_col)
	env["Y_ALPHA"] = str(y_alpha)
	env["METRIC_Y"] = metric_y

	if y_max is not None:
		env["Y_MAX"] = str(y_max)

	cmd = "gnuplot %s/acorn-vs-cass-a-metric-by-req-densities.gnuplot" % os.path.dirname(__file__)
	Util.RunSubp(cmd, print_cmd = False, env = env)
	Cons.P("Created %s %d" % (fn_out, os.path.getsize(fn_out)))


def _PlotCostLat():
	__PlotCostLat("lat-r", "Avg read latency (ms)", 10, y_max = 300)
	__PlotCostLat("lat-w", "Avg write latency (ms)", 8, y_max = 300)


def __PlotCostLat(metric_y, label_y, y_col, y_max = None):
	env = os.environ.copy()
	fn_out = "%s/acorn-vs-cass-by-req-densities-cost-vs-%s.pdf" % (_dn_out, metric_y)
	env["FN_OUT"] = fn_out
	env["LABEL_Y"] = label_y.replace("\n", "\\n")
	env["Y_COL"] = str(y_col)

	if y_max is not None:
		env["Y_MAX"] = str(y_max)

	cmd = "gnuplot %s/acorn-vs-cass-by-req-densities-cost-vs-lat.gnuplot" % os.path.dirname(__file__)
	Util.RunSubp(cmd, print_cmd = False, env = env)
	Cons.P("Created %s %d" % (fn_out, os.path.getsize(fn_out)))


if __name__ == "__main__":
	sys.exit(main(sys.argv))
