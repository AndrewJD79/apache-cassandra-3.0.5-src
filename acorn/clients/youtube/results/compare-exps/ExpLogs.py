import json
import os
import pprint
import re
import sys
import traceback
import zipfile

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons
import Util

import Conf
import Result

_dn_tmp = "%s/.tmp" % os.path.dirname(__file__)

def AcornVsCassByReqDensity():
	Cons.P("Acorn:")
	# { exp_name(req_density): exp_id }
	acorn = {}
	acorn_exps = Conf.Get("acorn_vs_cass_by_req_density").get("acorn")
	if acorn_exps is not None:
		for exp_name, exp_id in acorn_exps.iteritems():
			acorn[exp_name] = Exp(exp_name, exp_id)

	fmt = "%7s" \
			" %11d %12d" \
			" %7d" \
			" %8d" \
			" %6d %7d" \
			" %7.3f %8.3f" \
			" %7.3f %10.3f" \
			" %5.2f %6.2f" \
			" %5.0f"
	Cons.P(Util.BuildHeader(fmt,
		"request_density" \
		" eth0_rx eth0_tx" \
		" running_on_time_cnt" \
		" running_on_time_sleep_avg_in_ms" \
		" running_behind_cnt running_behind_min_in_ms" \
		" lat_w_avg lat_w_max" \
		" lat_r_avg lat_r_max" \
		" cpu_avg cpu_max" \
		" disk_used_in_mb"
		))
	for exp_name, e in sorted(acorn.iteritems()):
		e.Load()
		o = e.result_overall
		Cons.P(fmt %
				(exp_name.split("-")[1]
					, o.sum_rx, o.sum_tx
					, o.sum_r_ot_cnt
					, (sum(o.r_ot_sleep_avg_in_ms) / float(len(o.r_ot_sleep_avg_in_ms)))
					, o.sum_r_b_cnt
					, o.r_b_min_in_ms
					, sum(o.lat_w) / float(len(o.lat_w)), max(o.lat_w)
					, sum(o.lat_r) / float(len(o.lat_r)), max(o.lat_r)
					, sum(o.cpu) / float(len(o.cpu)), max(o.cpu)
					, o.sum_disk_used / 1000000.0
					)
				)

	Cons.P("")
	Cons.P("Cassandra:")
	cass = {}
	cass_exps = Conf.Get("acorn_vs_cass_by_req_density").get("cassandra")
	if cass_exps is not None:
		for exp_name, exp_id in cass_exps.iteritems():
			cass[exp_name] = Exp(exp_name, exp_id)
	for exp_name, e in sorted(cass.iteritems()):
		e.Load()
		o = e.result_overall
		Cons.P(fmt %
				(exp_name.split("-")[1]
					, o.sum_rx, o.sum_tx
					, o.sum_r_ot_cnt
					, (sum(o.r_ot_sleep_avg_in_ms) / float(len(o.r_ot_sleep_avg_in_ms)))
					, o.sum_r_b_cnt
					, o.r_b_min_in_ms
					, sum(o.lat_w) / float(len(o.lat_w)), max(o.lat_w)
					, sum(o.lat_r) / float(len(o.lat_r)), max(o.lat_r)
					, sum(o.cpu) / float(len(o.cpu)), max(o.cpu)
					, o.sum_disk_used / 1000000.0
					)
				)


def UnzipAndReportExp():
	Util.MkDirs(_dn_tmp)

	exps = []
	for exp_name, exp_id in sorted(Conf.Get("new_exps").iteritems()):
		exps.append(Exp(exp_name, exp_id))

	i = 0
	for e in exps:
		if i > 0:
			Cons.P("")
		e.Load()
		e.PrintStatByNodes()
		i += 1


class Exp:
	def __init__(self, exp_name, exp_id):
		self.exp_name = exp_name
		self.exp_id = exp_id

		# Conf options and ec2 instance tags for experiment parameters
		self.tags = {}
		self.confs_acorn = None
		self.confs_acorn_youtube = None

		# {ip: per-node stat}
		self.result_per_node = {}

		# Overall stat
		self.result_overall = None


	def Load(self):
		self._Unzip()
		self._ReadTags()
		self._ReadConfs()
		self._ReadStatLineByLine()

	def _Unzip(self):
		fn_zip = "%s/work/acorn-log/%s.zip" % (os.path.expanduser("~"), self.exp_id)
		with zipfile.ZipFile(fn_zip, "r") as zf:
			zf.extractall("%s/%s" % (_dn_tmp, self.exp_id))

	def _ReadTags(self):
		fn = "%s/.tmp/%s/var/log/cloud-init-output.log" % (os.path.dirname(__file__), self.exp_id)

		# Fall back to the old log file
		if not os.path.exists(fn):
			fn = "%s/.tmp/%s/var/log/acorn/ec2-init.log" % (os.path.dirname(__file__), self.exp_id)

		with open(fn) as fo:
			for line in fo.readlines():
				if ": tags_str: " in line:
					t = line.split(": tags_str: ")
					if len(t) != 2:
						raise RuntimeError("Unexpected %s" % line)
					for tags_kv in t[1].split(","):
						t1 = tags_kv.split(":")
						if len(t1) != 2:
							raise RuntimeError("Unexpected %s" % line)
						self.tags[t1[0]] = t1[1].rstrip()

	def _ReadConfs(self):
		fn = "%s/.tmp/%s/current-AcornYoutube-stdout-stderr" % (os.path.dirname(__file__), self.exp_id)
		with open(fn) as fo:
			for line in fo.readlines():
				if line.startswith("AcornOptions: "):
					#                 01234567890123
					self.confs_acorn = json.loads(line[14:])
					#Cons.P("Acorn options:\n%s" % Util.Indent(pprint.pformat(self.confs_acorn), 2))
				elif line.startswith("AcornYoutubeOptions: "):
					#                   012345678901234567890
					self.confs_acorn_youtube = json.loads(line[21:])
					self.confs_acorn_youtube.pop("mapDcCoord", None)
					#Cons.P("Acorn Youtube options:\n%s" % Util.Indent(pprint.pformat(self.confs_acorn_youtube), 2))

	def _ReadStatLineByLine(self):
		dn = "%s/.tmp/%s/pssh-out" % (os.path.dirname(__file__), self.exp_id)
		dn1 = [os.path.join(dn, o) for o in os.listdir(dn) if os.path.isdir(os.path.join(dn, o))]
		if len(dn1) == 0:
			raise RuntimeError("Unexpected %s" % dn1)
		# Get the last experiment in the directory. There can be repeated
		# experiment attempts due to bugs.
		dn1.sort()
		for f in os.listdir(dn1[-1]):
			f1 = os.path.join(dn1[-1], f)
			self.result_per_node[f] = Result.PerNode(f, f1)

		if len(self.result_per_node) != 11:
			Cons.P("WARNING: len(self.result_per_node)=%d" % len(self.result_per_node))

		for ip, ns in self.result_per_node.iteritems():
			try:
				with open(ns.fn_log) as fo:
					pos = "before_body"
					for line in fo.readlines():
						#Cons.P(line.strip())
						# The lines include a full list of AcornOptions and AcornYoutubeOptions

						if line.startswith("    Local DC="):
							#                 0123456789012
							t = re.split(" +|=", line[13:])
							#t = line.split("=")
							ns.SetRegion(t[0])
							continue

						t = line.split()

						if pos == "before_body":
							if len(t) > 10 and t[0] == "#":
								header_cnt = 0
								for i in range(1, 10 + 1):
									if t[i] == str(i):
										header_cnt += 1
								if header_cnt == 10:
									pos = "in_body"
						elif pos == "in_body":
							if len(t) > 0 and t[0] == "#":
								pos = "after_body"
								break

							#Cons.P(line.rstrip())
							if "ERROR com.datastax.driver.core.ControlConnection" in line:
								continue
							ns.AddStat(t)
			except Exception as e:
				Cons.P("Exception: fn_log=%s\nline=[%s]\n%s" % (ns.fn_log, line, traceback.format_exc()))
				raise e

		# Make overall stat
		self.result_overall = Result.Overall(self.result_per_node)

	def PrintStatByNodes(self):
		Cons.P("# exp_name: %s" % self.exp_name)
		Cons.P("# tags:")
		for k, v in sorted(self.tags.iteritems()):
			Cons.P("#   %s:%s" % (k, v))
		Cons.P("# Acorn options:\n%s" % re.sub(re.compile("^", re.MULTILINE), "#   ", pprint.pformat(self.confs_acorn)))
		Cons.P("# Acorn Youtube options:\n%s" % re.sub(re.compile("^", re.MULTILINE), "#   ", pprint.pformat(self.confs_acorn_youtube)))
		Cons.P("#")

		Cons.P(Result.PerNode.Header())
		for rpn in sorted(self.result_per_node.values(), key=lambda rpn: rpn.region):
			Cons.P(rpn)
		Cons.P(self.result_overall)
