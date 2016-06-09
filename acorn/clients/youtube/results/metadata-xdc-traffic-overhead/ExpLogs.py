import os
import re
import sys
import zipfile

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons
import Util

import Conf

_dn_tmp = None

# 0: with metadata traffic
# 1: without metadata traffic
_exps = []

def UnzipAndCalcMetadataTraffic():
	global _dn_tmp
	_dn_tmp = "%s/.tmp" % os.path.dirname(__file__)
	Util.MkDirs(_dn_tmp)

	# TODO: need to be global?
	global _exps
	_exps = [Exp("with_acorn_metadata", Conf.Get("exp_id")["full_rep_with_acorn_metadata_exchange"])
			, Exp("without_acorn_metadata", Conf.Get("exp_id")["full_rep_without_acorn_metadata_exchange"])]

	i = 0
	for e in _exps:
		if i > 0:
			Cons.P("")
		e.Unzip()
		e.CalcMetadataTraffic()
		e.PrintMetadataTraffic()
		i += 1


class Exp:
	def __init__(self, exp_name, exp_id):
		self.exp_name = exp_name
		self.exp_id = exp_id
		# ip: NodeStat
		self.nodes = {}

	def Unzip(self):
		fn_zip = "%s/work/acorn-data/%s.zip" % (os.path.expanduser("~"), self.exp_id)
		with zipfile.ZipFile(fn_zip, "r") as zf:
			zf.extractall("%s/%s" % (_dn_tmp, self.exp_id))

	class NodeStat:
		def __init__(self, ip, fn_log):
			self.dc_name = None
			self.ip = ip
			self.fn_log = fn_log
			# Count only outbound traffic to remote AWS regions. Inbound traffic is
			# for free. Traffic to Internet is even more expensive.
			self.eth0_rx = 0
			self.eth0_tx = 0
			self.running_on_time_cnt = 0
			self.running_on_time_sleep_avg_in_ms = []
			self.running_behind_cnt = 0
			self.running_behind_sleep_avg_in_ms = []

		def SetDcName(self, dc_name):
			self.dc_name = dc_name

		def AddStat(self, t):
			self.eth0_rx += int(t[13])
			self.eth0_tx += int(t[14])
			self.running_on_time_cnt += int(t[15])
			self.running_on_time_sleep_avg_in_ms.append(int(t[16]))
			self.running_behind_cnt += int(t[17])
			self.running_behind_sleep_avg_in_ms.append(int(t[18]))

		def RunningOnTimeSleepAvgInMs(self):
			return sum(self.running_on_time_sleep_avg_in_ms) / float(len(self.running_on_time_sleep_avg_in_ms))

		def RunningBehindSleepAvgInMs(self):
			return sum(self.running_behind_sleep_avg_in_ms) / float(len(self.running_behind_sleep_avg_in_ms))

	def CalcMetadataTraffic(self):
		dn = "%s/.tmp/%s/pssh-out" % (os.path.dirname(__file__), self.exp_id)
		dn1 = [os.path.join(dn, o) for o in os.listdir(dn) if os.path.isdir(os.path.join(dn, o))]
		if len(dn1) != 1:
			raise RuntimeError("Unexpected %s" % dn1)
		for f in os.listdir(dn1[0]):
			f1 = os.path.join(dn1[0], f)
			self.nodes[f] = Exp.NodeStat(f, f1)

		for ip, ns in self.nodes.iteritems():
			with open(ns.fn_log) as fo:
				pos = "before_body"
				for line in fo.readlines():
					#Cons.P(line.strip())
					# The lines include a full list of AcornOptions and AcornYoutubeOptions

					if line.startswith("    Local DC="):
						#                 0123456789012
						t = re.split(" +|=", line[13:])
						#t = line.split("=")
						ns.SetDcName(t[0])
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
						ns.AddStat(t)

	def PrintMetadataTraffic(self):
		Cons.P("%s:" % self.exp_name)
		sum_rx = 0
		sum_tx = 0
		sum_r_ot_cnt = 0
		sum_r_b_cnt = 0
		fmt = "%-14s %-15s" \
				" %8d %8d" \
				" %8d %8d" \
				" %8d %8d"
		Cons.P(Util.BuildHeader(fmt, "dc_name ip" \
				" eth0_rx eth0_tx" \
				" running_on_time_cnt running_on_time_sleep_avg_in_ms" \
				" running_behind_cnt running_behind_sleep_avg_in_ms"))
		out = []
		for ip, ns in self.nodes.iteritems():
			out.append(fmt % (
				ns.dc_name, ip
				, ns.eth0_rx, ns.eth0_tx
				, ns.running_on_time_cnt, ns.RunningOnTimeSleepAvgInMs()
				, ns.running_behind_cnt, ns.RunningBehindSleepAvgInMs()))

			sum_rx += ns.eth0_rx
			sum_tx += ns.eth0_tx
			sum_r_ot_cnt += ns.running_on_time_cnt
			sum_r_b_cnt += ns.running_behind_cnt

		# Sort by DC names
		Cons.P("\n".join(sorted(out)))

		Cons.P(fmt % (
			"total", ""
			, sum_rx, sum_tx
			, sum_r_ot_cnt, 0.0
			, sum_r_b_cnt, 0.0))
