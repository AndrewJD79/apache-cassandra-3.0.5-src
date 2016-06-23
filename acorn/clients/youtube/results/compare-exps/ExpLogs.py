import os
import re
import sys
import traceback
import zipfile

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons
import Util

import Conf

_dn_tmp = None

def UnzipAndCalcMetadataTraffic():
	global _dn_tmp
	_dn_tmp = "%s/.tmp" % os.path.dirname(__file__)
	Util.MkDirs(_dn_tmp)

	exps = []
	for exp_name, exp_id in Conf.Get("exp_id").iteritems():
		exps.append(Exp(exp_name, exp_id))

	i = 0
	for e in exps:
		if i > 0:
			Cons.P("")
		e.Unzip()
		e.ReadTags()
		e.ReadStatLineByLine()
		e.PrintStatByNodes()
		i += 1


class Exp:
	def __init__(self, exp_name, exp_id):
		self.exp_name = exp_name
		self.exp_id = exp_id
		# ip: NodeStat
		self.nodes = {}
		# Tags for experiment parameters
		self.tags = {}

	def Unzip(self):
		fn_zip = "%s/work/acorn-data/%s.zip" % (os.path.expanduser("~"), self.exp_id)
		with zipfile.ZipFile(fn_zip, "r") as zf:
			zf.extractall("%s/%s" % (_dn_tmp, self.exp_id))

	class NodeStat:
		def __init__(self, ip, fn_log):
			self.dc_name = None
			self.ip = ip
			self.fn_log = fn_log

			self.lat_w = []
			self.lat_r = []
			# Count only outbound traffic to remote AWS regions. Inbound traffic is
			# for free. Traffic to Internet is even more expensive.
			self.eth0_rx = 0
			self.eth0_tx = 0
			self.running_on_time_cnt = 0
			self.running_on_time_sleep_avg_in_ms = []
			self.running_behind_cnt = 0
			self.running_behind_sleep_avg_in_ms = []
			self.cpu = []
			self.acorn_data_disk_space = 0

		def SetDcName(self, dc_name):
			self.dc_name = dc_name

		def AddStat(self, t):
			self.lat_w.append(float(t[6]))
			self.lat_r.append(float(t[7]))
			self.eth0_rx += int(t[13])
			self.eth0_tx += int(t[14])
			self.running_on_time_cnt += int(t[15])
			self.running_on_time_sleep_avg_in_ms.append(int(t[16]))
			self.running_behind_cnt += int(t[17])
			self.running_behind_sleep_avg_in_ms.append(int(t[18]))
			self.cpu.append(float(t[19]))
			self.acorn_data_disk_used = int(t[20])

		def RunningOnTimeSleepAvgInMs(self):
			return sum(self.running_on_time_sleep_avg_in_ms) / float(len(self.running_on_time_sleep_avg_in_ms))

		def RunningBehindSleepAvgInMs(self):
			return sum(self.running_behind_sleep_avg_in_ms) / float(len(self.running_behind_sleep_avg_in_ms))

		def LatWavg(self):
			return sum(self.lat_w) / float(len(self.lat_w))

		def LatRavg(self):
			return sum(self.lat_r) / float(len(self.lat_r))

		def LatWmax(self):
			return max(self.lat_w)

		def LatRmax(self):
			return max(self.lat_r)

		def CpuAvg(self):
			return sum(self.cpu) / float(len(self.cpu))

		def CpuMax(self):
			return max(self.cpu)

	def ReadTags(self):
		fn = "%s/.tmp/%s/var/log/cloud-init-output.log" % (os.path.dirname(__file__), self.exp_id)
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

	def ReadStatLineByLine(self):
		dn = "%s/.tmp/%s/pssh-out" % (os.path.dirname(__file__), self.exp_id)
		dn1 = [os.path.join(dn, o) for o in os.listdir(dn) if os.path.isdir(os.path.join(dn, o))]
		if len(dn1) == 0:
			raise RuntimeError("Unexpected %s" % dn1)
		# Get the last experiment in the directory. There can be repeated
		# experiment attempts due to bugs.
		dn1.sort()
		for f in os.listdir(dn1[-1]):
			f1 = os.path.join(dn1[-1], f)
			self.nodes[f] = Exp.NodeStat(f, f1)

		if len(self.nodes) != 9:
			Cons.P("WARNING: len(self.nodes)=%d" % len(self.nodes))

		for ip, ns in self.nodes.iteritems():
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
							if "ERROR com.datastax.driver.core.ControlConnection" in line:
								continue
							ns.AddStat(t)
			except Exception as e:
				Cons.P("Exception: fn_log=%s\nline=[%s]\n%s" % (ns.fn_log, line, traceback.format_exc()))
				raise e

	def PrintStatByNodes(self):
		Cons.P("# exp_name=%s" % self.exp_name)
		Cons.P("# tags:")
		for k, v in sorted(self.tags.iteritems()):
			Cons.P("#   %s:%s" % (k, v))
		Cons.P("#")
		fmt = "%-14s %-15s" \
				" %11d %11d" \
				" %8d %8d" \
				" %8d %8d" \
				" %7.3f %8.3f %7.3f %8.3f" \
				" %5.2f %5.2f" \
				" %5.0f"
		Cons.P(Util.BuildHeader(fmt, "dc_name ip" \
				" eth0_rx eth0_tx" \
				" running_on_time_cnt running_on_time_sleep_avg_in_ms" \
				" running_behind_cnt running_behind_sleep_avg_in_ms" \
				" lat_w_avg lat_w_max lat_r_avg lat_r_max" \
				" cpu_avg cpu_max" \
				" disk_used_in_mb" \
				))

		out = []
		sum_rx = 0
		sum_tx = 0
		sum_r_ot_cnt = 0
		sum_r_b_cnt = 0
		all_lat_w = []
		all_lat_r = []
		all_cpu = []
		sum_disk_used = 0
		for ip, ns in self.nodes.iteritems():
			out.append(fmt % (
				ns.dc_name, ip
				, ns.eth0_rx, ns.eth0_tx
				, ns.running_on_time_cnt, ns.RunningOnTimeSleepAvgInMs()
				, ns.running_behind_cnt, ns.RunningBehindSleepAvgInMs()
				, ns.LatWavg(), ns.LatWmax(), ns.LatRavg(), ns.LatRmax()
				, ns.CpuAvg(), ns.CpuMax()
				, ns.acorn_data_disk_used / 1000000.0
				))

			sum_rx += ns.eth0_rx
			sum_tx += ns.eth0_tx
			sum_r_ot_cnt += ns.running_on_time_cnt
			sum_r_b_cnt += ns.running_behind_cnt
			sum_r_b_cnt += ns.running_behind_cnt
			all_lat_w += ns.lat_w
			all_lat_r += ns.lat_r
			all_cpu = ns.cpu
			sum_disk_used += ns.acorn_data_disk_used

		# Sort by DC names
		Cons.P("\n".join(sorted(out)))

		Cons.P(fmt % (
			"overall", ""
			, sum_rx, sum_tx
			, sum_r_ot_cnt, 0.0
			, sum_r_b_cnt, 0.0
			, sum(all_lat_w) / float(len(all_lat_w)), max(all_lat_w), sum(all_lat_r) / float(len(all_lat_r)), max(all_lat_r)
			, sum(all_cpu) / float(len(all_cpu)), max(all_cpu)
			, sum_disk_used / 1000000.0
			))
