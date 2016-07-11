import os
import sys

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons
import Util

_fmt = "%-14s" \
		" %11d %12d" \
		" %7d" \
		" %8d" \
		" %6d %7d" \
		" %7.3f %8.3f" \
		" %7.3f %10.3f" \
		" %5.2f %5.2f" \
		" %5.0f" \
		" %1s"

class PerNode:
	def __init__(self, ip, fn_log):
		self.region = None
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
		self.running_behind_min_in_ms = 0
		self.cpu = []
		self.acorn_data_disk_space = 0

		self.parsing_stopped = False

	def SetRegion(self, region):
		self.region = region

	def ParsingStopped(self):
		# Parsing stopped due to an error in the log file. For example, the cluster
		# is loaded and got a
		# com.datastax.driver.core.exceptions.NoHostAvailableException exception.
		self.parsing_stopped = True

	def AddStat(self, t):
		num_w = int(t[4])
		num_r = int(t[5])

		for i in range(num_w):
			self.lat_w.append(float(t[6]))

		for i in range(num_r):
			self.lat_r.append(float(t[7]))

		self.eth0_rx += int(t[13])
		self.eth0_tx += int(t[14])
		self.running_on_time_cnt += int(t[15])

		for i in range(int(t[15])):
			self.running_on_time_sleep_avg_in_ms.append(int(t[16]))

		self.running_behind_cnt += int(t[17])
		self.running_behind_min_in_ms = min(int(t[18]), self.running_behind_min_in_ms)

		self.cpu.append(float(t[19]))

		self.acorn_data_disk_used = int(t[20])

	@staticmethod
	def Header():
		return Util.BuildHeader(_fmt,
				"region" \
				#" ip" \
				" eth0_rx eth0_tx" \
				" running_on_time_cnt" \
				" running_on_time_sleep_avg_in_ms" \
				" running_behind_cnt running_behind_min_in_ms" \
				" lat_w_avg lat_w_max" \
				" lat_r_avg lat_r_max" \
				" cpu_avg cpu_max" \
				" disk_used_in_mb" \
				" parsing_stopped" \
				)

	def __str__(self):
		return _fmt % (
				self.region
				#, self.ip
				, self.eth0_rx, self.eth0_tx
				, self.running_on_time_cnt
				, (sum(self.running_on_time_sleep_avg_in_ms) / float(len(self.running_on_time_sleep_avg_in_ms)))
				, self.running_behind_cnt, self.running_behind_min_in_ms
				, sum(self.lat_w) / float(len(self.lat_w)), max(self.lat_w)
				, sum(self.lat_r) / float(len(self.lat_r)), max(self.lat_r)
				, sum(self.cpu) / float(len(self.cpu)), max(self.cpu)
				, self.acorn_data_disk_used / 1000000.0
				, ("T" if self.parsing_stopped else "F")
				)


class Overall():
	def __init__(self, per_node_results):
		self.sum_rx = 0
		self.sum_tx = 0
		self.sum_r_ot_cnt = 0
		self.r_ot_sleep_avg_in_ms = []

		self.sum_r_b_cnt = 0
		self.r_b_min_in_ms = 0

		self.lat_w = []
		self.lat_r = []
		self.cpu = []
		self.sum_disk_used = 0

		self.parsing_stopped = False

		for ip, nr in per_node_results.iteritems():
			self.sum_rx += nr.eth0_rx
			self.sum_tx += nr.eth0_tx
			self.sum_r_ot_cnt += nr.running_on_time_cnt
			self.r_ot_sleep_avg_in_ms += nr.running_on_time_sleep_avg_in_ms
			self.sum_r_b_cnt += nr.running_behind_cnt
			self.r_b_min_in_ms = min(self.r_b_min_in_ms, nr.running_behind_min_in_ms)

			self.lat_w += nr.lat_w
			self.lat_r += nr.lat_r
			self.cpu += nr.cpu
			self.sum_disk_used += nr.acorn_data_disk_used

			self.parsing_stopped = (self.parsing_stopped or nr.parsing_stopped)

	def __str__(self):
		return _fmt % (
			"overall"
			, self.sum_rx, self.sum_tx

			, self.sum_r_ot_cnt
			, (sum(self.r_ot_sleep_avg_in_ms) / float(len(self.r_ot_sleep_avg_in_ms)))

			, self.sum_r_b_cnt
			, self.r_b_min_in_ms

			, sum(self.lat_w) / float(len(self.lat_w)), max(self.lat_w)
			, sum(self.lat_r) / float(len(self.lat_r)), max(self.lat_r)

			, sum(self.cpu) / float(len(self.cpu)), max(self.cpu)
			, self.sum_disk_used / 1000000.0

			, ("T" if self.parsing_stopped else "F")
			)
