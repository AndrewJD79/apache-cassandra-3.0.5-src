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

	for e in _exps:
		e.Unzip()
		e.CalcMetadataTraffic()
		e.PrintMetadataTraffic()


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

		def AddEth0RxTx(self, r, t):
			self.eth0_rx += r
			self.eth0_tx += t

		def SetDcName(self, dc_name):
			self.dc_name = dc_name

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
						ns.AddEth0RxTx(int(t[13]), int(t[14]))

	def PrintMetadataTraffic(self):
		Cons.P("%s:" % self.exp_name)
		sum_rx = 0
		sum_tx = 0
		for ip, ns in self.nodes.iteritems():
			Cons.P("  %-10s %-15s %d %d" % (ns.dc_name, ip, ns.eth0_rx, ns.eth0_tx))
			sum_rx += ns.eth0_rx
			sum_tx += ns.eth0_tx
		Cons.P("  %-10s %-15s %d %d" % ("total", "", sum_rx, sum_tx))
