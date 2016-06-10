import sys
import time

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons

_fo_log = None

def P(msg):
	fn = "/var/log/acorn/ec2-init.log"
	global _fo_log
	if _fo_log is None:
		_fo_log = open(fn, "a")
	Cons.P(msg)
	Cons.P(msg, fo = _fo_log, prefix = "%s: " % time.strftime("%y%m%d-%H%M%S"))


# Measure time
class MT(Cons.MT):
	def P(self, m):
		Cons.P(m, fo = _fo_log, prefix = "%s: " % time.strftime("%y%m%d-%H%M%S"))
