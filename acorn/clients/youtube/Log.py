import sys
import time

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons


def P(msg):
	Cons.P(msg, prefix = "%s: " % time.strftime("%y%m%d-%H%M%S"))
	#Cons.P(msg, fo = _fo_log, prefix = "%s: " % time.strftime("%y%m%d-%H%M%S"))


# Measure time
class MT(Cons.MT):
	def P(self, m):
		Cons.P(m, prefix = "%s: " % time.strftime("%y%m%d-%H%M%S"))
