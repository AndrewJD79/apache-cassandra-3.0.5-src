import os
import sys
import yaml

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons

_docs = None

def Load():
	fn = "conf.yaml"
	global _fo, _docs
	_docs = yaml.load(open(fn))

def Get(k):
	return _docs[k]
