import os
import sys
import zipfile

sys.path.insert(0, "%s/work/acorn-tools/util/python" % os.path.expanduser("~"))
import Cons
import Util

import Conf

def Unzip():
	with Cons.MT("Unzipping data ..."):
		dn_tmp = "%s/.tmp" % os.path.dirname(__file__)
		Util.MkDirs(dn_tmp)

		full_rep = Conf.Get("exp_id")["full_rep"]
		partial_rep = Conf.Get("exp_id")["partial_rep"]

		exp_ids = [full_rep, partial_rep]
		for eid in exp_ids: 
			dn_data = "%s/work/acorn-data" % os.path.expanduser("~")
			fn_zip = "%s/%s.zip" % (dn_data, eid)

			with zipfile.ZipFile(fn_zip, "r") as zf:
				zf.extractall("%s/%s" % (dn_tmp, eid))
