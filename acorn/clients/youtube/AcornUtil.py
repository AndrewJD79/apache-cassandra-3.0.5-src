import os
import pprint
import sys

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/ec2")
import DescInst


def GenHostfiles():
	dn = "%s/.run" % os.path.dirname(os.path.realpath(__file__))
	fn_pssh_hn = "%s/pssh-hostnames" % dn

	# Hostnames except local DC
	fn_pssh_hn_el = "%s/pssh-hostnames-el" % dn

	fn_dc_ip_map = "%s/dc-ip-map" % dn

	# Generate all files if any of them doesn't exist
	if os.path.isfile(fn_pssh_hn) and os.path.isfile(fn_pssh_hn_el) and os.path.isfile(fn_dc_ip_map):
		return

	with Cons.MeasureTime("Generating host files ..."):
		sys.stdout.write("  ")
		inst_descriptions = DescInst.GetInstDescs("acorn-server")
		#Cons.P(pprint.pformat(inst_descriptions, indent=2, width=100))

		# Take only running instances. There can be other instances like "terminated".
		inst_descriptions = [a for a in inst_descriptions if a["State"]["Name"] == "running"]

		Util.RunSubp("mkdir -p %s" % dn)

		with open(fn_pssh_hn, "w") as fo:
			for inst_desc in inst_descriptions:
				fo.write("%s\n" % inst_desc["PublicIpAddress"])
		Cons.P("Created %s %d" % (fn_pssh_hn, os.path.getsize(fn_pssh_hn)))

		myPubIp = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4", print_cmd = False, print_result = False)
		with open(fn_pssh_hn_el, "w") as fo:
			for inst_desc in inst_descriptions:
				if myPubIp == inst_desc["PublicIpAddress"]:
					continue
				fo.write("%s\n" % inst_desc["PublicIpAddress"])

		Cons.P("Created %s %d" % (fn_pssh_hn_el, os.path.getsize(fn_pssh_hn_el)))

		with open(fn_dc_ip_map, "w") as fo:
			for inst_desc in inst_descriptions:
				az = inst_desc["Placement"]["AvailabilityZone"]
				dc = az[:-1]
				ip = inst_desc["PublicIpAddress"]
				fo.write("%s %s\n" % (dc, ip))
		Cons.P("Created %s %d" % (fn_dc_ip_map, os.path.getsize(fn_dc_ip_map)))
