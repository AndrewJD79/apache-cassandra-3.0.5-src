#!/usr/bin/env python

import os
import subprocess
import sys

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util


fn_dep_class_path = ".dep-class-path"


def BuildSrc():
	# Change cwd to this dir
	dn_this = os.path.dirname(os.path.realpath(__file__))
	os.chdir(dn_this)

	proj_name = "FetchOnDemand"
	need_to_build_dep = False
	fn_jar = "target/FetchOnDemand-0.1.jar"
	need_to_build_pkg = False

	if not os.path.isfile(fn_dep_class_path):
		need_to_build_dep = True

	if not os.path.isfile(fn_jar):
		need_to_build_pkg = True

	# Check modification times
	mt_src = Util.RunSubp("find src -name \"*.java\" -printf \"%T@\\n\" | sort | tail -n 1"
			, shell = True, print_cmd = False, print_result = False).strip()
	#Cons.P(mt_src)

	# No need to build dependency file everytime source files change
	#if ! $need_to_build_dep ; then
	#	MT_DEP=`find . -maxdepth 1 -name $fn_dep_class_path -printf "%T@\n"`
	#	if [[ "$MT_DEP" < "$mt_src" ]] ; then
	#		printf "The dependency file is outdated\n  %s\n  %s\n" $mt_src $MT_DEP
	#		need_to_build_dep=true
	#	fi
	#fi

	if need_to_build_pkg == False:
		mt_jar = Util.RunSubp("find target -maxdepth 1 -name \"FetchOnDemand-*.jar\" -printf \"%T@\\n\""
				, shell = True, print_cmd = False, print_result = False).strip()
		#Cons.P(mt_jar)

		if mt_jar < mt_src:
			#printf "The package file is outdated\n  %s\n  %s\n" $mt_src $mt_jar
			need_to_build_pkg = True


	# Build package first so that you can terminate early on errors
	if need_to_build_pkg:
		with Cons.MeasureTime("Building package file ..."):
			Util.RunSubp("mvn package -Dmaven.test.skip=true")

	if need_to_build_dep:
		with Cons.MeasureTime("Generating dependency file ..."):
			Util.RunSubp("mvn dependency:build-classpath -Dmdep.outputFile=%s" % fn_dep_class_path)
			# Force update last modification time
			Util.RunSubp("touch %s" % fn_dep_class_path, print_cmd = False, print_result = False)


def RunLocal(cur_datetime):
	with Cons.MeasureTime("Running locally ..."):
		Util.RunSubp("killall -qw FetchOnDemand-0.1.ja[r] || true", shell = True)

		cmd = "java -cp target/FetchOnDemand-0.1.jar:`cat %s` FetchOnDemand %s" % (fn_dep_class_path, cur_datetime)
		#Util.RunSubp(cmd, shell = True)
		# TODO: debugging
		_RunSubp(cmd)


def _RunSubp(cmd):
	Cons.P(cmd)
	p = subprocess.Popen(cmd, shell=True)
	stdouterr = p.communicate()[0]

#	# communidate() waits for termination
#	stdouterr = p.communicate()[0]
#	rc = p.returncode
#	if rc != 0:
#		raise RuntimeError("Error: cmd=[%s] rc=%d stdouterr=[%s]" % (cmd, rc, stdouterr))
#	if print_result and (len(stdouterr) > 0):
#		Cons.P(stdouterr)
#	return stdouterr


def main(argv):
	if len(argv) != 2:
		print "Usage: %s cur_datetime (used for identifying each run)" % argv[0]
		print "  E.g.: %s 160507-191237" % argv[0]
		sys.exit(1)

	cur_datetime = argv[1]

	BuildSrc()
	RunLocal(cur_datetime)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
