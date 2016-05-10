#!/usr/bin/env python

import datetime
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
			Util.RunSubp("mvn package -Dmaven.test.skip=true 2>&1", shell = True)

	if need_to_build_dep:
		with Cons.MeasureTime("Generating dependency file ..."):
			Util.RunSubp("mvn dependency:build-classpath -Dmdep.outputFile=%s 2>&1" % fn_dep_class_path, shell = True)
			# Force update last modification time
			Util.RunSubp("touch %s" % fn_dep_class_path, print_cmd = False, print_result = False)


def RunLocal(cur_datetime):
	with Cons.MeasureTime("Running locally ..."):
		Util.RunSubp("killall -qw FetchOnDemand-0.1.ja[r] || true", shell = True)

		cmd = "java -cp target/FetchOnDemand-0.1.jar:`cat %s` FetchOnDemand %s 2>&1" % (fn_dep_class_path, cur_datetime)
		Util.RunSubp(cmd, shell = True)


def main(argv):
	cur_datetime = None
	if len(argv) == 1:
		cur_datetime = datetime.datetime.utcnow().strftime("%y%m%d-%H%M%S")
		Cons.P("Experiment date time not provided. Using current datetime %s" % cur_datetime)
	elif len(argv) == 2:
		cur_datetime = argv[1]
	else:
		print "Usage: %s cur_datetime (used for identifying each run)" % argv[0]
		print "  E.g.: %s 160507-191237" % argv[0]
		sys.exit(1)

	BuildSrc()
	RunLocal(cur_datetime)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
