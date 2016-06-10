#!/usr/bin/env python

import datetime
import os
import subprocess
import sys

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util

import AcornUtil


fn_dep_class_path = ".dep-class-path"


def BuildSrc():
	# Change cwd to this dir
	dn_this = os.path.dirname(os.path.realpath(__file__))
	os.chdir(dn_this)

	need_to_build_dep = False
	fn_jar = "target/AcornYoutube-0.1.jar"
	need_to_build_pkg = False

	if not os.path.isfile(fn_dep_class_path):
		need_to_build_dep = True

	if not os.path.isfile(fn_jar):
		need_to_build_pkg = True

	# Check modification times
	mt_src = RunSubp("find src -name \"*.java\" -printf \"%T@\\n\" | sort | tail -n 1", print_output = False)
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
		mt_jar = RunSubp("find target -maxdepth 1 -name \"AcornYoutube-*.jar\" -printf \"%T@\\n\"", print_output = False).strip()
		#Cons.P(mt_jar)

		if mt_jar < mt_src:
			#printf "The package file is outdated\n  %s\n  %s\n" $mt_src $mt_jar
			need_to_build_pkg = True

	# Build package first so that you can terminate early on errors
	if need_to_build_pkg:
		with Cons.MT("Building package file ..."):
			RunSubp("mvn package -Dmaven.test.skip=true 2>&1")

	if need_to_build_dep:
		with Cons.MT("Generating dependency file ..."):
			RunSubp("mvn dependency:build-classpath -Dmdep.outputFile=%s 2>&1" % fn_dep_class_path)
			# Force update last modification time
			RunSubp("touch %s" % fn_dep_class_path)


def RunLocal(cur_datetime):
	with Cons.MT("Running locally ..."):
		RunSubp("killall -qw AcornYoutube-0.1.ja[r] || true")

		RunSubp("mkdir -p .run")
		cmd = "(java -cp target/AcornYoutube-0.1.jar:`cat %s` AcornYoutube %s 2>&1) | tee .run/current-AcornYoutube-stdout-stderr" \
				% (fn_dep_class_path, cur_datetime)
		RunSubp(cmd)


def RunSubp(cmd, env_ = os.environ.copy(), shell = True, print_output = True):
	# http://stackoverflow.com/questions/18421757/live-output-from-subprocess-command
	# It can read char by char depending on the requirements.
	lines = ""
	p = None
	if shell:
		p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE)
	else:
		p = subprocess.Popen(cmd.split(), shell=shell, stdout=subprocess.PIPE)
	for line in iter(p.stdout.readline, ''):
		if print_output:
			Cons.P(line.rstrip())
		lines += line
	p.wait()
	if p.returncode != 0:
		raise RuntimeError("Error: cmd=[%s] rc=%d" % (cmd, p.returncode))
	return lines


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

	# DC to public IP mapping is needed for fetch on demand requests.
	AcornUtil.GenHostfiles()

	BuildSrc()
	RunLocal(cur_datetime)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
