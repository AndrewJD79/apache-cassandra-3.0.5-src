#!/usr/bin/env python

import sys

sys.path.insert(0, "/home/ubuntu/work/acorn-tools/util/python")
import Cons
import Util


def _EditCassConf():
	cmd = "sed -i 's/" \
			"^    use_attr_user:.*" \
			"/    use_attr_user: true" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^    use_attr_topic:.*" \
			"/    use_attr_topic: false" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)



def main(argv):
	_EditCassConf()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
