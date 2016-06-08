#!/usr/bin/env python

import sys

import Conf
import ExpLogs


def main(argv):
	Conf.Load()
	ExpLogs.Unzip()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
