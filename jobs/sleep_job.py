#!/usr/bin/env python2

import time, sys


def run(t=10):
    time.sleep(t)
    print "sleep finished:", t


if __name__=="__main__":
    if len(sys.argv) == 1:
        run()
    else:
        run(int(sys.argv[1]))
