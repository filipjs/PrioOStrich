# -*- coding: utf-8 -*-
import os

"""
Script to remove unnecessary files.
"""

extensions = [".pyc"]

for tree in os.walk('./'):
    for filename in tree[2]:                        # tree[2] -> files in directory
        file_ext = os.path.splitext(filename)[1]    # splitext()[1] -> file extension

        # also remove temp files (ending with '~')
        if file_ext in extensions or filename[-1] == "~":
            file_path = os.path.join(tree[0], filename)
            print "deleting file", file_path
            os.remove(file_path)
