import ftptools
import sys

if len(sys.argv) > 1:
    starting_dir = sys.argv[-1]
    ftptools.utils.iterate(starting_dir)
else:
    ftptools.utils.iterate()
