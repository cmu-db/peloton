#
# src/test/testconfig.sh -- local configuration for unit tests
#
# This file tells the script unittest/unittest.sh which file system
# locations are to be used for testing.
#

#
# For tests that use trivial space and don't care what type of file system,
# just putting the files in the same directory as the test is usually most
# convenient.  But if there's some reason to put these small testfiles
# elsewhere, set the path here.
#
LOCAL_FS_DIR=.

#
# For tests that require true persistent memory, set the path to a directory
# on a PMEM-aware file system here.  Comment this line out if there's no
# actual persistent memory available on this system.
#
#PMEM_FS_DIR=/mnt/pmem

#
# For tests that require true a non-persitent memory aware file system (i.e.
# to verify something works on traditional page-cache based memory-mapped
# files) set the path to a directory on a normal file system here.
#
NON_PMEM_FS_DIR=/tmp
