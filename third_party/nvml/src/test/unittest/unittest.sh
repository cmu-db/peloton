#
# Copyright (c) 2014-2015, Intel Corporation
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in
#       the documentation and/or other materials provided with the
#       distribution.
#
#     * Neither the name of Intel Corporation nor the names of its
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

. ../testconfig.sh

# defaults
[ "$TEST" ] || export TEST=check
[ "$FS" ] || export FS=local
[ "$BUILD" ] || export BUILD=debug
[ "$MEMCHECK" ] || export MEMCHECK=auto

# force globs to fail if they don't match
shopt -s failglob

#
# For non-static build testing, the variable TEST_LD_LIBRARY_PATH is
# constructed so the test pulls in the appropriate library from this
# source tree.  To override this behavior (i.e. to force the test to
# use the libraries installed elsewhere on the system), set
# TEST_LD_LIBRARY_PATH and this script will not override it.
#
# For example, in a test directory, run:
#	TEST_LD_LIBRARY_PATH=/usr/lib ./TEST0
#
[ "$TEST_LD_LIBRARY_PATH" ] || {
	case "$BUILD"
	in
	debug)
		TEST_LD_LIBRARY_PATH=../../debug
		;;
	nondebug)
		TEST_LD_LIBRARY_PATH=../../nondebug
		;;
	esac
}

#
# When running static binary tests, append the build type to the binary
#
case "$BUILD"
in
	static-*)
		EXESUFFIX=.$BUILD
		;;
esac

#
# The variable DIR is constructed so the test uses that directory when
# constructing test files.  DIR is chosen based on the fs-type for
# this test, and if the appropriate fs-type doesn't have a directory
# defined in testconfig.sh, the test is skipped.
#
# This behavior can be overridden by setting DIR.  For example:
#	DIR=/force/test/dir ./TEST0
#
curtestdir=`basename $PWD`

# just in case
if [ ! -n "$curtestdir" ]; then
	exit 1
fi

curtestdir=test_$curtestdir

if [ ! -n "$UNITTEST_NUM" ]; then
	echo "UNITTEST_NUM does not have a value" >&2
	exit 1
fi

if [ "$DIR" ]; then
	 DIR=$DIR/$curtestdir$UNITTEST_NUM
else
	case "$FS"
	in
	local)
		DIR=$LOCAL_FS_DIR/$curtestdir$UNITTEST_NUM
		;;
	pmem)
		DIR=$PMEM_FS_DIR/$curtestdir$UNITTEST_NUM
		;;
	non-pmem)
		DIR=$NON_PMEM_FS_DIR/$curtestdir$UNITTEST_NUM
		;;
	esac
	[ "$DIR" ] || {
		[ "$UNITTEST_QUIET" ] || echo "$UNITTEST_NAME: SKIP fs-type $FS (not configured)"
		exit 0
	}
fi

#
# The default is to turn on library logging to level 3 and save it to local files.
# Tests that don't want it on, should override these environment variables.
#
export VMEM_LOG_LEVEL=3
export VMEM_LOG_FILE=vmem$UNITTEST_NUM.log
export PMEM_LOG_LEVEL=3
export PMEM_LOG_FILE=pmem$UNITTEST_NUM.log
export PMEMBLK_LOG_LEVEL=3
export PMEMBLK_LOG_FILE=pmemblk$UNITTEST_NUM.log
export PMEMLOG_LOG_LEVEL=3
export PMEMLOG_LOG_FILE=pmemlog$UNITTEST_NUM.log
export PMEMOBJ_LOG_LEVEL=3
export PMEMOBJ_LOG_FILE=pmemobj$UNITTEST_NUM.log

export VMMALLOC_POOL_DIR="$DIR"
export VMMALLOC_POOL_SIZE=$((16 * 1024 * 1024))
export VMMALLOC_LOG_LEVEL=3
export VMMALLOC_LOG_FILE=vmmalloc$UNITTEST_NUM.log

export MEMCHECK_LOG_FILE=memcheck_${BUILD}_${UNITTEST_NUM}.log
export VALIDATE_MEMCHECK_LOG=1

#
# create_file -- create zeroed out files of a given length in megs
#
# example, to create two files, each 1GB in size:
#	create_file 1024 testfile1 testfile2
#
function create_file() {
	size=$1
	shift
	for file in $*
	do
		dd if=/dev/zero of=$file bs=1M count=$size >> prep$UNITTEST_NUM.log
	done
}

#
# create_nonzeroed_file -- create non-zeroed files of a given length in megs
#
# A given first kilobytes of the file is zeroed out.
#
# example, to create two files, each 1GB in size, with first 4K zeroed
#	create_nonzeroed_file 1024 4 testfile1 testfile2
#
function create_nonzeroed_file() {
	offset=$2
	size=$(($1 * 1024 - $offset))
	shift 2
	for file in $*
	do
		truncate -s ${offset}K $file >> prep$UNITTEST_NUM.log
		dd if=/dev/zero bs=1K count=${size} 2>>prep$UNITTEST_NUM.log | tr '\0' '\132' >> $file
	done
}

#
# create_holey_file -- create holey files of a given length in megs
#
# example, to create two files, each 1GB in size:
#	create_holey_file 1024 testfile1 testfile2
#
function create_holey_file() {
	size=$1
	shift
	for file in $*
	do
		truncate -s ${size}M $file >> prep$UNITTEST_NUM.log
	done
}

#
# create_poolset -- create a dummy pool set
#
# Creates a pool set file using the provided list of part sizes and paths.
# Optionally, it also creates the selected part files (zeroed, partially zeroed
# or non-zeroed) with requested size and mode.  The actual file size may be
# different than the part size in the pool set file.
# 'r' or 'R' on the list of arguments indicate the beginning of the next
# replica set.
#
# Each part argument has the following format:
#   psize:ppath[:cmd[:fsize[:mode]]]
#
# where:
#   psize - part size
#   ppath - path
#   cmd   - (optional) can be:
#            x - do nothing (may be skipped if there's no 'fsize', 'mode')
#            z - create zeroed (holey) file
#            n - create non-zeroed file
#            h - create non-zeroed file, but with zeroed header (first 4KB)
#   fsize - (optional) the actual size of the part file (if 'cmd' is not 'x')
#   mode  - (optional) same format as for 'chmod' command
#
# example:
#   The following command define a pool set consisting of two parts: 16MB
#   and 32MB, and the replica with only one part of 48MB.  The first part file
#   is not created, the second is zeroed.  The only replica part is non-zeroed.
#   Also, the last file is read-only and its size does not match the information
#   from pool set file.
#
#	create_poolset ./pool.set 16M:testfile1 32M:testfile2:z \
#				R 48M:testfile3:n:11M:0400
#
function create_poolset() {
	psfile=$1
	shift 1
	echo "PMEMPOOLSET" > $psfile
	while [ "$1" ]
	do
		if [ "$1" = "R" ] || [ "$1" = "r" ]
		then
			echo "REPLICA" >> $psfile
			shift 1
			continue
		fi

		cmd=$1
		fparms=(${cmd//:/ })
		shift 1

		fsize=${fparms[0]}
		fpath=`readlink -mn ${fparms[1]}`
		cmd=${fparms[2]}
		asize=${fparams[3]}
		mode=${fparms[4]]}

		if [ ! $asize ]; then
			asize=$fsize
		fi

		case "$cmd"
		in
		x)
			# do nothing
			;;
		z)
			# zeroed (holey) file
			truncate -s $asize $fpath >> prep$UNITTEST_NUM.log
			;;
		n)
			# non-zeroed file
			dd if=/dev/zero bs=$asize count=1 2>>prep$UNITTEST_NUM.log | tr '\0' '\132' >> $fpath
			;;
		h)
			# non-zeroed file, except 4K header
			truncate -s 4K $fpath >> prep$UNITTEST_NUM.log
			dd if=/dev/zero bs=$asize count=1 2>>prep$UNITTEST_NUM.log | tr '\0' '\132' >> $fpath
			truncate -s $asize $fpath >> prep$UNITTEST_NUM.log
			;;
		esac

		if [ $mode ]; then
			chmod $mode $fpath
		fi

		echo "$fsize $fpath" >> $psfile
	done
}

#
# expect_normal_exit -- run a given command, expect it to exit 0
#
function expect_normal_exit() {
	if [ "$RUN_MEMCHECK" ]; then
		OLDTRACE="$TRACE"
		rm -f $MEMCHECK_LOG_FILE
		if echo "$*" | grep -v valgrind >/dev/null; then
			if [ "$MEMCHECK_DONT_CHECK_LEAKS" != "1" ]; then
				export OLD_MEMCHECK_OPTS="$MEMCHECK_OPTS"
				export MEMCHECK_OPTS="$MEMCHECK_OPTS --leak-check=full"
			fi

			TRACE="valgrind --log-file=$MEMCHECK_LOG_FILE $MEMCHECK_OPTS $TRACE"
		fi
	fi

	if [ "$MEMCHECK_DONT_CHECK_LEAKS" = "1" ]; then
		export OLD_ASAN_OPTIONS="${ASAN_OPTIONS}"
		export ASAN_OPTIONS="detect_leaks=0 ${ASAN_OPTIONS}"
	fi

	set +e
	eval $ECHO LD_LIBRARY_PATH=$TEST_LD_LIBRARY_PATH LD_PRELOAD=$TEST_LD_PRELOAD \
	$TRACE $*
	ret=$?
	set -e

	if [ "$ret" -ne "0" ]; then
		if [ "$ret" -gt "128" ]; then
			msg="crashed (signal $(($ret - 128)))"
		else
			msg="failed with exit code $ret"
		fi
		[ -t 2 ] && command -v tput >/dev/null && msg="$(tput setaf 1)$msg$(tput sgr0)"

		if [ -f err$UNITTEST_NUM.log ]; then
			if [ "$UNITTEST_QUIET" = "1" ]; then
				echo -e "$UNITTEST_NAME $msg. err$UNITTEST_NUM.log below." >&2
				cat err$UNITTEST_NUM.log >&2
			else
				echo -e "$UNITTEST_NAME $msg. err$UNITTEST_NUM.log above." >&2
			fi
		else
			echo -e "$UNITTEST_NAME $msg." >&2
		fi

		if [ "$RUN_MEMCHECK" ]; then
			echo "$MEMCHECK_LOG_FILE below." >&2
			ln=`wc -l < $MEMCHECK_LOG_FILE`
			paste -d " " <(yes $UNITTEST_NAME $MEMCHECK_LOG_FILE | head -n $ln) <(head -n $ln $MEMCHECK_LOG_FILE) >&2
		fi

		false
	fi

	if [ "$RUN_MEMCHECK" ]; then
		TRACE="$OLDTRACE"
		if [ -f $MEMCHECK_LOG_FILE -a "${VALIDATE_MEMCHECK_LOG}" = "1" ]; then
			if ! grep "ERROR SUMMARY: 0 errors" $MEMCHECK_LOG_FILE >/dev/null; then
				msg="failed"
				[ -t 2 ] && command -v tput >/dev/null && msg="$(tput setaf 1)$msg$(tput sgr0)"
				echo -e "$UNITTEST_NAME $msg with Valgrind. See $MEMCHECK_LOG_FILE. First 20 lines below." >&2
				paste -d " " <(yes $UNITTEST_NAME $MEMCHECK_LOG_FILE | head -n 20) <(head -n 20 $MEMCHECK_LOG_FILE) >&2
				false
			fi
		fi

		if [ "$MEMCHECK_DONT_CHECK_LEAKS" != "1" ]; then
			export MEMCHECK_OPTS="$OLD_MEMCHECK_OPTS"
		fi
	fi

	if [ "$MEMCHECK_DONT_CHECK_LEAKS" = "1" ]; then
		export ASAN_OPTIONS="${OLD_ASAN_OPTIONS}"
	fi
}

#
# expect_abnormal_exit -- run a given command, expect it to exit non-zero
#
function expect_abnormal_exit() {
	set +e
	eval $ECHO ASAN_OPTIONS="detect_leaks=0 ${ASAN_OPTIONS}" LD_LIBRARY_PATH=$TEST_LD_LIBRARY_PATH LD_PRELOAD=$TEST_LD_PRELOAD \
	$TRACE $*
	ret=$?
	set -e

	if [ "$ret" -eq "0" ]; then
		msg="succeeded"
		[ -t 2 ] && command -v tput >/dev/null && msg="$(tput setaf 1)$msg$(tput sgr0)"

		echo -e "$UNITTEST_NAME command $msg unexpectedly." >&2

		false
	fi
}

#
# require_unlimited_vm -- require unlimited virtual memory
#
# This implies requirements for:
# - overcommit_memory enabled (/proc/sys/vm/overcommit_memory is 0 or 1)
# - unlimited virtual memory (ulimit -v is unlimited)
#
function require_unlimited_vm() {
	local overcommit=$(cat /proc/sys/vm/overcommit_memory)
	local vm_limit=$(ulimit -v)
	[ "$overcommit" != "2" ] && [ "$vm_limit" = "unlimited" ] && return
	echo "$UNITTEST_NAME: SKIP required: overcommit_memory enabled and unlimited virtual memory"
	exit 0
}

#
# require_no_superuser -- require user without superuser rights
#
function require_no_superuser() {
	local user_id=$(id -u)
	[ "$user_id" != "0" ] && return
	echo "$UNITTEST_NAME: SKIP required: run without superuser rights"
	exit 0
}

#
# require_test_type -- only allow script to continue for a certain test type
#
function require_test_type() {
	for type in $*
	do
		[ "$type" = "$TEST" ] && return
	done
	[ "$UNITTEST_QUIET" ] || echo "$UNITTEST_NAME: SKIP test-type $TEST ($* required)"
	exit 0
}

#
# require_fs_type -- only allow script to continue for a certain fs type
#
function require_fs_type() {
	for type in $*
	do
		[ "$type" = "$FS" ] && return
	done
	[ "$UNITTEST_QUIET" ] || echo "$UNITTEST_NAME: SKIP fs-type $FS ($* required)"
	exit 0
}

#
# require_build_type -- only allow script to continue for a certain build type
#
function require_build_type() {
	for type in $*
	do
		[ "$type" = "$BUILD" ] && return
	done
	[ "$UNITTEST_QUIET" ] || echo "$UNITTEST_NAME: SKIP build-type $BUILD ($* required)"
	exit 0
}

#
# memcheck -- only allow script to continue when memcheck's settings match
#
function memcheck() {
	if [ "$1" = "force-enable" ]; then
		export RUN_MEMCHECK=1
	elif [ "$1" = "force-disable" ]; then
		if [ "$MEMCHECK" = "force-enable" ]; then
			echo "$UNITTEST_NAME: SKIP memcheck $1 vs $MEMCHECK"
			exit 0
		fi
	else
		echo "invalid memcheck parameter" >&2
		exit 1
	fi
}

#
# require_valgrind -- continue script execution only if
#	valgrind package is installed
#
function require_valgrind() {
	require_no_asan
	VALGRINDEXE=`which valgrind 2>/dev/null` && return
	echo "$UNITTEST_NAME: SKIP valgrind package required"
	exit 0
}

#
# require_valgrind_pmemcheck -- continue script execution only if
#	valgrind with pmemcheck is installed
#
function require_valgrind_pmemcheck() {
	require_valgrind
	local binary=$1
	[ -n "$binary" ] || binary=*
        strings ${binary} 2>&1 | \
            grep -q "compiled with support for Valgrind pmemcheck" && true
        if [ $? -ne 0 ]; then
            echo "$UNITTEST_NAME: SKIP not compiled with support for Valgrind pmemcheck"
            exit 0
        fi

	valgrind --tool=pmemcheck --help 2>&1 | \
		grep -q "pmemcheck is Copyright (c)" && true
        if [ $? -ne 0 ]; then
            echo "$UNITTEST_NAME: SKIP valgrind package with pmemcheck required"
            exit 0;
        fi

        return
}

#
# require_valgrind_helgrind -- continue script execution only if
#	valgrind with helgrind is installed
#
function require_valgrind_helgrind() {
	require_valgrind
	local binary=$1
	[ -n "$binary" ] || binary=*
        strings ${binary}.static-debug 2>&1 | \
            grep -q "compiled with support for Valgrind helgrind" && true
        if [ $? -ne 0 ]; then
            echo "$UNITTEST_NAME: SKIP not compiled with support for Valgrind helgrind"
            exit 0
        fi

	valgrind --tool=helgrind --help 2>&1 | \
		grep -q "Helgrind is Copyright (C)" && true
        if [ $? -ne 0 ]; then
            echo "$UNITTEST_NAME: SKIP valgrind package with helgrind required"
            exit 0;
        fi

        return
}

#
# require_valgrind_memcheck -- continue script execution only if
#	valgrind with memcheck is installed
#
function require_valgrind_memcheck() {
	require_valgrind
	local binary=$1
	[ -n "$binary" ] || binary=*
	strings ${binary} 2>&1 | \
		grep -q "compiled with support for Valgrind memcheck" && true
	if [ $? -ne 0 ]; then
		echo "$UNITTEST_NAME: SKIP not compiled with support for Valgrind memcheck"
		exit 0
	fi

	return
}

#
# set_valgrind_exe_name -- set the actual Valgrind executable name
#
# On some systems (Ubuntu), "valgrind" is a shell script that calls
# the actual executable "valgrind.bin".
# The wrapper script doesn't work well with LD_PRELOAD, so we want
# to call Valgrind directly.
#
function set_valgrind_exe_name() {
	VALGRINDDIR=`dirname $VALGRINDEXE`
	if [ -x $VALGRINDDIR/valgrind.bin ]; then
		VALGRINDEXE=$VALGRINDDIR/valgrind.bin
	fi
}

#
# require_valgrind_dev_3_7 -- continue script execution only if
#	version 3.7 (or later) of valgrind-devel package is installed
#
function require_valgrind_dev_3_7() {
	require_valgrind
	echo "
        #include <valgrind/valgrind.h>
        #if defined (VALGRIND_RESIZEINPLACE_BLOCK)
        VALGRIND_VERSION_3_7_OR_LATER
        #endif" | gcc ${EXTRA_CFLAGS} -E - 2>&1 | \
		grep -q "VALGRIND_VERSION_3_7_OR_LATER" && return
	echo "$UNITTEST_NAME: SKIP valgrind-devel package (ver 3.7 or later) required"
	exit 0
}

#
# require_valgrind_dev_3_8 -- continue script execution only if
#	version 3.8 (or later) of valgrind-devel package is installed
#
function require_valgrind_dev_3_8() {
	require_valgrind
	echo "
        #include <valgrind/valgrind.h>
        #if defined (__VALGRIND_MAJOR__) && defined (__VALGRIND_MINOR__)
        #if (__VALGRIND_MAJOR__ > 3) || \
             ((__VALGRIND_MAJOR__ == 3) && (__VALGRIND_MINOR__ >= 8))
        VALGRIND_VERSION_3_8_OR_LATER
        #endif
        #endif" | gcc ${EXTRA_CFLAGS} -E - 2>&1 | \
		grep -q "VALGRIND_VERSION_3_8_OR_LATER" && return
	echo "$UNITTEST_NAME: SKIP valgrind-devel package (ver 3.8 or later) required"
	exit 0
}

#
# require_valgrind_dev_3_10 -- continue script execution only if
#	version 3.10 (or later) of valgrind-devel package is installed
#
function require_valgrind_dev_3_10() {
	require_valgrind
	echo "
        #include <valgrind/valgrind.h>
        #if defined (__VALGRIND_MAJOR__) && defined (__VALGRIND_MINOR__)
        #if (__VALGRIND_MAJOR__ > 3) || \
             ((__VALGRIND_MAJOR__ == 3) && (__VALGRIND_MINOR__ >= 10))
        VALGRIND_VERSION_3_10_OR_LATER
        #endif
        #endif" | gcc ${EXTRA_CFLAGS} -E - 2>&1 | \
		grep -q "VALGRIND_VERSION_3_10_OR_LATER" && return
	echo "$UNITTEST_NAME: SKIP valgrind-devel package (ver 3.10 or later) required"
	exit 0
}

#
# require_no_asan_for - continue script execution only if passed binary does
#	NOT require libasan
#
function require_no_asan_for() {
	ASAN_ENABLED=`nm $1 | grep __asan_ | wc -l`
	if [ "$ASAN_ENABLED" != "0" ]; then
		echo "$UNITTEST_NAME: SKIP: ASAN enabled"
		exit 0
	fi
}

#
# require_no_asan - continue script execution only if libpmem does NOT require
#	libasan
#
function require_no_asan() {
	case "$BUILD"
	in
	debug)
		require_no_asan_for ../../debug/libpmem.so
		;;
	nondebug)
		require_no_asan_for ../../nondebug/libpmem.so
		;;
	static-debug)
		require_no_asan_for ../../debug/libpmem.a
		;;
	static-nondebug)
		require_no_asan_for ../../nondebug/libpmem.a
		;;
	esac
}

#
# setup -- print message that test setup is commencing
#
function setup() {
	# make sure we have a well defined locale for string operations here
	export LC_ALL="C"

	if [ "$MEMCHECK" = "force-enable" ]; then
		export RUN_MEMCHECK=1
	fi

	if [ "$RUN_MEMCHECK" ]; then
		MCSTR="/memcheck"
	else
		MCSTR=""
	fi

	echo "$UNITTEST_NAME: SETUP ($TEST/$FS/$BUILD$MCSTR)"
	if [ -d "$DIR" ]; then
		rm --one-file-system -rf -- $DIR
	fi
	mkdir $DIR
}

#
# check -- check test results (using .match files)
#
function check() {
	../match $(find . -regex "[^0-9]*${UNITTEST_NUM}\.log\.match" | xargs)
}

#
# pass -- print message that the test has passed
#
function pass() {
	msg="PASS"
	[ -t 1 ] && command -v tput >/dev/null && msg="$(tput setaf 2)$msg$(tput sgr0)"
	echo -e "$UNITTEST_NAME: $msg"
	rm --one-file-system -rf -- $DIR
}

TOOLS=../tools
# Paths to some useful tools
[ -n "$PMEMPOOL" ] || PMEMPOOL=../../tools/pmempool/pmempool
[ -n "$PMEMSPOIL" ] || PMEMSPOIL=$TOOLS/pmemspoil/pmemspoil.static-nondebug
[ -n "$PMEMWRITE" ] || PMEMWRITE=$TOOLS/pmemwrite/pmemwrite
[ -n "$PMEMALLOC" ] || PMEMALLOC=$TOOLS/pmemalloc/pmemalloc

# Length of pool file's signature
SIG_LEN=8

# Offset and length of pmemobj layout
LAYOUT_OFFSET=4096
LAYOUT_LEN=1024

# Length of arena's signature
ARENA_SIG_LEN=16

# Signature of BTT Arena
ARENA_SIG="BTT_ARENA_INFO"

# Offset to first arena
ARENA_OFF=8192

#
# check_file -- check if file exists and print error message if not
#
check_file()
{
	if [ ! -f $1 ]
	then
		echo "Missing file: ${1}" >&2
		exit 1
	fi
}

#
# check_files -- check if files exist and print error message if not
#
check_files()
{
	for file in $*
	do
		check_file $file
	done
}

#
# check_no_files -- check if files has been deleted and print error message if not
#
check_no_files()
{
	for file in $*
	do
		if [ -f $file ]
		then
			echo "Not deleted file: ${file}" >&2
			exit 1
		fi
	done
}

#
# get_size -- return size of file
#
get_size()
{
	stat -c%s $1
}

#
# get_mode -- return mode of file
#
get_mode()
{
	stat -c%a $1
}

#
# check_size -- validate file size
#
check_size()
{
	local size=$1
	local file=$2
	local file_size=$(get_size $file)

	if [[ $size != $file_size ]]
	then
		echo "error: wrong size ${file_size} != ${size}" >&2
		exit 1
	fi
}

#
# check_mode -- validate file mode
#
check_mode()
{
	local mode=$1
	local file=$2
	local file_mode=$(get_mode $file)

	if [[ $mode != $file_mode ]]
	then
		echo "error: wrong mode ${file_mode} != ${mode}" >&2
		exit 1
	fi
}

#
# check_signature -- check if file contains specified signature
#
check_signature()
{
	local sig=$1
	local file=$2
	local file_sig=$(dd if=$file bs=1 count=$SIG_LEN 2>/dev/null)

	if [[ $sig != $file_sig ]]
	then
		echo "error: $file: signature doesn't match ${file_sig} != ${sig}" >&2
		exit 1
	fi
}

#
# check_signatures -- check if multiple files contain specified signature
#
check_signatures()
{
	local sig=$1
	shift 1
	for file in $*
	do
		check_signature $sig $file
	done
}

#
# check_layout -- check if pmemobj pool contains specified layout
#
check_layout()
{
	local layout=$1
	local file=$2
	local file_layout=$(dd if=$file bs=1\
		skip=$LAYOUT_OFFSET count=$LAYOUT_LEN 2>/dev/null)

	if [[ $layout != $file_layout ]]
	then
		echo "error: layout doesn't match ${file_layout} != ${layout}" >&2
		exit 1
	fi
}

#
# check_arena -- check if file contains specified arena signature
#
check_arena()
{
	local file=$1
	local sig=$(dd if=$file bs=1 skip=$ARENA_OFF count=$ARENA_SIG_LEN 2>/dev/null)

	if [[ $sig != $ARENA_SIG ]]
	then
		echo "error: can't find arena signature" >&2
		exit 1
	fi
}

#
# dump_pool_info -- dump selected pool metadata and/or user data
#
function dump_pool_info() {
	# ignore selected header fields that differ by definition
	${PMEMPOOL}.static-nondebug info $* | sed -e "/^UUID/,/^Checksum/d"
}

#
# compare_replicas -- check replicas consistency by comparing `pmempool info` output
#
function compare_replicas() {
	set +e
	diff <(dump_pool_info $1 $2) <(dump_pool_info $1 $3)
	set -e
}
