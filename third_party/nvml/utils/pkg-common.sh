#!/bin/bash
#
# Copyright (c) 2014, Intel Corporation
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
# pkg-common.sh - common functions and variables for building packages
#

export LC_ALL="C"

function error() {
	echo -e "error: $@"
}

function check_dir() {
	if [ ! -d $1 ]
	then
		error "Directory '$1' does not exist."
		exit 1
	fi
}

function check_file() {
	if [ ! -f $1 ]
	then
		error "File '$1' does not exist."
		exit 1
	fi
}

function check_tool() {
	local tool=$1
	if [ -z "$(which $tool 2>/dev/null)" ]
	then
		error "'${tool}' not installed or not in PATH"
		exit 1
	fi
}

function format_version () {
	echo $1 | sed 's|0*\([0-9]\+\)|\1|g'
}

function get_version_item() {
	local INPUT=$1
	local TARGET=$2
	local REGEX="([^0-9]*)(([0-9]+\.){,2}[0-9]+)([.+-]?.*)"

	if [[ $INPUT =~ $REGEX ]]
	then
		local VERSION="${BASH_REMATCH[2]}"
		local RELEASE="${BASH_REMATCH[4]}"

		case $TARGET in
			version)
				echo -n $VERSION
				;;
			release)
				echo -n $RELEASE
				;;
			*)
				error "Wrong target"
				exit 1
				;;
		esac
	else
		error "Wrong tag format"
		exit 1
	fi
}

function get_version() {
	local VERSION=$(get_version_item $1 version)
	local RELEASE=$(get_version_item $1 release)

	VERSION=$(format_version $VERSION)

	if [ -z $RELEASE ]
	then
		echo -n $VERSION
	else
		RELEASE=${RELEASE//[-:_.]/"~"}
		echo -n ${VERSION}${RELEASE}
	fi
}

REGEX_DATE_AUTHOR="([a-zA-Z]{3} [a-zA-Z]{3} [0-9]{2} [0-9]{4})\s*(.*)"
REGEX_MESSAGE_START="\s*\*\s*(.*)"
REGEX_MESSAGE="\s*(\S.*)"
