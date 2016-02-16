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
# pmempool.sh -- bash completion script for pmempool
#

#
# _pmempool_gen -- generates result for completion
# Arguments:
# $1 - values
# $2 - current string
# $3 - prefix for results
_pmempool_gen()
{
	COMPREPLAY=()
	local values=$1
	local cur=$2
	local prefix=$3
	local i=${#COMPREPLY[@]}
	for v in $values
	do
		[[ "$v" == "$cur"* ]] && COMPREPLY[i++]="$prefix$v"
	done
}

#
# _pmempool_get_cmds -- returns available pmempool commands
#
_pmempool_get_cmds()
{
	echo -n $(pmempool --help | grep -e '^\S\+\s\+-' |\
			grep -o '^\S\+' | sed '/help/d')
}

#
# _pmempool_get_opts -- returns available options for specified command
# Arguments:
# $1 - command
#
_pmempool_get_opts()
{
	local c=$1
	local opts=$(pmempool ${c} --help | grep -o -e "-., --\S\+" |\
			grep -o -e "--\S\+")
	echo "$opts"
}

#
# _pmempool_get_values -- returns available values for specified option
# Arguments:
# $1 - command
# $2 - option
# $3 - values delimiter
# $4 - current values, will be removed from result
#
_pmempool_get_values()
{
	local cmd=$1
	local opt=$2
	local delim=$3
	local curvals=$4
	local vals=$(pmempool ${cmd} --help |\
			grep -o -e "${opt}\s\+\S\+${delim}\S\+" |\
			sed "s/${opt}\s\+\(\S\+${delim}\S\+\)/\1/" |\
			sed "s/${delim}/ /g")
	if [ -n "$curvals" ]
	then
		local OLD_IFS=$IFS
		IFS=","
		for v in $curvals
		do
			vals=$(echo $vals | sed "s/$v//g")
		done
		IFS=$OLD_IFS
	fi
	echo "${vals}"
}

#
# _pmempool_get_cmd -- returns command name if exist in specified array
# Arguments:
# $1 - command name
# $2 - list of available commands
#
_pmempool_get_cmd()
{
	local cmd=$1
	local cmds=$2

	[[ ${cmds} =~ ${cmd} ]] && echo -n ${cmd}
}

#
# _pmempool_get_used_values -- returns already used values
# Arguments:
# $1 - current string
# $2 - values delimiter
#
_pmempool_get_used_values()
{
	local cur=$1
	local delim=$2
	local used=$(echo $cur | rev | cut -d $delim -s -f1 --complement | rev)
	[ -n "$used" ] && used="$used$delim"
	echo "$used"
}

#
# _pmempool_get_current_value -- returns current value string
# Arguments:
# $1 - current string
# $2 - values delimiter
#
_pmempool_get_current_value()
{
	local cur=$1
	local delim=$2
	echo $cur | rev | cut -d $delim -f1 | rev
}

_pmempool()
{
	local cur prev opts
	cur="${COMP_WORDS[COMP_CWORD]}"
	prev="${COMP_WORDS[COMP_CWORD-1]}"
	cmds=$(_pmempool_get_cmds)
	cmds_all="$cmds help"
	opts_pool_types="blk log obj"
	cmd=$(_pmempool_get_cmd "${COMP_WORDS[1]}" "$cmds_all")

	if [[ ${cur} == -* ]]
	then
		local opts=$(_pmempool_get_opts $cmd)
		_pmempool_gen "$opts" "$cur"
	elif [[ ${prev} == --* ]]
	then
		local used=$(_pmempool_get_used_values "$cur" ",")
		local _cur=$(_pmempool_get_current_value "$cur" ",")
		local values=$(_pmempool_get_values ${cmd} ${prev} "," $used)
		if [ -n "${values}" ]
		then
			# values separated by ',' may contain multiple values
			_pmempool_gen "$values" "$_cur" "$used"
		else
			# values separated by '|' may contain only one value
			values=$(_pmempool_get_values $cmd $prev "|")
			_pmempool_gen "$values" "$cur"
		fi
	elif [[ $cmd == create ]]
	then
		case "${COMP_WORDS[@]}" in
			*blk*|*log*|*obj*|*--inherit*)
				;;
			*)
				_pmempool_gen "$opts_pool_types" "$cur"
				;;
		esac
	elif [[ ${prev} == help ]]
	then
		_pmempool_gen "$cmds" "$cur"
	elif [[ ${prev} == pmempool ]]
	then
		_pmempool_gen "$cmds_all" "$cur"
	fi
}

complete -o default -F _pmempool pmempool
