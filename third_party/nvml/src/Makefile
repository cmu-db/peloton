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
# src/Makefile -- Makefile for NVML
#
TOP := $(dir $(lastword $(MAKEFILE_LIST)))/..
include $(TOP)/src/common.inc

TARGETS = libpmem libvmem libpmemblk libpmemlog libpmemobj libvmmalloc tools
ALL_TARGETS = $(TARGETS) examples benchmarks
CLEAN_TARGETS = $(ALL_TARGETS) test jemalloc
CLOBBER_TARGETS = $(ALL_TARGETS) test jemalloc
CSTYLE_TARGETS = $(ALL_TARGETS) common test
INSTALL_TARGETS = $(TARGETS)

HEADERS_DESTDIR = $(DESTDIR)$(prefix)/include
HEADERS_INSTALL = include/libpmem.h include/libvmem.h\
		include/libpmemobj.h include/libpmemblk.h\
		include/libpmemlog.h include/libvmmalloc.h

SCOPE_SRC_DIRS = $(TARGETS) include jemalloc/src
SCOPE_HDR_DIRS = $(TARGETS) include jemalloc/src\
		jemalloc/include/jemalloc\
		jemalloc/include/jemalloc/internal\
		debug/jemalloc/include/jemalloc\
		debug/jemalloc/include/jemalloc/internal\
		nondebug/jemalloc/include/jemalloc\
		nondebug/jemalloc/include/jemalloc/internal

SCOPE_SRC_FILES = $(foreach d, $(SCOPE_SRC_DIRS), $(wildcard $(d)/*.c))
SCOPE_HDR_FILES = $(foreach d, $(SCOPE_HDR_DIRS), $(wildcard $(D)/*.h))
SCOPEFILES = $(SCOPE_SRC_FILES) $(SCOPE_HDR_FILES)

all: $(ALL_TARGETS)
install: $(INSTALL_TARGETS:=-install)
uninstall: $(INSTALL_TARGETS:=-uninstall)
clean: $(CLEAN_TARGETS:=-clean)
clobber: $(CLOBBER_TARGETS:=-clobber)
cstyle: $(CSTYLE_TARGETS:=-cstyle)
examples benchmarks: $(TARGETS)
benchmarks: examples

custom_build = $(DEBUG)$(OBJDIR)

libvmmalloc libvmem: jemalloc
tools: libpmem libpmemblk libpmemlog
libpmemblk libpmemlog libpmemobj: libpmem

%-install: %
	$(MAKE) -C $* install
ifeq ($(custom_build),)
	$(MAKE) -C $* install DEBUG=1
endif

%-uninstall:
	$(MAKE) -C $* uninstall
ifeq ($(custom_build),)
	$(MAKE) -C $* uninstall DEBUG=1
endif

%-clean:
	$(MAKE) -C $* clean
ifeq ($(custom_build),)
	$(MAKE) -C $* clean DEBUG=1
endif

%-clobber:
	$(MAKE) -C $* clobber
ifeq ($(custom_build),)
	$(MAKE) -C $* clobber DEBUG=1
endif

%-cstyle:
	$(MAKE) -C $* cstyle

$(ALL_TARGETS):
	$(MAKE) -C $@
ifeq ($(custom_build),)
	$(MAKE) -C $@ DEBUG=1
endif

jemalloc-check: jemalloc-test

test: all jemalloc-test
	$(MAKE) -C test all

check pcheck: test jemalloc-check
	$(MAKE) -C test $@

jemalloc jemalloc-clean jemalloc-clobber jemalloc-test jemalloc-check:
	$(MAKE) -C jemalloc -f Makefile.nvml $@
ifeq ($(custom_build),)
	$(MAKE) -C jemalloc -f Makefile.nvml $@ DEBUG=1
endif

install: all
	install -d $(HEADERS_DESTDIR)
	install -p -m 0644 $(HEADERS_INSTALL) $(HEADERS_DESTDIR)

uninstall:
	$(foreach f, $(HEADERS_INSTALL), $(RM) $(HEADERS_DESTDIR)/$(notdir $(f)))

cstyle:
	$(CSTYLE) include/*.h

cscope:
	cscope -q -b $(SCOPEFILES)
	ctags -e $(SCOPEFILES)

clean:
	$(RM) tags cscope.in.out cscope.out cscope.po.out

.NOTPARALLEL: libvmem libvmmalloc

.PHONY: all install uninstall clean clobber cstyle test check pcheck\
	jemalloc jemalloc-clean jemalloc-test jemalloc-check cscope $(ALL_TARGETS)
