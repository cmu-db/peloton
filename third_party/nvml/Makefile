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
# Makefile -- top-level Makefile for NVM Library
#
# Use "make" to build the library.
#
# Use "make test" to build unit tests.
#
# Use "make check" to run unit tests.
#
# Use "make clean" to delete all intermediate files (*.o, etc).
#
# Use "make clobber" to delete everything re-buildable (binaries, etc.).
#
# Use "make cstyle" to run cstyle on all C source files
#
# Use "make rpm" to build rpm packages
#
# Use "make dpkg" to build dpkg packages
#
# Use "make source DESTDIR=path_to_dir" to copy source files
# from HEAD to 'path_to_dir/nvml' directory.
#
# As root, use "make install" to install the library in the usual
# locations (/usr/local/lib, /usr/local/include, and /usr/local/share/man).
# You can provide custom directory prefix for installation using
# DESTDIR variable e.g.: "make install DESTDIR=/opt"
# You can override the prefix within DESTDIR using prefix variable
# e.g.: "make install prefix=/usr"

export SRCVERSION = $(shell git describe 2>/dev/null || cat .version)

export prefix = /usr/local

RPM_BUILDDIR=rpmbuild
DPKG_BUILDDIR=dpkgbuild
rpm : override DESTDIR=$(CURDIR)/$(RPM_BUILDDIR)
dpkg: override DESTDIR=$(CURDIR)/$(DPKG_BUILDDIR)
rpm dpkg: override prefix=/usr

all:
	$(MAKE) -C src $@
	$(MAKE) -C doc $@

clean:
	$(MAKE) -C src $@
	$(MAKE) -C doc $@
	$(RM) -r $(RPM_BUILDDIR) $(DPKG_BUILDDIR)

clobber:
	$(MAKE) -C src $@
	$(MAKE) -C doc $@
	$(RM) -r $(RPM_BUILDDIR) $(DPKG_BUILDDIR) rpm dpkg

test check pcheck: all
	$(MAKE) -C src $@

cstyle:
	$(MAKE) -C src $@
	@echo Checking files for whitespace issues...
	@utils/check_whitespace
	@echo Done.

source:
	$(if $(shell git rev-parse 2>&1), $(error Not a git repository))
	$(if $(shell git status --porcelain), $(error Working directory is dirty: $(shell git status --porcelain)))
	$(if $(DESTDIR), , $(error Please provide DESTDIR variable))
	mkdir -p $(DESTDIR)/nvml
	echo -n $(SRCVERSION) > $(DESTDIR)/nvml/.version
	git archive HEAD | tar -x -C $(DESTDIR)/nvml

pkg-clean:
	$(RM) -r $(DESTDIR)

rpm dpkg: pkg-clean source
	+utils/build-$@.sh $(SRCVERSION) $(DESTDIR)/nvml $(DESTDIR) $(CURDIR)/$@\
			$(CURDIR)/src/test/testconfig.sh

install uninstall:
	$(MAKE) -C src $@
	$(MAKE) -C doc $@

.PHONY: all clean clobber test check cstyle install uninstall\
	source rpm dpkg pkg-clean pcheck $(SUBDIRS)
