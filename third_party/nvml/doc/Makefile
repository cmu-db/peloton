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

#
# doc/Makefile -- Makefile for NVM library man pages
#
MANPAGES_3 = libpmem.3 libpmemblk.3 libpmemlog.3 libpmemobj.3 libvmem.3 \
	libvmmalloc.3
MANPAGES_1 = pmempool.1 pmempool-info.1 pmempool-create.1 \
	pmempool-check.1 pmempool-dump.1 pmempool-rm.1
MANPAGES = $(MANPAGES_1) $(MANPAGES_3)
TXTFILES = $(MANPAGES:=.txt)
HTMLFILES = $(MANPAGES:=.html)
GZFILES_1 = $(MANPAGES_1:=.gz)
GZFILES_3 = $(MANPAGES_3:=.gz)
GZFILES = $(GZFILES_1) $(GZFILES_3)
INSTALL_SECTIONS = install-1 install-3
UNINSTALL_SECTIONS = uninstall-1 uninstall-3
MANPAGES_DESTDIR = $(DESTDIR)$(prefix)/share/man/man

all: $(TXTFILES)

%.txt: %
	man ./$< > $@

html: $(HTMLFILES)

%.html: %
	groff -mandoc -Thtml ./$< > $@

compress: $(GZFILES)

%.gz: %
	gzip -c ./$< > $@

clean:

clobber: clean
	$(RM) -f $(TXTFILES) $(HTMLFILES) $(GZFILES)

install: compress
	install -d $(MANPAGES_DESTDIR)1
	install -p -m 0644 $(GZFILES_1) $(MANPAGES_DESTDIR)1
	install -d $(MANPAGES_DESTDIR)3
	install -p -m 0644 $(GZFILES_3) $(MANPAGES_DESTDIR)3

uninstall:
	$(foreach f, $(GZFILES_1), $(RM) $(MANPAGES_DESTDIR)1/$(f))
	$(foreach f, $(GZFILES_3), $(RM) $(MANPAGES_DESTDIR)3/$(f))

.PHONY: all html clean compress clobber cstyle install uninstall
