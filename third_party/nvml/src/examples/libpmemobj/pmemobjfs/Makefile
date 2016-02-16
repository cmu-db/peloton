#
# Copyright (c) 2015, Intel Corporation
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
# examples/libpmemobj/pmemobjfs/Makefile -- build simple fuse base on libpmemobj
#

vpath %.c ../tree_map/
vpath %.c ../map/

FUSE := $(shell pkg-config fuse && echo "y" || echo "n")
ifeq ($(FUSE), y)
LINKS = mkfs.pmemobjfs\
       pmemobjfs.tx_begin\
       pmemobjfs.tx_commit\
       pmemobjfs.tx_abort
PROGS = pmemobjfs
endif

all-progs: $(LINKS)
clean-progs: clean-links

include ../../Makefile.inc

LIBS += -lpmemobj -lpmem -pthread $(shell pkg-config --libs fuse)
CFLAGS += $(shell pkg-config --cflags fuse)
CFLAGS += -I../tree_map
CFLAGS += -I../map
CFLAGS += -DFUSE_USE_VERSION=28
CFLAGS += -DUSE_CTREE

$(LINKS): pmemobjfs
	ln -sf pmemobjfs $@

clean-links:
	$(RM) $(LINKS)

pmemobjfs: pmemobjfs.o ../map/libmap_ctree.a
../map/libmap_ctree.a:
	$(MAKE) -C ../map map_ctree

.PHONY: clean-links
