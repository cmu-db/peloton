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
# src/deps/jemalloc.mk -- rules for jemalloc
#

ifeq ($(DEBUG),1)
OBJDIR = debug
else
OBJDIR = nondebug
endif

JEMALLOC_DIR = $(realpath ../jemalloc)
ifeq ($(OBJDIR),$(abspath $(OBJDIR)))
JEMALLOC_OBJDIR = $(OBJDIR)/jemalloc
else
JEMALLOC_OBJDIR = ../$(OBJDIR)/jemalloc
endif
JEMALLOC_MAKEFILE = $(JEMALLOC_OBJDIR)/Makefile
JEMALLOC_CFG = $(JEMALLOC_DIR)/configure
JEMALLOC_CFG_AC = $(JEMALLOC_DIR)/configure.ac
JEMALLOC_LIB_AR = libjemalloc_pic.a
JEMALLOC_LIB = $(JEMALLOC_OBJDIR)/lib/$(JEMALLOC_LIB_AR)
JEMALLOC_CFG_IN_FILES = $(shell find $(JEMALLOC_DIR) -name "*.in")
JEMALLOC_CFG_GEN_FILES = $(JEMALLOC_CFG_IN_FILES:.in=)
JEMALLOC_CFG_OUT_FILES = $(patsubst $(JEMALLOC_DIR)/%, $(JEMALLOC_OBJDIR)/%, $(JEMALLOC_CFG_GEN_FILES))
JEMALLOC_AUTOM4TE_CACHE=autom4te.cache
.NOTPARALLEL: $(JEMALLOC_CFG_OUT_FILES)
JEMALLOC_CONFIG_FILE = $(JEMALLOC_DIR)/jemalloc.cfg
JEMALLOC_CONFIG = $(shell cat $(JEMALLOC_CONFIG_FILE))
CFLAGS_FILTER += -fno-common
CFLAGS_FILTER += -Wmissing-prototypes
CFLAGS_FILTER += -Wpointer-arith
CFLAGS_FILTER += -Wunused-macros
CFLAGS_FILTER += -Wmissing-field-initializers
CFLAGS_FILTER += -Wunreachable-code-return
CFLAGS_FILTER += -Wmissing-variable-declarations
CFLAGS_FILTER += -Weverything
CFLAGS_FILTER += -Wextra
CFLAGS_FILTER += -Wsign-conversion
CFLAGS_FILTER += -Wsign-compare
CFLAGS_FILTER += -Wconversion
CFLAGS_FILTER += -Wunused-parameter
CFLAGS_FILTER += -Wpadded
CFLAGS_FILTER += -Wcast-align
CFLAGS_FILTER += -Wvla
CFLAGS_FILTER += -Wpedantic
CFLAGS_FILTER += -Wshadow
CFLAGS_FILTER += -Wdisabled-macro-expansion
CFLAGS_FILTER += -Wlanguage-extension-token
JEMALLOC_CFLAGS=$(filter-out $(CFLAGS_FILTER), $(CFLAGS))
JEMALLOC_REMOVE_LDFLAGS_TMP = -Wl,--warn-common
JEMALLOC_LDFLAGS=$(filter-out $(JEMALLOC_REMOVE_LDFLAGS_TMP), $(LDFLAGS))

jemalloc $(JEMALLOC_LIB): $(JEMALLOC_CFG_OUT_FILES)
	$(MAKE) objroot=$(JEMALLOC_OBJDIR)/ -f $(JEMALLOC_MAKEFILE) -C $(JEMALLOC_DIR) all

$(JEMALLOC_CFG_OUT_FILES): $(JEMALLOC_CFG) $(JEMALLOC_CONFIG_FILE)
	$(MKDIR) -p $(JEMALLOC_OBJDIR)
	$(RM) -f $(JEMALLOC_CFG_OUT_FILES)
	cd $(JEMALLOC_OBJDIR) && \
		CFLAGS="$(JEMALLOC_CFLAGS)" LDFLAGS="$(JEMALLOC_LDFLAGS)" $(JEMALLOC_DIR)/configure $(JEMALLOC_CONFIG)

$(JEMALLOC_CFG): $(JEMALLOC_CFG_AC)
	cd $(JEMALLOC_DIR) && \
	    autoconf

jemalloc-clean:
	@if [ -f $(JEMALLOC_MAKEFILE) ];\
	then\
		$(MAKE) cfgoutputs_out+=$(JEMALLOC_MAKEFILE) objroot=$(JEMALLOC_OBJDIR)/ -f $(JEMALLOC_MAKEFILE) -C $(JEMALLOC_DIR) clean;\
	fi

jemalloc-clobber:
	@if [ -f $(JEMALLOC_MAKEFILE) ];\
	then\
		$(MAKE) cfgoutputs_out+=$(JEMALLOC_MAKEFILE) objroot=$(JEMALLOC_OBJDIR)/ -f $(JEMALLOC_MAKEFILE) -C $(JEMALLOC_DIR) distclean;\
	fi
	$(RM) $(JEMALLOC_CFG) $(JEMALLOC_CFG_GEN_FILES) $(JEMALLOC_CFG_OUT_FILES)
	$(RM) -r $(JEMALLOC_OBJDIR)
	$(RM) -r $(JEMALLOC_AUTOM4TE_CACHE)

jemalloc-test: jemalloc
	$(MAKE) objroot=$(JEMALLOC_OBJDIR)/ -f $(JEMALLOC_MAKEFILE) -C $(JEMALLOC_DIR) tests

jemalloc-check: jemalloc-test
	$(MAKE) objroot=$(JEMALLOC_OBJDIR)/ -f $(JEMALLOC_MAKEFILE) -C $(JEMALLOC_DIR) check

.PHONY: jemalloc jemalloc-clean jemalloc-clobber jemalloc-test jemalloc-check
