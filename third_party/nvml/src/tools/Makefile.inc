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
# src/tools/Makefile.inc -- Makefile include for all tools
#

TOP := $(dir $(lastword $(MAKEFILE_LIST)))/../..
include $(TOP)/src/common.inc

INCS += -I.
INCS += -I$(TOP)/src/include
CFLAGS += -std=gnu99
CFLAGS += -Wall
CFLAGS += -Werror
CFLAGS += -Wmissing-prototypes
CFLAGS += -Wpointer-arith
CFLAGS += -Wunused-macros
CFLAGS += -Wmissing-field-initializers
CFLAGS += -Wsign-conversion
CFLAGS += -Wsign-compare
#CFLAGS += -Wconversion
CFLAGS += -fno-common
CFLAGS += $(EXTRA_CFLAGS)
CFLAGS += -O2 -D_FORTIFY_SOURCE=2
CFLAGS += -DSRCVERSION='"$(SRCVERSION)"'
ifeq ($(call check_flag, -Wunreachable-code-return), y)
CFLAGS += -Wunreachable-code-return
endif
ifeq ($(call check_flag, -Wmissing-variable-declarations), y)
CFLAGS += -Wmissing-variable-declarations
endif

LDFLAGS += -Wl,-z,relro -Wl,--warn-common -Wl,--fatal-warnings $(EXTRA_LDFLAGS) -L$(TOP)/src/nondebug
TOOLSDIR=$(prefix)/bin
TARGET_DIR=$(DESTDIR)$(TOOLSDIR)
BASH_COMP_FILES ?=
BASH_COMP_DIR =$(DESTDIR)/etc/bash_completion.d/
CSTYLE=$(TOP)/utils/cstyle

TARGET_STATIC_NONDEBUG=$(TARGET).static-nondebug
TARGET_STATIC_DEBUG=$(TARGET).static-debug

LIBSDIR=$(TOP)/src

LIBS += -pthread -luuid

ifeq ($(LIBPMEMBLK), y)
DYNAMIC_LIBS += -lpmemblk
STATIC_DEBUG_LIBS += $(LIBSDIR)/debug/libpmemblk.a
STATIC_NONDEBUG_LIBS += $(LIBSDIR)/nondebug/libpmemblk.a
endif

ifeq ($(LIBPMEMLOG), y)
DYNAMIC_LIBS += -lpmemlog
STATIC_DEBUG_LIBS += $(LIBSDIR)/debug/libpmemlog.a
STATIC_NONDEBUG_LIBS += $(LIBSDIR)/nondebug/libpmemlog.a
endif

ifeq ($(LIBPMEMOBJ), y)
DYNAMIC_LIBS += -lpmemobj
STATIC_DEBUG_LIBS += $(LIBSDIR)/debug/libpmemobj.a
STATIC_NONDEBUG_LIBS += $(LIBSDIR)/nondebug/libpmemobj.a
endif

ifeq ($(LIBPMEM),y)
DYNAMIC_LIBS += -lpmem
STATIC_DEBUG_LIBS += $(LIBSDIR)/debug/libpmem.a
STATIC_NONDEBUG_LIBS += $(LIBSDIR)/nondebug/libpmem.a
endif

ifeq ($(LIBVMEM),y)
DYNAMIC_LIBS += -lvmem
STATIC_DEBUG_LIBS += $(LIBSDIR)/debug/libvmem.a
STATIC_NONDEBUG_LIBS += $(LIBSDIR)/nondebug/libvmem.a
endif

all: $(TARGET) $(TARGET_STATIC_NONDEBUG) $(TARGET_STATIC_DEBUG)

clean:
	$(RM) $(OBJS) $(CLEAN_FILES)

clobber: clean
	$(RM) $(TARGET)
	$(RM) $(TARGET_STATIC_NONDEBUG)
	$(RM) $(TARGET_STATIC_DEBUG)
	$(RM) -r .deps

cstyle:
	$(CSTYLE) *.[ch]

install: all
ifneq ($(TARGET),)
	install -d $(TARGET_DIR)
	install -p -m 0755 $(TARGET) $(TARGET_DIR)
endif
ifneq ($(BASH_COMP_FILES),)
	install -d $(BASH_COMP_DIR)
	install -p -m 0644 $(BASH_COMP_FILES) $(BASH_COMP_DIR)
endif

uninstall:
ifneq ($(TARGET),)
	$(RM) $(TARGET_DIR)/$(TARGET)
endif
ifneq ($(BASH_COMP_FILES),)
	$(RM) $(BASH_COMP_DIR)/$(BASH_COMP_FILES)
endif

%.gz: %
	gzip -c ./$< > $@

%.txt: %
	man ./$< > $@

$(TARGET_STATIC_DEBUG): $(OBJS) $(STATIC_DEBUG_LIBS)
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(STATIC_DEBUG_LIBS) $(LIBS)

$(TARGET_STATIC_NONDEBUG): $(OBJS) $(STATIC_NONDEBUG_LIBS)
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(STATIC_NONDEBUG_LIBS) $(LIBS)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $(OBJS) $(LIBS) $(DYNAMIC_LIBS)

%.o: %.c
	@mkdir -p .deps
	$(CC) -MD $(CFLAGS) $(INCS) -c -o $@ $<
	@cp $*.d .deps/$*.P; \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
	    -e '/^$$/ d' -e 's/$$/ :/' < $*.d >> .deps/$*.P; \
	$(RM) -f $*.d

test check pcheck: all

.PHONY: all clean clobber cstyle install uninstall test check pcheck

-include .deps/*.P
