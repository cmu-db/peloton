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
# src/test/unittest/Makefile -- build unittest support library
#

TOP := $(dir $(lastword $(MAKEFILE_LIST)))/../../..
include $(TOP)/src/common.inc

TARGET = libut.a
OBJS = ut.o ut_alloc.o ut_file.o ut_pthread.o ut_signal.o ut_backtrace.o
CFLAGS = -I../../include
CFLAGS += -std=gnu99
CFLAGS += -ggdb
CFLAGS += -Wall
CFLAGS += -Werror
CFLAGS += -Wmissing-prototypes
CFLAGS += -Wpointer-arith
CFLAGS += -Wunused-macros
CFLAGS += -Wmissing-field-initializers
CFLAGS += -Wsign-conversion
CFLAGS += -Wsign-compare
#CFLAGS += -Wconversion
CFLAGS += -pthread
CFLAGS += -fno-common
CFLAGS += $(EXTRA_CFLAGS)
ifeq ($(call check_flag, -Wunreachable-code-return), y)
CFLAGS += -Wunreachable-code-return
endif
ifeq ($(call check_flag, -Wmissing-variable-declarations), y)
CFLAGS += -Wmissing-variable-declarations
endif

ifneq ($(USE_LIBUNWIND),)
CFLAGS += $(shell pkg-config --cflags libunwind) -DUSE_LIBUNWIND
endif

all test: $(TARGET)

$(TARGET): $(OBJS)
	$(AR) rv $@ $(OBJS)

.c.o:
	@mkdir -p .deps
	$(CC) -MD -c $(CFLAGS) $(INCS) $(COMMONINCS) $< -o $@
	@cp $*.d .deps/$*.P; \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
	    -e '/^$$/ d' -e 's/$$/ :/' < $*.d >> .deps/$*.P; \
	$(RM) -f $*.d

clean:
	$(RM) *.o core a.out

cstyle:
	$(CSTYLE) *.[ch]

clobber: clean
	$(RM) $(TARGET)
	$(RM) -r .deps

test check pcheck: all

.PHONY: all test check clean clobber cstyle pcheck

-include .deps/*.P
