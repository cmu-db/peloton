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
# src/benchmarks/Makefile -- build all benchmarks
#
OBJCOPY=objcopy

vpath %.c ../examples/libpmemobj/tree_map
vpath %.c ../examples/libpmemobj/map
vpath %.c ../examples/libpmemobj/hashmap

TOP := $(dir $(lastword $(MAKEFILE_LIST)))/../..
include $(TOP)/src/common.inc

vpath %.c ../libpmemobj
vpath %.c ../common

BENCHMARK = pmembench

GLIB:= $(shell pkg-config glib-2.0 && echo "y" || echo "n")
ifeq ($(GLIB),y)
TARGET = $(BENCHMARK)
else
TARGET = no-glib
endif

SRC=pmembench.c\
    benchmark_time.c\
    benchmark_worker.c\
    clo.c\
    clo_vec.c\
    config_reader.c\
    scenario.c\
    log.c\
    blk.c\
    vmem.c\
    pmem_memset.c\
    pmem_memcpy.c\
    pmemobj_gen.c\
    obj_pmalloc.c\
    obj_locks.c\
    obj_lanes.c\
    map_bench.c\
    pmemobj_tx.c

# Configuration file without the .cfg extension
CONFIGS=pmembench_log\
	pmembench_blk\
	pmembench_vmem\
	pmembench_memset\
	pmembench_memcpy\
	pmembench_obj_pmalloc\
	pmembench_obj_gen\
	pmembench_obj_locks\
	pmembench_obj_lanes\
	pmembench_map\
	pmembench_tx

OBJS=$(SRC:.c=.o)
LDFLAGS = -L$(LIBS_PATH)
LDFLAGS += -L../examples/libpmemobj/map
LDFLAGS += $(EXTRA_LDFLAGS)
LIBS += -lpmemobj -lpmemlog -lpmemblk -lpmem -lvmem -pthread -lm -luuid
CFLAGS  = -std=gnu99
CFLAGS += -Wall
CFLAGS += -Werror
CFLAGS += -Wmissing-prototypes
CFLAGS += -Wpointer-arith
CFLAGS += -Wunused-macros
CFLAGS += -pthread
CFLAGS += $(EXTRA_CFLAGS)
CFLAGS += -I../include
CFLAGS += -I../libpmemobj
CFLAGS += -I../common
CFLAGS += -I../examples/libpmemobj/map
CFLAGS += $(shell pkg-config --cflags glib-2.0)
LIBS += $(shell pkg-config --libs glib-2.0)

LIBMAP_DIR=../examples/libpmemobj/map
LIBMAP=$(LIBMAP_DIR)/libmap.a

OBJS += pmemobj.o

ifeq ($(DEBUG),)
CFLAGS += -O3
LIBS_PATH=../nondebug
else
CFLAGS += -ggdb
LIBS_PATH=../debug
endif

GLIB_TEST_PROG="\#include <glib.h>\nint main(){return 0;}"
GLIB_SILENCE := $(shell printf $(GLIB_TEST_PROG) |\
	$(CC) $(CFLAGS) -x c -o /dev/null - 2>/dev/null && echo n || echo y)

ifeq ($(GLIB_SILENCE), y)
CFLAGS += -Wno-unknown-attributes
endif

default: $(TARGET)

no-glib:
	@echo "glib-2.0 development package required"

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

$(BENCHMARK): $(OBJS) $(LIBMAP)
	$(CC) -o $@ $(LDFLAGS) $^ $(LIBS)

$(LIBMAP):
	$(MAKE) -C $(LIBMAP_DIR) map

clean:
	$(RM) $(OBJS)
	$(RM) *.csv

clobber: clean
	$(RM) $(BENCHMARK)
	$(RM) *.csv

cstyle:
	$(CSTYLE) *.[ch]

$(CONFIGS):
	LD_LIBRARY_PATH=$(LIBS_PATH) ./$(BENCHMARK) $@.cfg > $@.csv

run: $(BENCHMARK) $(CONFIGS)

.PHONY: all clean clobber run cstyle no-glib $(CONFIGS)

PMEMOBJ_SYMBOLS=pmalloc pfree lane_hold lane_release

pmemobj.o: $(LIBS_PATH)/libpmemobj/libpmemobj_unscoped.o
	objcopy --localize-hidden $(addprefix -G, $(PMEMOBJ_SYMBOLS)) $< $@
