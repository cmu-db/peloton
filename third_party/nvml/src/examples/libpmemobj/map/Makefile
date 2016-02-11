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
# examples/libpmemobj/map/Makefile -- build the map example
#

PROGS = mapcli data_store
LIBRARIES = map_ctree map_btree map_rbtree\
	    map_hashmap_atomic map_hashmap_tx\
	    map

LIBS = -lpmemobj -pthread

include ../../Makefile.inc

CFLAGS += -I../
CFLAGS += -I../hashmap
CFLAGS += -I../tree_map

mapcli: mapcli.o libmap.a
data_store: data_store.o libmap.a

libmap_ctree.o: map_ctree.o map.o ../tree_map/libctree_map.a
libmap_btree.o: map_btree.o map.o ../tree_map/libbtree_map.a
libmap_rbtree.o: map_rbtree.o map.o ../tree_map/librbtree_map.a
libmap_hashmap_atomic.o: map_hashmap_atomic.o map.o ../hashmap/libhashmap_atomic.a
libmap_hashmap_tx.o: map_hashmap_tx.o map.o ../hashmap/libhashmap_tx.a

libmap.o: map.o map_ctree.o map_btree.o map_rbtree.o\
	map_hashmap_atomic.o map_hashmap_tx.o\
	../tree_map/libctree_map.a\
	../tree_map/libbtree_map.a\
	../tree_map/librbtree_map.a\
	../hashmap/libhashmap_atomic.a\
	../hashmap/libhashmap_tx.a

../tree_map/libctree_map.a:
	$(MAKE) -C ../tree_map ctree_map

../tree_map/libbtree_map.a:
	$(MAKE) -C ../tree_map btree_map

../tree_map/librbtree_map.a:
	$(MAKE) -C ../tree_map rbtree_map

../hashmap/libhashmap_atomic.a:
	$(MAKE) -C ../hashmap hashmap_atomic

../hashmap/libhashmap_tx.a:
	$(MAKE) -C ../hashmap hashmap_tx
