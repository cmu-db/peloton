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
# src/test/Makefile -- build all unit tests
#
# Makefile -- build all unit tests
#

TEST_DEPS = \
       unittest\
       tools

BLK_TESTS = \
       blk_nblock\
       blk_non_zero\
       blk_pool\
       blk_pool_lock\
       blk_recovery\
       blk_rw\
       blk_rw_mt

LOG_TESTS = \
       log_basic\
       log_pool\
       log_pool_lock\
       log_recovery\
       log_walker

OBJ_DEPS = obj_list
# long tests first
OBJ_TESTS = \
       obj_basic_integration\
       obj_many_size_allocs\
       obj_realloc\
       obj_sync\
       \
       obj_bucket\
       obj_check\
       obj_ctree\
       obj_cuckoo\
       obj_debug\
       obj_direct\
       obj_heap\
       obj_heap_state\
       obj_lane\
       obj_list_insert\
       obj_list_insert_new\
       obj_list_move\
       obj_list_move_oob\
       obj_list_realloc\
       obj_list_realloc_move\
       obj_list_recovery\
       obj_list_remove\
       obj_list_remove_free\
       obj_list_valgrind\
       obj_list_macro\
       obj_memcheck\
       obj_out_of_memory\
       obj_persist_count\
       obj_pmalloc_basic\
       obj_pmalloc_mt\
       obj_pmalloc_oom_mt\
       obj_pool\
       obj_pool_lock\
       obj_recovery\
       obj_recreate\
       obj_redo_log\
       obj_store\
       obj_tx_alloc\
       obj_tx_add_range\
       obj_tx_add_range_direct\
       obj_tx_flow\
       obj_tx_free\
       obj_tx_locks\
       obj_tx_locks_abort\
       obj_tx_realloc\
       obj_tx_strdup\
       obj_pool_lookup\
       obj_first

OTHER_TESTS = \
       arch_flags\
       checksum\
       magic\
       out_err\
       out_err_mt\
       pmemspoil\
       scope\
       set_funcs\
       traces\
       traces_custom_function\
       traces_pmem\
       util_file_create\
       util_file_open\
       util_map_proc\
       util_poolset\
       util_poolset_parse

PMEM_TESTS = \
       pmem_isa_proc\
       pmem_is_pmem\
       pmem_is_pmem_proc\
       pmem_map\
       pmem_memcpy\
       pmem_memmove\
       pmem_memset\
       pmem_movnt\
       pmem_movnt_align\
       pmem_valgr_simple

PMEMPOOL_TESTS = \
       pmempool_check\
       pmempool_create\
       pmempool_dump\
       pmempool_help\
       pmempool_info\
       pmempool_rm

VMEM_TESTS = \
       vmem_aligned_alloc\
       vmem_calloc\
       vmem_check_allocations\
       vmem_check_version\
       vmem_check\
       vmem_create\
       vmem_create_error\
       vmem_create_in_region\
       vmem_custom_alloc\
       vmem_delete\
       vmem_malloc\
       vmem_malloc_usable_size\
       vmem_mix_allocations\
       vmem_multiple_pools\
       vmem_out_of_memory\
       vmem_pages_purging\
       vmem_realloc\
       vmem_realloc_inplace\
       vmem_stats\
       vmem_strdup\
       vmem_valgrind

VMMALLOC_TESTS = \
       vmmalloc_calloc\
       vmmalloc_check_allocations\
       vmmalloc_fork\
       vmmalloc_init\
       vmmalloc_malloc\
       vmmalloc_malloc_hooks\
       vmmalloc_malloc_usable_size\
       vmmalloc_memalign\
       vmmalloc_out_of_memory\
       vmmalloc_realloc\
       vmmalloc_valgrind\
       vmmalloc_valloc

EXAMPLES_TESTS = \
       ex_libpmem\
       ex_libpmemblk\
       ex_libpmemlog\
       ex_libpmemobj

TESTS = \
       $(TEST_DEPS)\
       $(OBJ_DEPS)\
       $(OBJ_TESTS)\
       $(BLK_TESTS)\
       $(LOG_TESTS)\
       $(OTHER_TESTS)\
       $(PMEM_TESTS)\
       $(PMEMPOOL_TESTS)\
       $(VMEM_TESTS)\
       $(VMMALLOC_TESTS)\
       $(EXAMPLES_TESTS)

all     : TARGET = all
clean   : TARGET = clean
clobber : TARGET = clobber
test    : TARGET = test
cstyle  : TARGET = cstyle
check   : TARGET = check
pcheck  : TARGET = pcheck

TEST_TYPE = check
TEST_BUILD = all
TEST_FS = all
TEST_TIME = 3m
MEMCHECK = auto

all test cstyle: $(TESTS)

clean clobber: $(TESTS)

$(OBJ_DEPS) $(OBJ_TESTS) $(BLK_TESTS) $(LOG_TESTS) $(OTHER_TESTS) $(PMEM_TESTS) $(PMEMPOOL_TESTS) $(VMEM_TESTS) $(VMMALLOC_TESTS): $(TEST_DEPS)

$(OBJ_TESTS): $(OBJ_DEPS)

$(TESTS):
	$(MAKE) -C $@ $(TARGET)

memcheck-summary:
	grep ERROR */memcheck*.log

memcheck-summary-errors:
	grep ERROR */memcheck*.log | grep -v " 0 errors" || true

memcheck-summary-leaks:
	grep "in use at exit" */memcheck*.log | grep -v " 0 bytes in 0 blocks" || true

check: test
	@./RUNTESTS -b $(TEST_BUILD) -t $(TEST_TYPE) -f $(TEST_FS) -o $(TEST_TIME) -m $(MEMCHECK)
	@echo "No failures."

pcheck: $(TESTS)
	@echo "No failures."

pcheck_blk: TARGET = pcheck
pcheck_blk: $(BLK_TESTS)
	@echo "No failures."

pcheck_log: TARGET = pcheck
pcheck_log: $(LOG_TESTS)
	@echo "No failures."

pcheck_obj: TARGET = pcheck
pcheck_obj: $(OBJ_TESTS)
	@echo "No failures."

pcheck_other: TARGET = pcheck
pcheck_other: $(OTHER_TESTS)
	@echo "No failures."

pcheck_pmem: TARGET = pcheck
pcheck_pmem: $(PMEM_TESTS)
	@echo "No failures."

pcheck_pmempool: TARGET = pcheck
pcheck_pmempool: $(PMEMPOOL_TESTS)
	@echo "No failures."

pcheck_vmem: TARGET = pcheck
pcheck_vmem: $(VMEM_TESTS)
	@echo "No failures."

pcheck_vmmalloc: TARGET = pcheck
pcheck_vmmalloc: $(VMMALLOC_TESTS)
	@echo "No failures."

.PHONY: all check clean clobber cstyle pcheck pcheck_blk pcheck_log pcheck_obj\
        pcheck_other pcheck_pmem pcheck_pmempool pcheck_vmem pcheck_vmmalloc\
        test unittest $(TESTS) $(TEST_DEPS)
