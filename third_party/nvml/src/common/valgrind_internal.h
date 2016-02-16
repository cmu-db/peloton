/*
 * Copyright (c) 2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * valgrind_internal.h -- internal definitions for valgrind macros
 */

#ifdef USE_VALGRIND
#define	USE_VG_PMEMCHECK
#define	USE_VG_HELGRIND
#define	USE_VG_MEMCHECK
#endif

#if defined(USE_VG_PMEMCHECK) || defined(USE_VG_HELGRIND) ||\
	defined(USE_VG_MEMCHECK)
extern unsigned On_valgrind;
#include <valgrind/valgrind.h>
#else
#define	On_valgrind (0)
#endif

#ifdef USE_VG_HELGRIND
#include <valgrind/helgrind.h>

#define	VALGRIND_ANNOTATE_HAPPENS_BEFORE(obj) do {\
	if (On_valgrind) \
		ANNOTATE_HAPPENS_BEFORE((obj));\
} while (0)

#define	VALGRIND_ANNOTATE_HAPPENS_AFTER(obj) do {\
	if (On_valgrind) \
		ANNOTATE_HAPPENS_AFTER((obj));\
} while (0)

#else

#define	VALGRIND_ANNOTATE_HAPPENS_BEFORE(obj) do { (void)(obj); } while (0)

#define	VALGRIND_ANNOTATE_HAPPENS_AFTER(obj) do { (void)(obj); } while (0)

#endif

#ifdef USE_VG_PMEMCHECK

#include <valgrind/pmemcheck.h>

#define	VALGRIND_REGISTER_PMEM_MAPPING(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_REGISTER_PMEM_MAPPING((addr), (len));\
} while (0)

#define	VALGRIND_REGISTER_PMEM_FILE(desc, base_addr, size, offset) do {\
	if (On_valgrind)\
		VALGRIND_PMC_REGISTER_PMEM_FILE((desc), (base_addr), (size), \
		(offset));\
} while (0)

#define	VALGRIND_REMOVE_PMEM_MAPPING(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_REMOVE_PMEM_MAPPING((addr), (len));\
} while (0)

#define	VALGRIND_CHECK_IS_PMEM_MAPPING(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_CHECK_IS_PMEM_MAPPING((addr), (len));\
} while (0)

#define	VALGRIND_PRINT_PMEM_MAPPINGS do {\
	if (On_valgrind)\
		VALGRIND_PMC_PRINT_PMEM_MAPPINGS;\
} while (0)

#define	VALGRIND_DO_FLUSH(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_DO_FLUSH((addr), (len));\
} while (0)

#define	VALGRIND_DO_FENCE do {\
	if (On_valgrind)\
		VALGRIND_PMC_DO_FENCE;\
} while (0)

#define	VALGRIND_DO_COMMIT do {\
	if (On_valgrind)\
		VALGRIND_PMC_DO_COMMIT;\
} while (0)

#define	VALGRIND_DO_PERSIST(addr, len) do {\
	if (On_valgrind) {\
		VALGRIND_PMC_DO_FLUSH((addr), (len));\
		VALGRIND_PMC_DO_FENCE;\
		VALGRIND_PMC_DO_COMMIT;\
		VALGRIND_PMC_DO_FENCE;\
	}\
} while (0)

#define	VALGRIND_SET_CLEAN(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_SET_CLEAN(addr, len);\
} while (0)

#define	VALGRIND_WRITE_STATS do {\
	if (On_valgrind)\
		VALGRIND_PMC_WRITE_STATS;\
} while (0)

#define	VALGRIND_LOG_STORES do {\
	if (On_valgrind)\
		VALGRIND_PMC_LOG_STORES;\
} while (0)

#define	VALGRIND_NO_LOG_STORES do {\
	if (On_valgrind)\
		VALGRIND_PMC_NO_LOG_STORES;\
} while (0)

#define	VALGRIND_ADD_LOG_REGION(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_ADD_LOG_REGION((addr), (len));\
} while (0)

#define	VALGRIND_REMOVE_LOG_REGION(addr, len) do {\
	if (On_valgrind)\ \
		VALGRIND_PMC_REMOVE_LOG_REGION((addr), (len));\
} while (0)

#define	VALGRIND_FULL_REORDER do {\
	if (On_valgrind)\
		VALGRIND_PMC_FULL_REORDER;\
} while (0)

#define	VALGRIND_PARTIAL_REORDER do {\
	if (On_valgrind)\
		VALGRIND_PMC_PARTIAL_REORDER;\
} while (0)

#define	VALGRIND_ONLY_FAULT do {\
	if (On_valgrind)\
		VALGRIND_PMC_ONLY_FAULT;\
} while (0)

#define	VALGRIND_STOP_REORDER_FAULT do {\
	if (On_valgrind)\
		VALGRIND_PMC_STOP_REORDER_FAULT;\
} while (0)

#define	VALGRIND_START_TX do {\
	if (On_valgrind)\
		VALGRIND_PMC_START_TX;\
} while (0)

#define	VALGRIND_START_TX_N(txn) do {\
	if (On_valgrind)\
		VALGRIND_PMC_START_TX_N(txn);\
} while (0)

#define	VALGRIND_END_TX do {\
	if (On_valgrind)\
		VALGRIND_PMC_END_TX;\
} while (0)

#define	VALGRIND_END_TX_N(txn) do {\
	if (On_valgrind)\
		VALGRIND_PMC_END_TX_N(txn);\
} while (0)

#define	VALGRIND_ADD_TO_TX(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_ADD_TO_TX(addr, len);\
} while (0)

#define	VALGRIND_ADD_TO_TX_N(txn, addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_ADD_TO_TX_N(txn, addr, len);\
} while (0)

#define	VALGRIND_REMOVE_FROM_TX(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_REMOVE_FROM_TX(addr, len);\
} while (0)

#define	VALGRIND_REMOVE_FROM_TX_N(txn, addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_REMOVE_FROM_TX_N(txn, addr, len);\
} while (0)

#define	VALGRIND_ADD_TO_GLOBAL_TX_IGNORE(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_PMC_ADD_TO_GLOBAL_TX_IGNORE(addr, len);\
} while (0)

#else

#define	VALGRIND_REGISTER_PMEM_MAPPING(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_REGISTER_PMEM_FILE(desc, base_addr, size, offset) do {\
	(void) (desc);\
	(void) (base_addr);\
	(void) (size);\
	(void) (offset);\
} while (0)

#define	VALGRIND_REMOVE_PMEM_MAPPING(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_CHECK_IS_PMEM_MAPPING(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_PRINT_PMEM_MAPPINGS do {} while (0)

#define	VALGRIND_DO_FLUSH(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_DO_FENCE do {} while (0)

#define	VALGRIND_DO_COMMIT do {} while (0)

#define	VALGRIND_DO_PERSIST(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_SET_CLEAN(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_WRITE_STATS do {} while (0)

#define	VALGRIND_LOG_STORES do {} while (0)

#define	VALGRIND_NO_LOG_STORES do {} while (0)

#define	VALGRIND_ADD_LOG_REGION(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_REMOVE_LOG_REGION(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_FULL_REORDER do {} while (0)

#define	VALGRIND_PARTIAL_REORDER do {} while (0)

#define	VALGRIND_ONLY_FAULT do {} while (0)

#define	VALGRIND_STOP_REORDER_FAULT do {} while (0)

#define	VALGRIND_START_TX do {} while (0)

#define	VALGRIND_START_TX_N(txn) do { (void) (txn); } while (0)

#define	VALGRIND_END_TX do {} while (0)

#define	VALGRIND_END_TX_N(txn) do {\
	(void) (txn);\
} while (0)

#define	VALGRIND_ADD_TO_TX(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_ADD_TO_TX_N(txn, addr, len) do {\
	(void) (txn);\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_REMOVE_FROM_TX(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_REMOVE_FROM_TX_N(txn, addr, len) do {\
	(void) (txn);\
	(void) (addr);\
	(void) (len);\
} while (0)

#define	VALGRIND_ADD_TO_GLOBAL_TX_IGNORE(addr, len) do {\
	(void) (addr);\
	(void) (len);\
} while (0)

#endif

#ifdef USE_VG_MEMCHECK

#include <valgrind/memcheck.h>

#define	VALGRIND_DO_DISABLE_ERROR_REPORTING do {\
	if (On_valgrind)\
		VALGRIND_DISABLE_ERROR_REPORTING;\
} while (0)

#define	VALGRIND_DO_ENABLE_ERROR_REPORTING do {\
	if (On_valgrind)\
		VALGRIND_ENABLE_ERROR_REPORTING;\
} while (0)

#define	VALGRIND_DO_CREATE_MEMPOOL(pool, rzB, is_zeroed) do {\
	if (On_valgrind)\
		VALGRIND_CREATE_MEMPOOL(pool, rzB, is_zeroed);\
} while (0)

#define	VALGRIND_DO_DESTROY_MEMPOOL(pool) do {\
	if (On_valgrind)\
		VALGRIND_DESTROY_MEMPOOL(pool);\
} while (0)

#define	VALGRIND_DO_MEMPOOL_ALLOC(pool, addr, size) do {\
	if (On_valgrind && pool->is_master_replica)\
		VALGRIND_MEMPOOL_ALLOC(pool, addr, size);\
} while (0)

#define	VALGRIND_DO_MEMPOOL_FREE(pool, addr) do {\
	if (On_valgrind && pool->is_master_replica)\
		VALGRIND_MEMPOOL_FREE(pool, addr);\
} while (0)

#define	VALGRIND_DO_MEMPOOL_CHANGE(pool, addrA, addrB, size) do {\
	if (On_valgrind && pool->is_master_replica)\
		VALGRIND_MEMPOOL_CHANGE(pool, addrA, addrB, size);\
} while (0)

#define	VALGRIND_DO_MAKE_MEM_DEFINED(pool, addr, len) do {\
	if (On_valgrind && pool->is_master_replica)\
		VALGRIND_MAKE_MEM_DEFINED(addr, len);\
} while (0)

#define	VALGRIND_DO_MAKE_MEM_UNDEFINED(pool, addr, len) do {\
	if (On_valgrind && pool->is_master_replica)\
		VALGRIND_MAKE_MEM_UNDEFINED(addr, len);\
} while (0)

#define	VALGRIND_DO_MAKE_MEM_NOACCESS(pool, addr, len) do {\
	if (On_valgrind && pool->is_master_replica)\
		VALGRIND_MAKE_MEM_NOACCESS(addr, len);\
} while (0)

#define	VALGRIND_DO_CHECK_MEM_IS_ADDRESSABLE(addr, len) do {\
	if (On_valgrind)\
		VALGRIND_CHECK_MEM_IS_ADDRESSABLE(addr, len);\
} while (0)

#else

#define	VALGRIND_DO_DISABLE_ERROR_REPORTING do {} while (0)

#define	VALGRIND_DO_ENABLE_ERROR_REPORTING do {} while (0)

#define	VALGRIND_DO_CREATE_MEMPOOL(pool, rzB, is_zeroed)\
	do { (void) (pool); (void) (rzB); (void) (is_zeroed); } while (0)

#define	VALGRIND_DO_DESTROY_MEMPOOL(pool)\
	do { (void) (pool); } while (0)

#define	VALGRIND_DO_MEMPOOL_ALLOC(pool, addr, size)\
	do { (void) (pool); (void) (addr); (void) (size); } while (0)

#define	VALGRIND_DO_MEMPOOL_FREE(pool, addr)\
	do { (void) (pool); (void) (addr); } while (0)

#define	VALGRIND_DO_MEMPOOL_CHANGE(pool, addrA, addrB, size)\
	do {\
		(void) (pool); (void) (addrA); (void) (addrB); (void) (size);\
	} while (0)

#define	VALGRIND_DO_MAKE_MEM_DEFINED(pool, addr, len)\
	do { (void) (pool); (void) (addr); (void) (len); } while (0)

#define	VALGRIND_DO_MAKE_MEM_UNDEFINED(pool, addr, len)\
	do { (void) (pool); (void) (addr); (void) (len); } while (0)

#define	VALGRIND_DO_MAKE_MEM_NOACCESS(pool, addr, len)\
	do { (void) (pool); (void) (addr); (void) (len); } while (0)

#define	VALGRIND_DO_CHECK_MEM_IS_ADDRESSABLE(addr, len)\
	do { (void) (addr); (void) (len); } while (0)

#endif
