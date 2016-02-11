/*
 * Copyright (c) 2014-2015, Intel Corporation
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
 * libvmem.c -- basic libvmem functions
 */
#include <stdio.h>
#include <stdint.h>

#include "libvmem.h"

#include "jemalloc.h"
#include "out.h"
#include "util.h"
#include "vmem.h"


/*
 * vmem_check_version -- see if library meets application version requirements
 */
const char *
vmem_check_version(unsigned major_required, unsigned minor_required)
{
	vmem_init();
	LOG(3, "major_required %u minor_required %u",
			major_required, minor_required);

	if (major_required != VMEM_MAJOR_VERSION) {
		ERR("libvmem major version mismatch (need %u, found %u)",
			major_required, VMEM_MAJOR_VERSION);
		return out_get_errormsg();
	}

	if (minor_required > VMEM_MINOR_VERSION) {
		ERR("libvmem minor version mismatch (need %u, found %u)",
			minor_required, VMEM_MINOR_VERSION);
		return out_get_errormsg();
	}

	return NULL;
}

/*
 * vmem_set_funcs -- allow overriding libvmem's call to malloc, etc.
 */
void
vmem_set_funcs(
		void *(*malloc_func)(size_t size),
		void (*free_func)(void *ptr),
		void *(*realloc_func)(void *ptr, size_t size),
		char *(*strdup_func)(const char *s),
		void (*print_func)(const char *s))
{
	vmem_init();
	LOG(3, NULL);

	util_set_alloc_funcs(malloc_func, free_func,
			realloc_func, strdup_func);
	out_set_print_func(print_func);
	je_vmem_pool_set_alloc_funcs(malloc_func, free_func);
}

/*
 * vmem_errormsg -- return last error message
 */
const char *
vmem_errormsg(void)
{
	return out_get_errormsg();
}
