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
 * vmem_check.c -- unit test for vmem_check
 *
 * usage: vmem_check [directory]
 */

#include "unittest.h"

int
main(int argc, char *argv[])
{
	char *dir = NULL;
	void *mem_pool = NULL;
	VMEM *vmp;

	START(argc, argv, "vmem_check");

	if (argc == 2) {
		dir = argv[1];
	} else if (argc > 2) {
		FATAL("usage: %s [directory]", argv[0]);
	}

	if (dir == NULL) {
		/* allocate memory for function vmem_create_in_region() */
		mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL * 2, 4 << 20);

		vmp = vmem_create_in_region(mem_pool, VMEM_MIN_POOL);
		if (vmp == NULL)
			FATAL("!vmem_create_in_region");
	} else {
		vmp = vmem_create(dir, VMEM_MIN_POOL);
		if (vmp == NULL)
			FATAL("!vmem_create");
	}

	ASSERTeq(1, vmem_check(vmp));

	/* create pool in this same memory region */
	if (dir == NULL) {
		void *mem_pool2 = (void *)(((uintptr_t)mem_pool +
			VMEM_MIN_POOL / 2) & ~(Ut_pagesize - 1));

		VMEM *vmp2 = vmem_create_in_region(mem_pool2,
			VMEM_MIN_POOL);

		if (vmp2 == NULL)
			FATAL("!vmem_create_in_region");

		/* detect memory range collision */
		ASSERTne(1, vmem_check(vmp));
		ASSERTne(1, vmem_check(vmp2));

		vmem_delete(vmp2);

		ASSERTne(1, vmem_check(vmp2));
	}

	vmem_delete(vmp);

	/* for vmem_create() memory unmapped after delete pool */
	if (!dir)
		ASSERTne(1, vmem_check(vmp));

	DONE(NULL);
}
