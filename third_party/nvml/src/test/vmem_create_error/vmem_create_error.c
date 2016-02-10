/*
 * Copyright (c) 2014, Intel Corporation
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
 * vmem_create_error.c -- unit test for vmem_create_error
 *
 * usage: vmem_create_error
 */

#include "unittest.h"

static char mem_pool[VMEM_MIN_POOL];

int
main(int argc, char *argv[])
{
	VMEM *vmp;

	START(argc, argv, "vmem_create_error");

	if (argc > 1)
		FATAL("usage: %s", argv[0]);

	errno = 0;
	vmp = vmem_create_in_region(mem_pool, 0);
	ASSERTeq(vmp, NULL);
	ASSERTeq(errno, EINVAL);

	errno = 0;
	vmp = vmem_create("./", 0);
	ASSERTeq(vmp, NULL);
	ASSERTeq(errno, EINVAL);

	errno = 0;
	vmp = vmem_create("invalid dir !@#$%^&*()=", VMEM_MIN_POOL);
	ASSERTeq(vmp, NULL);
	ASSERTne(errno, 0);

	DONE(NULL);
}
