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
 * vmem.h -- internal definitions for libvmem
 */

#define	VMEM_LOG_PREFIX "libvmem"
#define	VMEM_LOG_LEVEL_VAR "VMEM_LOG_LEVEL"
#define	VMEM_LOG_FILE_VAR "VMEM_LOG_FILE"

/* attributes of the vmem memory pool format for the pool header */
#define	VMEM_HDR_SIG "VMEM   "	/* must be 8 bytes including '\0' */
#define	VMEM_FORMAT_MAJOR 1
#define	VMEM_FORMAT_COMPAT 0x0000
#define	VMEM_FORMAT_INCOMPAT 0x0000
#define	VMEM_FORMAT_RO_COMPAT 0x0000

extern unsigned long Pagesize;

struct vmem {
	struct pool_hdr hdr;	/* memory pool header */

	void *addr;	/* mapped region */
	size_t size;	/* size of mapped region */
	int caller_mapped;
};

void vmem_init(void);
