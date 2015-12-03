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
 * clo_vec.h -- command line options vector declarations
 */
#include <stdlib.h>
#include <sys/queue.h>

struct clo_vec_args
{
	TAILQ_ENTRY(clo_vec_args) next;
	void *args;
};

struct clo_vec_alloc
{
	TAILQ_ENTRY(clo_vec_alloc) next;
	void *ptr;
};

struct clo_vec_value
{
	TAILQ_ENTRY(clo_vec_value) next;
	void *ptr;
};

struct clo_vec_vlist
{
	TAILQ_HEAD(valueshead, clo_vec_value) head;
	size_t nvalues;
};

struct clo_vec
{
	size_t size;
	TAILQ_HEAD(argshead, clo_vec_args) args;
	size_t nargs;
	TAILQ_HEAD(allochead, clo_vec_alloc) allocs;
	size_t nallocs;
};

struct clo_vec *clo_vec_alloc(size_t size);
void clo_vec_free(struct clo_vec *clovec);
void *clo_vec_get_args(struct clo_vec *clovec, size_t i);
int clo_vec_add_alloc(struct clo_vec *clovec, void *ptr);
int clo_vec_memcpy(struct clo_vec *clovec, size_t off, size_t size, void *ptr);
int clo_vec_memcpy_list(struct clo_vec *clovec, size_t off, size_t size,
		struct clo_vec_vlist *list);
struct clo_vec_vlist *clo_vec_vlist_alloc(void);
void clo_vec_vlist_free(struct clo_vec_vlist *list);
void clo_vec_vlist_add(struct clo_vec_vlist *list, void *ptr, size_t size);
