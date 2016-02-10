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
 * bucket.h -- internal definitions for bucket
 */

struct bucket;

struct bucket *bucket_new(size_t unit_size, unsigned unit_max);
void bucket_delete(struct bucket *b);

int bucket_is_small(struct bucket *b);
uint32_t bucket_calc_units(struct bucket *b, size_t size);
size_t bucket_unit_size(struct bucket *b);
unsigned bucket_unit_max(struct bucket *b);
void bucket_insert_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block m);
int bucket_get_rm_block_bestfit(struct bucket *b, struct memory_block *m);
int bucket_get_rm_block_exact(struct bucket *b, struct memory_block m);
int bucket_get_block_exact(struct bucket *b, struct memory_block m);
int bucket_lock(struct bucket *b);
int bucket_is_empty(struct bucket *b);
unsigned bucket_bitmap_nval(struct bucket *b);
uint64_t bucket_bitmap_lastval(struct bucket *b);
unsigned bucket_bitmap_nallocs(struct bucket *b);
void bucket_unlock(struct bucket *b);
