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
 * btt.h -- btt module definitions
 */

/* callback functions passed to btt_init() */
struct ns_callback {
	int (*nsread)(void *ns, unsigned lane,
		void *buf, size_t count, uint64_t off);
	int (*nswrite)(void *ns, unsigned lane,
		const void *buf, size_t count, uint64_t off);
	int (*nszero)(void *ns, unsigned lane, size_t count, uint64_t off);
	ssize_t (*nsmap)(void *ns, unsigned lane, void **addrp,
			size_t len, uint64_t off);
	void (*nssync)(void *ns, unsigned lane, void *addr, size_t len);

	int ns_is_zeroed;
};

struct btt *btt_init(uint64_t rawsize, uint32_t lbasize, uint8_t parent_uuid[],
		unsigned maxlane, void *ns, const struct ns_callback *ns_cbp);
unsigned btt_nlane(struct btt *bttp);
size_t btt_nlba(struct btt *bttp);
int btt_read(struct btt *bttp, unsigned lane, uint64_t lba, void *buf);
int btt_write(struct btt *bttp, unsigned lane, uint64_t lba, const void *buf);
int btt_set_zero(struct btt *bttp, unsigned lane, uint64_t lba);
int btt_set_error(struct btt *bttp, unsigned lane, uint64_t lba);
int btt_check(struct btt *bttp);
void btt_fini(struct btt *bttp);
