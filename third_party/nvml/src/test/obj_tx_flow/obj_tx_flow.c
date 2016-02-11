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
 * obj_tx_flow.c -- unit test for transaction flow
 */
#include "unittest.h"
#include "libpmemobj.h"
#include "util.h"

#define	LAYOUT_NAME "direct"

#define	TEST_VALUE_A 5
#define	TEST_VALUE_B 10
#define	TEST_VALUE_C 15

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_tx_flow");

	if (argc != 2)
		FATAL("usage: %s [file]", argv[0]);

	PMEMobjpool *pop;
	if ((pop = pmemobj_create(argv[1], LAYOUT_NAME, PMEMOBJ_MIN_POOL,
	    S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create");

	int a = 0;
	int b = 0;
	int c = 0;

	TX_BEGIN(pop) {
		a = TEST_VALUE_A;
	} TX_ONCOMMIT {
		ASSERT(a == TEST_VALUE_A);
		b = TEST_VALUE_B;
	} TX_ONABORT { /* not called */
		a = TEST_VALUE_B;
	} TX_FINALLY {
		ASSERT(b == TEST_VALUE_B);
		c = TEST_VALUE_C;
	} TX_END

	ASSERT(a == TEST_VALUE_A);
	ASSERT(b == TEST_VALUE_B);
	ASSERT(c == TEST_VALUE_C);

	a = 0;
	b = 0;
	c = 0;

	TX_BEGIN(pop) {
		a = TEST_VALUE_A;
		pmemobj_tx_abort(EINVAL);
		a = TEST_VALUE_B;
	} TX_ONCOMMIT { /* not called */
		a = TEST_VALUE_B;
	} TX_ONABORT {
		ASSERT(a == TEST_VALUE_A);
		b = TEST_VALUE_B;
	} TX_FINALLY {
		ASSERT(b == TEST_VALUE_B);
		c = TEST_VALUE_C;
	} TX_END

	ASSERT(a == TEST_VALUE_A);
	ASSERT(b == TEST_VALUE_B);
	ASSERT(c == TEST_VALUE_C);

	a = 0;
	b = 0;
	c = 0;

	TX_BEGIN(pop) {
		TX_BEGIN(pop) {
			a = TEST_VALUE_A;
		} TX_ONCOMMIT {
			ASSERT(a == TEST_VALUE_A);
			b = TEST_VALUE_B;
		} TX_END
	} TX_ONCOMMIT {
		c = TEST_VALUE_C;
	} TX_END

	ASSERT(a == TEST_VALUE_A);
	ASSERT(b == TEST_VALUE_B);
	ASSERT(c == TEST_VALUE_C);

	a = 0;
	b = 0;
	c = 0;

	TX_BEGIN(pop) {
		a = TEST_VALUE_C;
		TX_BEGIN(pop) {
			a = TEST_VALUE_A;
			pmemobj_tx_abort(EINVAL);
			a = TEST_VALUE_B;
		} TX_ONCOMMIT { /* not called */
			a = TEST_VALUE_C;
		} TX_ONABORT {
			ASSERT(a == TEST_VALUE_A);
			b = TEST_VALUE_B;
		} TX_FINALLY {
			ASSERT(b == TEST_VALUE_B);
			c = TEST_VALUE_C;
		} TX_END
		a = TEST_VALUE_B;
	} TX_ONCOMMIT { /* not called */
		ASSERT(a == TEST_VALUE_A);
		c = TEST_VALUE_C;
	} TX_ONABORT {
		ASSERT(a == TEST_VALUE_A);
		ASSERT(b == TEST_VALUE_B);
		ASSERT(c == TEST_VALUE_C);
		a = TEST_VALUE_B;
	} TX_FINALLY {
		ASSERT(a == TEST_VALUE_B);
		b = TEST_VALUE_A;
	} TX_END

	ASSERT(a == TEST_VALUE_B);
	ASSERT(b == TEST_VALUE_A);
	ASSERT(c == TEST_VALUE_C);

	a = 0;
	b = 0;
	c = 0;

	pmemobj_tx_begin(pop, NULL, TX_LOCK_NONE);
	pmemobj_tx_abort(EINVAL);
	ASSERT(pmemobj_tx_stage() == TX_STAGE_ONABORT);
	a = TEST_VALUE_A;
	pmemobj_tx_end();

	ASSERT(a == TEST_VALUE_A);

	pmemobj_close(pop);

	DONE(NULL);
}
