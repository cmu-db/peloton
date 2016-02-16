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
 * benchmark_time.c -- benchmark_time module definitions
 */
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include "benchmark_time.h"

#define	NSECPSEC	1000000000

/*
 * benchmark_time_get -- get timestamp from clock source
 */
void
benchmark_time_get(benchmark_time_t *time)
{
	clock_gettime(CLOCK_MONOTONIC, time);
}

/*
 * benchmark_time_diff_dummy -- get time interval for dummy operation
 */
void
benchmark_time_diff_dummy(benchmark_time_t *d, benchmark_time_t *t1,
		benchmark_time_t *t2)
{
	long long int nsecs = (t2->tv_sec  - t1->tv_sec) * NSECPSEC +
		t2->tv_nsec - t1->tv_nsec;
	if (nsecs >= 0)
		benchmark_time_diff(d, t1, t2);
	else
		memset(d, 0, sizeof (*d));
}

/*
 * benchmark_time_diff -- get time interval
 */
void
benchmark_time_diff(benchmark_time_t *d, benchmark_time_t *t1,
		benchmark_time_t *t2)
{
	long long int nsecs = (t2->tv_sec  - t1->tv_sec) * NSECPSEC +
		t2->tv_nsec - t1->tv_nsec;
	assert(nsecs >= 0);
	d->tv_sec = nsecs / NSECPSEC;
	d->tv_nsec = nsecs % NSECPSEC;
}

/*
 * benchmark_time_get_secs -- get total number of seconds
 */
double
benchmark_time_get_secs(benchmark_time_t *t)
{
	return (double)t->tv_sec + (double)t->tv_nsec / NSECPSEC;
}

/*
 * benchmark_time_get_nsecs -- get total number of nanoseconds
 */
unsigned long long
benchmark_time_get_nsecs(benchmark_time_t *t)
{
	unsigned long long ret = t->tv_nsec;

	ret += t->tv_sec * NSECPSEC;

	return ret;
}
