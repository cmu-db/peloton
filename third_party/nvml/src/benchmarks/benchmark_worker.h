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
 * benchmark_worker.h -- benchmark_worker module declarations
 */

#include <pthread.h>
#include "benchmark.h"

/*
 *
 * The following table shows valid state transitions upon specified
 * API calls and operations performed by the worker thread:
 *
 * +========================+==========================+=============+
 * |       Application      |           State          |    Worker   |
 * +========================+==========================+=============+
 * | benchmark_worker_alloc | WORKER_STATE_IDLE        | wait        |
 * +------------------------+--------------------------+-------------+
 * | benchmark_worker_init  | WORKER_STATE_INIT        | invoke init |
 * +------------------------+--------------------------+-------------+
 * | wait                   | WORKER_STATE_INITIALIZED | end of init |
 * +------------------------+--------------------------+-------------+
 * | benchmark_worker_run   | WORKER_STATE_RUN         | invoke func |
 * +------------------------+--------------------------+-------------+
 * | benchmark_worker_join  | WORKER_STATE_END         | end of func |
 * +------------------------+--------------------------+-------------+
 * | benchmark_worker_exit  | WORKER_STATE_EXIT        | invoke exit |
 * +------------------------+--------------------------+-------------+
 * | wait                   | WORKER_STATE_DONE        | end of exit |
 * +------------------------+--------------------------+-------------+
 */
enum benchmark_worker_state {
	WORKER_STATE_IDLE,
	WORKER_STATE_INIT,
	WORKER_STATE_INITIALIZED,
	WORKER_STATE_RUN,
	WORKER_STATE_END,
	WORKER_STATE_EXIT,
	WORKER_STATE_DONE,

	MAX_WORKER_STATE,
};

struct benchmark_worker
{
	pthread_t thread;
	struct benchmark *bench;
	struct benchmark_args *args;
	struct worker_info info;
	int ret;
	int ret_init;
	int ret_exit;
	int (*func)(struct benchmark *bench, struct worker_info *info);
	int (*init)(struct benchmark *bench, struct benchmark_args *args,
			struct worker_info *info);
	int (*exit)(struct benchmark *bench, struct benchmark_args *args,
			struct worker_info *info);
	pthread_cond_t cond;
	pthread_mutex_t lock;
	enum benchmark_worker_state state;
};

struct benchmark_worker *benchmark_worker_alloc(void);
void benchmark_worker_free(struct benchmark_worker *);

int benchmark_worker_init(struct benchmark_worker *);
int benchmark_worker_exit(struct benchmark_worker *);
int benchmark_worker_run(struct benchmark_worker *);
int benchmark_worker_join(struct benchmark_worker *);
