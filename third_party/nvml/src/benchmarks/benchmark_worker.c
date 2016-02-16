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
 * benchmark_worker.c -- benchmark_worker module definitions
 */

#include <err.h>
#include <assert.h>

#include "benchmark_worker.h"
/*
 * worker_state_wait_for_transition -- wait for transition from and to
 * specified states
 */
static void
worker_state_wait_for_transition(struct benchmark_worker *worker,
		enum benchmark_worker_state state,
		enum benchmark_worker_state new_state)
{
	while (worker->state == state)
		pthread_cond_wait(&worker->cond, &worker->lock);
	assert(worker->state == new_state);
}

/*
 * worker_state_transition -- change worker state from and to specified states
 */
static void
worker_state_transition(struct benchmark_worker *worker,
		enum benchmark_worker_state old_state,
		enum benchmark_worker_state new_state)
{
	assert(worker->state == old_state);
	worker->state = new_state;
	pthread_cond_signal(&worker->cond);
}

/*
 * thread_func -- (internal) callback for pthread
 */
static void *
thread_func(void *arg)
{
	assert(arg != NULL);
	struct benchmark_worker *worker = arg;

	pthread_mutex_lock(&worker->lock);

	worker_state_wait_for_transition(worker,
		WORKER_STATE_IDLE, WORKER_STATE_INIT);

	if (worker->init)
		worker->ret_init = worker->init(worker->bench,
				worker->args, &worker->info);

	worker_state_transition(worker,
		WORKER_STATE_INIT, WORKER_STATE_INITIALIZED);

	worker_state_wait_for_transition(worker,
		WORKER_STATE_INITIALIZED, WORKER_STATE_RUN);

	worker->ret = worker->func(worker->bench, &worker->info);

	worker_state_transition(worker,
		WORKER_STATE_RUN, WORKER_STATE_END);

	worker_state_wait_for_transition(worker,
		WORKER_STATE_END, WORKER_STATE_EXIT);

	if (worker->exit)
		worker->ret_exit = worker->exit(worker->bench,
				worker->args, &worker->info);

	worker_state_transition(worker,
		WORKER_STATE_EXIT, WORKER_STATE_DONE);

	pthread_mutex_unlock(&worker->lock);
	return NULL;
}

/*
 * benchmark_worker_alloc -- allocate benchmark worker
 */
struct benchmark_worker *
benchmark_worker_alloc(void)
{
	struct benchmark_worker *w = calloc(1, sizeof (*w));
	if (!w)
		return NULL;

	if (pthread_mutex_init(&w->lock, NULL))
		goto err_free_worker;

	if (pthread_cond_init(&w->cond, NULL))
		goto err_free_worker;

	if (pthread_create(&w->thread, NULL, thread_func, w))
		goto err_destroy_cond;

	return w;
err_destroy_cond:
	pthread_cond_destroy(&w->cond);
err_free_worker:
	free(w);

	return NULL;
}

/*
 * benchmark_worker_free -- release benchmark worker
 */
void
benchmark_worker_free(struct benchmark_worker *w)
{
	pthread_join(w->thread, NULL);
	pthread_cond_destroy(&w->cond);
	free(w);
}

/*
 * benchmark_worker_init -- call init function for worker
 */
int
benchmark_worker_init(struct benchmark_worker *worker)
{
	pthread_mutex_lock(&worker->lock);

	worker_state_transition(worker,
		WORKER_STATE_IDLE, WORKER_STATE_INIT);

	worker_state_wait_for_transition(worker,
		WORKER_STATE_INIT, WORKER_STATE_INITIALIZED);

	int ret = worker->ret_init;

	pthread_mutex_unlock(&worker->lock);

	return ret;
}

/*
 * benchmark_worker_exit -- call exit function for worker
 */
int
benchmark_worker_exit(struct benchmark_worker *worker)
{
	pthread_mutex_lock(&worker->lock);

	worker_state_transition(worker,
		WORKER_STATE_END, WORKER_STATE_EXIT);

	worker_state_wait_for_transition(worker,
		WORKER_STATE_EXIT, WORKER_STATE_DONE);

	int ret = worker->ret_exit;

	pthread_mutex_unlock(&worker->lock);

	return ret;
}

/*
 * benchmark_worker_run -- run benchmark worker
 */
int
benchmark_worker_run(struct benchmark_worker *worker)
{
	int ret = 0;

	pthread_mutex_lock(&worker->lock);

	worker_state_transition(worker,
		WORKER_STATE_INITIALIZED, WORKER_STATE_RUN);

	pthread_mutex_unlock(&worker->lock);

	return ret;
}

/*
 * benchmark_worker_join -- join benchmark worker
 */
int
benchmark_worker_join(struct benchmark_worker *worker)
{
	pthread_mutex_lock(&worker->lock);

	worker_state_wait_for_transition(worker,
		WORKER_STATE_RUN, WORKER_STATE_END);

	pthread_mutex_unlock(&worker->lock);

	return 0;
}
