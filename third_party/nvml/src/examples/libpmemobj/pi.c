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
 * pi.c -- example usage of user lists
 *
 * Calculates pi number with multiple threads using Leibniz formula.
 */

#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <libpmemobj.h>

/*
 * Layout definition
 */
POBJ_LAYOUT_BEGIN(pi);
POBJ_LAYOUT_ROOT(pi, struct pi);
POBJ_LAYOUT_TOID(pi, struct pi_task);
POBJ_LAYOUT_END(pi);


static PMEMobjpool *pop;

struct pi_task_proto {
	uint64_t start;
	uint64_t stop;
	long double result;
};

struct pi_task {
	struct pi_task_proto proto;
	POBJ_LIST_ENTRY(struct pi_task) todo;
	POBJ_LIST_ENTRY(struct pi_task) done;
};

struct pi {
	POBJ_LIST_HEAD(todo, struct pi_task) todo;
	POBJ_LIST_HEAD(done, struct pi_task) done;
};

/*
 * pi_task_construct -- task constructor
 */
void
pi_task_construct(PMEMobjpool *pop, void *ptr, void *arg)
{
	struct pi_task *t = ptr;
	struct pi_task_proto *p = arg;
	t->proto = *p;
	pmemobj_persist(pop, t, sizeof (*t));
}

/*
 * calc_pi -- worker for pi calculation
 */
void *
calc_pi(void *arg)
{
	TOID(struct pi) pi = POBJ_ROOT(pop, struct pi);
	TOID(struct pi_task) task = *((TOID(struct pi_task) *)arg);

	long double result = 0;
	for (int i = D_RO(task)->proto.start; i < D_RO(task)->proto.stop; ++i) {
		result += (pow(-1, i) / (2 * i + 1));
	}
	D_RW(task)->proto.result = result;
	pmemobj_persist(pop, &D_RW(task)->proto.result, sizeof (double));

	POBJ_LIST_MOVE_ELEMENT_HEAD(pop, &D_RW(pi)->todo, &D_RW(pi)->done,
					task, todo, done);

	return NULL;
}

/*
 * calc_pi_mt -- calculate all the pending to-do tasks
 */
void
calc_pi_mt()
{
	TOID(struct pi) pi = POBJ_ROOT(pop, struct pi);

	int pending = 0;
	TOID(struct pi_task) iter;
	POBJ_LIST_FOREACH(iter, &D_RO(pi)->todo, todo)
		pending++;

	if (pending == 0)
		return;

	int i = 0;
	TOID(struct pi_task) tasks[pending];
	POBJ_LIST_FOREACH(iter, &D_RO(pi)->todo, todo)
		tasks[i++] = iter;

	pthread_t workers[pending];

	for (i = 0; i < pending; ++i)
		if (pthread_create(&workers[i], NULL, calc_pi, &tasks[i]) != 0)
			break;

	for (i = i - 1; i >= 0; --i)
		pthread_join(workers[i], NULL);
}

/*
 * prep_todo_list -- create tasks to be done
 */
int
prep_todo_list(int threads, int ops)
{
	TOID(struct pi) pi = POBJ_ROOT(pop, struct pi);

	if (!POBJ_LIST_EMPTY(&D_RO(pi)->todo))
		return -1;

	int ops_per_thread = ops / threads;
	uint64_t last = 0; /* last calculated denominator */

	TOID(struct pi_task) iter;
	POBJ_LIST_FOREACH(iter, &D_RO(pi)->done, done) {
		if (last < D_RO(iter)->proto.stop)
			last = D_RO(iter)->proto.stop;
	}

	int i;
	for (i = 0; i < threads; ++i) {
		uint64_t start = last + (i * ops_per_thread);
		struct pi_task_proto proto = {
			.start = start,
			.stop = start + ops_per_thread,
			.result = 0
		};

		POBJ_LIST_INSERT_NEW_HEAD(pop, &D_RW(pi)->todo, todo,
			sizeof (struct pi_task), pi_task_construct, &proto);
	}

	return 0;
}

int
main(int argc, char *argv[])
{
	if (argc < 3) {
		printf("usage: %s file-name "
			"[print|done|todo|finish|calc [# of threads] [ops]]\n",
			argv[0]);
		return 1;
	}

	const char *path = argv[1];

	pop = NULL;

	if (access(path, F_OK) != 0) {
		if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(pi),
			PMEMOBJ_MIN_POOL, S_IRWXU)) == NULL) {
			printf("failed to create pool\n");
			return 1;
		}
	} else {
		if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(pi))) == NULL) {
			printf("failed to open pool\n");
			return 1;
		}
	}

	TOID(struct pi) pi = POBJ_ROOT(pop, struct pi);

	char op = argv[2][0];
	switch (op) {
		case 'p': { /* print pi */
			long double pi_val = 0;
			TOID(struct pi_task) iter;
			POBJ_LIST_FOREACH(iter, &D_RO(pi)->done, done) {
				pi_val += D_RO(iter)->proto.result;
			}

			printf("pi: %Lf\n", pi_val * 4);
		} break;
		case 'd': { /* print done list */
			TOID(struct pi_task) iter;
			POBJ_LIST_FOREACH(iter, &D_RO(pi)->done, done) {
				printf("(%lu - %lu) = %Lf\n",
					D_RO(iter)->proto.start,
					D_RO(iter)->proto.stop,
					D_RO(iter)->proto.result);
			}
		} break;
		case 't': { /* print to-do list */
			TOID(struct pi_task) iter;
			POBJ_LIST_FOREACH(iter, &D_RO(pi)->todo, todo) {
				printf("(%lu - %lu) = %Lf\n",
					D_RO(iter)->proto.start,
					D_RO(iter)->proto.stop,
					D_RO(iter)->proto.result);
			}
		} break;
		case 'c': { /* calculate pi */
			int threads = atoi(argv[3]);
			int ops = atoi(argv[4]);
			if (prep_todo_list(threads, ops) == -1)
				printf("pending todo tasks\n");
			else
				calc_pi_mt();
		} break;
		case 'f': { /* finish to-do tasks */
			calc_pi_mt();
		} break;
	}

	pmemobj_close(pop);

	return 0;
}
