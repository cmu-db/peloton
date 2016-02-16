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
 * scenario.c -- scenario module definitions
 */
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/queue.h>

#include "scenario.h"

/*
 * kv_alloc -- allocate key/value structure
 */
struct kv *
kv_alloc(const char *key, const char *value)
{
	struct kv *kv = malloc(sizeof (*kv));
	assert(kv != NULL);

	kv->key = strdup(key);
	assert(kv->key != NULL);

	kv->value = strdup(value);
	assert(kv->value != NULL);

	return kv;
}

/*
 * kv_free -- free the key/value structure
 */
void
kv_free(struct kv *kv)
{
	assert(kv != NULL);
	free(kv->key);
	free(kv->value);
	free(kv);
}

/*
 * scenario_alloc -- allocate scenario structure
 */
struct scenario *
scenario_alloc(const char *name, const char *bench)
{
	struct scenario *s = malloc(sizeof (*s));
	assert(s != NULL);

	TAILQ_INIT(&s->head);
	s->name = strdup(name);
	assert(s->name != NULL);

	s->benchmark = strdup(bench);
	assert(s->benchmark != NULL);

	return s;
}

/*
 * scenario_free -- free the scenario structure and all its content
 */
void
scenario_free(struct scenario *s)
{
	assert(s != NULL);

	while (!TAILQ_EMPTY(&s->head)) {
		struct kv *kv = TAILQ_FIRST(&s->head);
		TAILQ_REMOVE(&s->head, kv, next);
		kv_free(kv);
	}

	free(s->name);
	free(s->benchmark);
	free(s);
}

/*
 * scenarios_alloc -- allocate scenarios structure
 */
struct scenarios *
scenarios_alloc(void)
{
	struct scenarios *scenarios = malloc(sizeof (*scenarios));
	assert(NULL != scenarios);

	TAILQ_INIT(&scenarios->head);

	return scenarios;
}

/*
 * scenarios_free -- free scenarios structure and all its content
 */
void
scenarios_free(struct scenarios *scenarios)
{
	assert(scenarios != NULL);
	while (!TAILQ_EMPTY(&scenarios->head)) {
		struct scenario *sce = TAILQ_FIRST(&scenarios->head);
		TAILQ_REMOVE(&scenarios->head, sce, next);
		scenario_free(sce);
	}

	free(scenarios);
}

/*
 * scenarios_get_scenario -- get scenario of given name
 */
struct scenario *
scenarios_get_scenario(struct scenarios *ss, const char *name)
{
	struct scenario *scenario;
	FOREACH_SCENARIO(scenario, ss) {
		if (strcmp(scenario->name, name) == 0)
			return scenario;
	}
	return NULL;
}

/*
 * contains_scenarios -- check if cmd line args contain any scenarios from ss
 */
bool
contains_scenarios(int argc, char **argv, struct scenarios *ss)
{
	assert(argv != NULL);
	assert(argc > 0);
	assert(ss != NULL);

	for (int i = 0; i < argc; i++) {
		if (scenarios_get_scenario(ss, argv[i]))
			return true;
	}
	return false;
}

/*
 * clone_scenario -- alloc a new scenario and copy all data from src scenario
 */
struct scenario *
clone_scenario(struct scenario *src_scenario)
{
	assert(src_scenario != NULL);

	struct scenario *new_scenario = scenario_alloc(src_scenario->name,
						src_scenario->benchmark);
	assert(new_scenario != NULL);

	struct kv *src_kv;

	FOREACH_KV(src_kv, src_scenario) {
		struct kv *new_kv = kv_alloc(src_kv->key, src_kv->value);
		assert(new_kv != NULL);

		TAILQ_INSERT_TAIL(&new_scenario->head, new_kv, next);
	}

	return new_scenario;
}
/*
 * find_kv_in_scenario - find a kv in the given scenario with the given key
 * value. Function returns the pointer to the kv structure containing the key or
 * NULL if it is not found
 */
struct kv *
find_kv_in_scenario(const char *key, const struct scenario *scenario)
{
	struct kv *kv;

	FOREACH_KV(kv, scenario) {
		if (strcmp(kv->key, key) == 0)
			return kv;
	}
	return NULL;
}
