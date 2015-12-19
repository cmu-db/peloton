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
 * scenario.h -- scenario module declaration
 */

#include <stdbool.h>

struct kv
{
	TAILQ_ENTRY(kv) next;
	char *key;
	char *value;
};

struct scenario
{
	TAILQ_ENTRY(scenario) next;
	TAILQ_HEAD(scenariohead, kv) head;
	char *name;
	char *benchmark;
};

struct scenarios
{
	TAILQ_HEAD(scenarioshead, scenario) head;
};

#define	FOREACH_SCENARIO(s, ss)	TAILQ_FOREACH((s), &(ss)->head, next)
#define	FOREACH_KV(kv, s) TAILQ_FOREACH((kv), &(s)->head, next)

struct kv *kv_alloc(const char *key, const char *value);
void kv_free(struct kv *kv);

struct scenario *scenario_alloc(const char *name, const char *bench);
void scenario_free(struct scenario *s);

struct scenarios *scenarios_alloc(void);
void scenarios_free(struct scenarios *scenarios);

struct scenario *scenarios_get_scenario(struct scenarios *ss, const char *name);

bool contains_scenarios(int argc, char **argv, struct scenarios *ss);
struct scenario *clone_scenario(struct scenario *src_scenario);
struct kv *find_kv_in_scenario(const char *key,
					const struct scenario *scenario);
