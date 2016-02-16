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
 * pmembench.c -- main source file for benchmark framework
 */

#include <stdio.h>
#include <string.h>
#include <err.h>
#include <assert.h>
#include <getopt.h>
#include <unistd.h>
#include <math.h>
#include <float.h>
#include <sys/queue.h>
#include <linux/limits.h>
#include <dirent.h>

#include "benchmark.h"
#include "benchmark_worker.h"
#include "scenario.h"
#include "clo_vec.h"
#include "clo.h"
#include "config_reader.h"

/*
 * struct pmembench -- main context
 */
struct pmembench
{
	int argc;
	char **argv;
	struct scenario *scenario;
	struct clo_vec *clovec;
	bool override_clos;
};

/*
 * struct benchmark -- benchmark's context
 */
struct benchmark
{
	LIST_ENTRY(benchmark) next;
	struct benchmark_info *info;
	void *priv;
	struct benchmark_clo *clos;
	size_t nclos;
	size_t args_size;
};

/*
 * struct results -- statistics for total measurements
 */
struct results
{
	double min;
	double max;
	double avg;
	double std_dev;
	double med;
};

/*
 * struct latency -- statistics for latency measurements
 */
struct latency
{
	uint64_t max;
	uint64_t min;
	uint64_t avg;
	double std_dev;
};

/*
 * struct bench_list -- list of available benchmarks
 */
struct bench_list
{
	LIST_HEAD(benchmarks_head, benchmark) head;
	bool initialized;
};

/*
 * struct benchmark_opts -- arguments for pmembench
 */
struct benchmark_opts
{
	bool help;
	bool version;
	const char *file_name;
};

static struct version_s
{
	unsigned int major;
	unsigned int minor;
} version = {1, 0};


/* benchmarks list initialization */
static struct bench_list benchmarks = {
	.initialized = false,
};

/* list of arguments for pmembench */
static struct benchmark_clo pmembench_opts[] = {
	{
		.opt_short	= 'h',
		.opt_long	= "help",
		.descr		= "Print help",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct benchmark_opts,
							help),
		.ignore_in_res	= true,
	},
	{
		.opt_short	= 'v',
		.opt_long	= "version",
		.descr		= "Print version",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct benchmark_opts,
						version),
		.ignore_in_res	= true,
	},
};

/* common arguments for benchmarks */
static struct benchmark_clo pmembench_clos[] = {
	{
		.opt_short	= 'h',
		.opt_long	= "help",
		.descr		= "Print help for single benchmark",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct benchmark_args,
						help),
		.ignore_in_res	= true,
	},
	{
		.opt_short	= 't',
		.opt_long	= "threads",
		.type		= CLO_TYPE_UINT,
		.descr		= "Number of working threads",
		.off		= clo_field_offset(struct benchmark_args,
						n_threads),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct benchmark_args,
						n_threads),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= 32,
		},
	},
	{
		.opt_short	= 'n',
		.opt_long	= "ops-per-thread",
		.type		= CLO_TYPE_UINT,
		.descr		= "Number of operations per thread",
		.off		= clo_field_offset(struct benchmark_args,
						n_ops_per_thread),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct benchmark_args,
						n_ops_per_thread),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= ULLONG_MAX,
		},
	},
	{
		.opt_short	= 'd',
		.opt_long	= "data-size",
		.type		= CLO_TYPE_UINT,
		.descr		= "IO data size",
		.off		= clo_field_offset(struct benchmark_args,
						dsize),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct benchmark_args,
						dsize),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 1,
			.max	= ULONG_MAX,
		},
	},
	{
		.opt_short	= 'f',
		.opt_long	= "file",
		.type		= CLO_TYPE_STR,
		.descr		= "File name",
		.off		= clo_field_offset(struct benchmark_args,
						fname),
		.def		= "/mnt/pmem/testfile",
		.ignore_in_res	= true,
	},
	{
		.opt_short	= 'm',
		.opt_long	= "fmode",
		.type		= CLO_TYPE_UINT,
		.descr		= "File mode",
		.off		= clo_field_offset(struct benchmark_args,
					fmode),
		.def		= "0666",
		.ignore_in_res	= true,
		.type_uint	= {
			.size	= clo_field_size(struct benchmark_args, fmode),
			.base	= CLO_INT_BASE_OCT,
			.min	= 0,
			.max	= ULONG_MAX,
		},
	},
	{
		.opt_short	= 's',
		.opt_long	= "seed",
		.type		= CLO_TYPE_UINT,
		.descr		= "PRNG seed",
		.off		= clo_field_offset(struct benchmark_args, seed),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct benchmark_args, seed),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= ~0,
		},
	},
	{
		.opt_short	= 'r',
		.opt_long	= "repeats",
		.type		= CLO_TYPE_UINT,
		.descr		= "Number of repeats of scenario",
		.off		= clo_field_offset(struct benchmark_args,
						repeats),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct benchmark_args,
						repeats),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 1,
			.max	= ULONG_MAX,
		},
	},
};

/*
 * pmembench_get_priv -- return private structure of benchmark
 */
void *
pmembench_get_priv(struct benchmark *bench)
{
	return bench->priv;
}

/*
 * pmembench_set_priv -- set private structure of benchmark
 */
void
pmembench_set_priv(struct benchmark *bench, void *priv)
{
	bench->priv = priv;
}

/*
 * pmembench_register -- register benchmark
 */
int
pmembench_register(struct benchmark_info *bench_info)
{
	struct benchmark *bench = calloc(1, sizeof (*bench));
	assert(bench != NULL);

	bench->info = bench_info;

	if (!benchmarks.initialized) {
		LIST_INIT(&benchmarks.head);
		benchmarks.initialized = true;
	}

	LIST_INSERT_HEAD(&benchmarks.head, bench, next);

	return 0;
}

/*
 * pmembench_get_info -- return structure with information about benchmark
 */
struct benchmark_info *
pmembench_get_info(struct benchmark *bench)
{
	return bench->info;
}

/*
 * pmembench_release_clos -- release CLO structure
 */
static void
pmembench_release_clos(struct benchmark *bench)
{
	free(bench->clos);
}

/*
 * pmembench_merge_clos -- merge benchmark's CLOs with common CLOs
 */
static void
pmembench_merge_clos(struct benchmark *bench)
{
	size_t size = sizeof (struct benchmark_args);
	size_t pb_nclos = ARRAY_SIZE(pmembench_clos);
	size_t nclos = pb_nclos;
	size_t i;

	if (bench->info->clos) {
		size += bench->info->opts_size;
		nclos += bench->info->nclos;
	}

	struct benchmark_clo *clos = calloc(nclos,
			sizeof (struct benchmark_clo));
	assert(clos != NULL);

	memcpy(clos, pmembench_clos, pb_nclos * sizeof (struct benchmark_clo));

	if (bench->info->clos) {
		memcpy(&clos[pb_nclos], bench->info->clos, bench->info->nclos *
				sizeof (struct benchmark_clo));

		for (i = 0; i < bench->info->nclos; i++) {
			clos[pb_nclos + i].off +=
				sizeof (struct benchmark_args);
		}
	}

	bench->clos = clos;
	bench->nclos = nclos;
	bench->args_size = size;
}

/*
 * pmembench_run_worker -- run worker with benchmark operation
 */
static int
pmembench_run_worker(struct benchmark *bench, struct worker_info *winfo)
{
	uint64_t i;
	uint64_t ops = winfo->nops;
	benchmark_time_t start, stop;
	for (i = 0; i < ops; i++) {
		if (bench->info->op_init) {
			if (bench->info->op_init(bench, &winfo->opinfo[i]))
				return -1;
		}
		benchmark_time_get(&start);
		if (bench->info->operation(bench, &winfo->opinfo[i]))
			return -1;
		benchmark_time_get(&stop);
		benchmark_time_diff(&winfo->opinfo[i].t_diff, &start, &stop);
		if (bench->info->op_exit) {
			if (bench->info->op_exit(bench, &winfo->opinfo[i]))
				return -1;
		}
	}

	return 0;
}

/*
 * pmembench_print_header -- print header of benchmark's results
 */
static void
pmembench_print_header(struct pmembench *pb, struct benchmark *bench,
		struct clo_vec *clovec)
{
	if (pb->scenario) {
		printf("%s: %s [%ld]\n", pb->scenario->name,
			bench->info->name, clovec->nargs);
	} else {
		printf("%s [%ld]\n", bench->info->name, clovec->nargs);
	}
	printf("total-avg;"
		"ops-per-second;"
		"total-max;"
		"total-min;"
		"total-median;"
		"total-std-dev;"
		"latency-avg;"
		"latency-min;"
		"latency-max;"
		"latency-std-dev");
	size_t i;
	for (i = 0; i < bench->nclos; i++) {
		if (!bench->clos[i].ignore_in_res) {
			printf(";%s", bench->clos[i].opt_long);
		}
	}
	printf("\n");
}

/*
 * pmembench_print_results -- print benchmark's results
 */
static void
pmembench_print_results(struct benchmark *bench, struct benchmark_args *args,
				size_t n_threads, size_t n_ops,
				struct results *stats, struct latency *latency)
{
	double opsps = n_threads * n_ops / stats->avg;
	printf("%f;%f;%f;%f;%f;%f;%ld;%ld;%ld;%f", stats->avg,
			opsps,
			stats->max,
			stats->min,
			stats->med,
			stats->std_dev,
			latency->avg,
			latency->min,
			latency->max,
			latency->std_dev);

	size_t i;
	for (i = 0; i < bench->nclos; i++) {
		if (!bench->clos[i].ignore_in_res)
			printf(";%s",
				benchmark_clo_str(&bench->clos[i], args,
					bench->args_size));
	}
	printf("\n");
}

/*
 * pmembench_parse_clos -- parse command line arguments for benchmark
 */
static int
pmembench_parse_clo(struct pmembench *pb, struct benchmark *bench,
		struct clo_vec *clovec)
{
	if (!pb->scenario) {
		return benchmark_clo_parse(pb->argc, pb->argv, bench->clos,
						bench->nclos, clovec);
	}

	if (pb->override_clos) {
		/*
		 * Use only ARRAY_SIZE(pmembench_clos) clos - these are the
		 * general clos and are placed at the beginning of the
		 * clos array.
		 */
		int ret = benchmark_override_clos_in_scenario(pb->scenario,
					pb->argc, pb->argv, bench->clos,
					ARRAY_SIZE(pmembench_clos));
		/* reset for the next benchmark in the config file */
		optind = 1;

		if (ret)
			return ret;
	}

	return benchmark_clo_parse_scenario(pb->scenario, bench->clos,
						bench->nclos, clovec);
}

/*
 * pmembench_init_workers -- init benchmark's workers
 */
static int
pmembench_init_workers(struct benchmark_worker **workers, size_t nworkers,
	size_t n_ops, struct benchmark *bench, struct benchmark_args *args)
{
	size_t i;
	for (i = 0; i < nworkers; i++) {
		workers[i] = benchmark_worker_alloc();
		workers[i]->info.index = i;
		workers[i]->info.nops = n_ops;
		workers[i]->info.opinfo = calloc(n_ops,
				sizeof (struct operation_info));
		size_t j;
		for (j = 0; j < n_ops; j++) {
			workers[i]->info.opinfo[j].worker = &workers[i]->info;
			workers[i]->info.opinfo[j].args = args;
			workers[i]->info.opinfo[j].index = j;
		}
		workers[i]->bench = bench;
		workers[i]->args = args;
		workers[i]->func = pmembench_run_worker;
		workers[i]->init = bench->info->init_worker;
		workers[i]->exit = bench->info->free_worker;
		benchmark_worker_init(workers[i]);
	}
	return 0;
}

/*
 * pmembench_dummy_op -- dummy operation
 */
static int
pmembench_dummy_op()
{
	return 0;
}

/*
 * compare_doubles -- comparing function used for sorting
 */
static int
compare_doubles(const void *a1, const void *b1)
{
	const double *a = (const double *)a1;
	const double *b = (const double *)b1;
	return (*a > *b) - (*a < *b);
}

/*
 * pmembench_get_results -- return results of one repeat
 */
static void
pmembench_get_results(struct benchmark_worker **workers, size_t nworkers,
		struct latency *stats, double *workers_times)
{
	memset(stats, 0, sizeof (*stats));
	stats->min = ~0;
	uint64_t i;
	uint64_t j;
	uint64_t d;
	uint64_t nsecs = 0;
	uint64_t nsecs_dummy = 0;
	double secs_dummy = 0;
	uint64_t count = 0;
	benchmark_time_t start, stop, dummy;
	benchmark_time_get(&start);
	pmembench_dummy_op();
	benchmark_time_get(&stop);
	benchmark_time_diff(&dummy, &start, &stop);
	nsecs_dummy = benchmark_time_get_nsecs(&dummy);
	secs_dummy = benchmark_time_get_secs(&dummy);
	for (i = 0; i < nworkers; i++) {
		for (j = 0; j < workers[i]->info.nops; j++) {
			nsecs = benchmark_time_get_nsecs(
				&workers[i]->info.opinfo[j].t_diff);
			if (nsecs > nsecs_dummy)
				nsecs -= nsecs_dummy;

			workers_times[i] += benchmark_time_get_secs(
				&workers[i]->info.opinfo[j].t_diff);
			if (workers_times[i] > secs_dummy)
				workers_times[i] -= secs_dummy;

			if (nsecs > stats->max)
				stats->max = nsecs;
			if (nsecs < stats->min)
				stats->min = nsecs;
			stats->avg += nsecs;
			count++;
		}
	}
	assert(count != 0);
	if (count > 0)
		stats->avg /= count;
	for (i = 0; i < nworkers; i++) {
		for (j = 0; j < workers[i]->info.nops; j++) {
			nsecs = benchmark_time_get_nsecs(
				&workers[i]->info.opinfo[j].t_diff)
				- nsecs_dummy;
			d = nsecs > stats->avg ? nsecs - stats->avg :
							stats->avg - nsecs;
			stats->std_dev += d * d;
		}
	}
	stats->std_dev = sqrt(stats->std_dev / count);

}

/*
 * pmembench_get_total_results -- return results of all repeats of scenario
 */
static void
pmembench_get_total_results(struct latency *stats, double *workers_times,
				struct results *total, struct latency *latency,
				size_t repeats, size_t nworkers)
{
	memset(total, 0, sizeof (*total));
	memset(latency, 0, sizeof (*latency));
	total->min = DBL_MAX;
	latency->min = ~0;
	size_t nresults = repeats * nworkers;
	size_t i;
	size_t j;
	size_t d;
	double df;
	for (i = 0; i < repeats; i++) {
		/* latency */
		if (stats[i].max > latency->max)
			latency->max = stats[i].max;
		if (stats[i].min < latency->min)
			latency->min = stats[i].min;
		latency->avg += stats[i].avg;

		/* total time */
		for (j = 0; j < nworkers; j++) {
			int idx = i * nworkers + j;
			total->avg += workers_times[idx];
		}
	}
	latency->avg /= repeats;
	total->avg /= nresults;
	qsort(workers_times, nresults, sizeof (double), compare_doubles);
	total->min = workers_times[0];
	total->max = workers_times[nresults - 1];
	total->med = nresults % 2 ? (workers_times[nresults / 2] +
				workers_times[nresults / 2 + 1]) / 2:
				workers_times[nresults / 2];

	for (i = 0; i < repeats; i++) {
		d = stats[i].std_dev > latency->avg
				? stats[i].std_dev - latency->avg
				: latency->avg - stats[i].std_dev;
		latency->std_dev += d * d;

		for (j = 0; j < nworkers; j++) {
			int idx = i * nworkers + j;
			df = workers_times[idx] > total->avg ?
					workers_times[idx] - total->avg :
					total->avg - workers_times[idx];
			total->std_dev += df * df;
		}
	}
	latency->std_dev = sqrt(latency->std_dev / repeats);
	total->std_dev = sqrt(total->std_dev / nresults);
}

/*
 * pmembench_print_args -- print arguments for one benchmark
 */
static void
pmembench_print_args(struct benchmark_clo *clos, size_t nclos)
{
	struct benchmark_clo clo;
	for (size_t i = 0; i < nclos; i++) {
		clo = clos[i];
		if (clo.opt_short != 0)
			printf("\t-%c,", clo.opt_short);
		else
			printf("\t");
		printf("\t--%-15s\t\t%s", clo.opt_long, clo.descr);
		if (clo.type != CLO_TYPE_FLAG)
			printf(" [default: %s]", clo.def);

		if (clo.type == CLO_TYPE_INT) {
			if (clo.type_int.min != LONG_MIN)
				printf(" [min: %jd]", clo.type_int.min);
			if (clo.type_int.max != LONG_MAX)
				printf(" [max: %jd]", clo.type_int.max);
		} else if (clo.type == CLO_TYPE_UINT) {
			if (clo.type_uint.min != 0)
				printf(" [min: %ju]", clo.type_uint.min);
			if (clo.type_uint.max != ULONG_MAX)
				printf(" [max: %ju]", clo.type_uint.max);
		}
		printf("\n");
	}
}

/*
 * pmembench_print_help_single -- prints help for single benchmark
 */
static void
pmembench_print_help_single(struct benchmark *bench)
{
	struct benchmark_info *info = bench->info;
	printf("%s\n%s\n", info->name, info->brief);
	printf("\nArguments:\n");
	size_t nclos = sizeof (pmembench_clos) / sizeof (struct benchmark_clo);
	pmembench_print_args(pmembench_clos, nclos);
	if (info->clos == NULL)
		return;
	pmembench_print_args(info->clos, info->nclos);
}

/*
 * pmembench_print_usage -- print usage of framework
 */
static void
pmembench_print_usage()
{
	printf("Usage: $ pmembench [-h|--help] [-v|--version]"
			"\t[<benchmark>[<args>]]\n");
	printf("\t\t\t\t\t\t[<config>[<scenario>]]\n");
	printf("\t\t\t\t\t\t[<config>[<scenario>[<common_args>]]]\n");
}

/*
 *  pmembench_print_version -- print version of framework
 */
static void
pmembench_print_version()
{
	printf("Benchmark framework - version %d.%d\n", version.major,
							version.minor);
}

/*
 * pmembench_print_examples() -- print examples of using framework
 */
static void
pmembench_print_examples()
{
	printf("\nExamples:\n");
	printf("$ pmembench <benchmark_name> <args>\n");
	printf(" # runs benchmark of name <benchmark> with arguments <args>\n");
	printf("or\n");
	printf("$ pmembench <config_file>\n");
	printf(" # runs all scenarios from config file\n");
	printf("or\n");
	printf("$ pmembench [<benchmark_name>] [-h|--help [-v|--version]\n");
	printf(" # prints help\n");
	printf("or\n");
	printf("$ pmembench <config_file> <name_of_scenario>\n");
	printf(" # runs the specified scenario from config file\n");
	printf("$ pmembench <config_file> <name_of_scenario_1> "
		"<name_of_scenario_2> <common_args>\n");
	printf(" # runs the specified scenarios from config file and overwrites"
		" the given common_args from the config file\n");
}

/*
 * pmembench_print_help -- print help for framework
 */
static void
pmembench_print_help()
{
	pmembench_print_version();
	pmembench_print_usage();
	printf("\nCommon arguments:\n");
	size_t nclos = sizeof (pmembench_opts) / sizeof (struct benchmark_clo);
	pmembench_print_args(pmembench_opts, nclos);

	printf("\nAvaliable benchmarks:\n");
	struct benchmark *bench = NULL;
	LIST_FOREACH(bench, &benchmarks.head, next)
		printf("\t%-20s\t\t%s\n", bench->info->name,
						bench->info->brief);
	printf("\n$ pmembench <benchmark> --help to print detailed information"
				" about benchmark arguments\n");
	pmembench_print_examples();
}

/*
 * pmembench_get_bench -- searching benchmarks by name
 */
static struct benchmark *
pmembench_get_bench(const char *name)
{
	struct benchmark *bench;
	LIST_FOREACH(bench, &benchmarks.head, next) {
		if (strcmp(name, bench->info->name) == 0)
			return bench;
	}

	return NULL;
}

/*
 * pmembench_parse_opts -- parse arguments for framework
 */
static int
pmembench_parse_opts(struct pmembench *pb)
{
	int ret = 0;
	int argc = ++pb->argc;
	char **argv = --pb->argv;
	struct benchmark_opts *opts = NULL;
	struct clo_vec *clovec;
	size_t size, n_clos;
	size = sizeof (struct benchmark_opts);
	n_clos = ARRAY_SIZE(pmembench_opts);
	clovec = clo_vec_alloc(size);
	assert(clovec != NULL);

	if (benchmark_clo_parse(argc, argv, pmembench_opts,
			n_clos, clovec)) {
		ret = -1;
		goto out;
	}

	opts = clo_vec_get_args(clovec, 0);

	if (opts->help)
		pmembench_print_help();
	if (opts->version)
		pmembench_print_version();

out:
	clo_vec_free(clovec);
	free(opts);
	return ret;
}

/*
 * pmembench_remove_file -- remove file or directory if exists
 */
static int
pmembench_remove_file(const char *dname)
{
	int ret = 0;
	DIR *dir;
	struct dirent *d;
	char *tmp;
	dir = opendir(dname);
	if (dir == NULL) {
		if (access(dname, F_OK) == 0)
			remove(dname);
		return ret;
	}

	while ((d = readdir(dir)) != NULL) {
		if (strcmp(d->d_name, ".") == 0 || strcmp(d->d_name, "..") == 0)
			continue;
		tmp = malloc(strlen(dname) + strlen(d->d_name) + 2);
		sprintf(tmp, "%s/%s", dname, d->d_name);
		ret = (d->d_type == DT_DIR) ? pmembench_remove_file(tmp)
								: remove(tmp);
		free(tmp);
		if (ret != 0)
			return ret;
	}
	ret = rmdir(dname);
	return ret;
}

/*
 * pmembench_run -- runs one benchmark. Parses arguments and performs
 * specific functions.
 */
static int
pmembench_run(struct pmembench *pb, struct benchmark *bench)
{
	char old_wd[PATH_MAX];
	int ret = 0;

	assert(bench->info != NULL);
	pmembench_merge_clos(bench);

	/*
	 * Check if PMEMBENCH_DIR env var is set and change
	 * the working directory accordingly.
	 */
	char *wd = getenv("PMEMBENCH_DIR");
	if (wd != NULL) {
		/* get current dir name */
		if (getcwd(old_wd, PATH_MAX) == NULL) {
			perror("getcwd");
			ret = -1;
			goto out_release_clos;
		}
		if (chdir(wd)) {
			perror("chdir(wd)");
			ret = -1;
			goto out_release_clos;
		}
	}

	if (bench->info->pre_init) {
		if (bench->info->pre_init(bench)) {
			warn("%s: pre-init failed", bench->info->name);
			ret = -1;
			goto out_old_wd;
		}
	}

	struct benchmark_args *args = NULL;

	struct clo_vec *clovec = clo_vec_alloc(bench->args_size);
	assert(clovec != NULL);

	if (pmembench_parse_clo(pb, bench, clovec)) {
		warn("%s: parsing command line arguments failed",
				bench->info->name);
		ret = -1;
		goto out_release_args;
	}

	args = clo_vec_get_args(clovec, 0);
	if (args->help) {
		pmembench_print_help_single(bench);
		goto out;
	}

	pmembench_print_header(pb, bench, clovec);

	size_t args_i;
	for (args_i = 0; args_i < clovec->nargs; args_i++) {
		args = clo_vec_get_args(clovec, args_i);
		args->opts = (void *)((uintptr_t)args +
				sizeof (struct benchmark_args));

		size_t n_threads = !bench->info->multithread ? 1 :
						args->n_threads;
		size_t n_ops = !bench->info->multiops ? 1 :
						args->n_ops_per_thread;

		struct latency *stats = calloc(args->repeats,
						sizeof (struct latency));
		assert(stats != NULL);
		double *workers_times = calloc(n_threads * args->repeats,
							sizeof (double));
		assert(workers_times != NULL);

		for (unsigned int i = 0; i < args->repeats; i++) {
			if (bench->info->rm_file) {
				ret = pmembench_remove_file(args->fname);
				if (ret != 0) {
					perror("removing file failed");
					goto out;
				}
			}

			if (bench->info->init) {
				if (bench->info->init(bench, args)) {
					warn("%s: initialization failed",
						bench->info->name);
					ret = -1;
					goto out;
				}
			}

			assert(bench->info->operation != NULL);

			struct benchmark_worker **workers;
			workers = malloc(args->n_threads *
					sizeof (struct benchmark_worker *));
			assert(workers != NULL);

			if ((ret = pmembench_init_workers(workers, n_threads,
						n_ops, bench, args)) != 0) {
				if (bench->info->exit)
					bench->info->exit(bench, args);
				goto out;
			}

			unsigned int j;
			for (j = 0; j < args->n_threads; j++) {
				benchmark_worker_run(workers[j]);
			}

			for (j = 0; j < args->n_threads; j++) {
				benchmark_worker_join(workers[j]);
				if (workers[j]->ret != 0) {
					ret = workers[j]->ret;
					fprintf(stderr,
					"thread number %d failed \n", j);
				}
			}
			if (ret == 0)
				pmembench_get_results(workers, n_threads,
						&stats[i],
						&workers_times[i * n_threads]);

			for (j = 0; j < args->n_threads; j++) {
				benchmark_worker_exit(workers[j]);

				free(workers[j]->info.opinfo);
				benchmark_worker_free(workers[j]);
			}

			free(workers);

			if (bench->info->exit)
			bench->info->exit(bench, args);
		}
		struct results total;
		struct latency latency;
		pmembench_get_total_results(stats, workers_times, &total,
					&latency, args->repeats, n_threads);
		pmembench_print_results(bench, args, n_threads, n_ops,
							&total, &latency);
		free(stats);
		free(workers_times);
	}
out:
out_release_args:
	clo_vec_free(clovec);

out_old_wd:
	/* restore the original working directory */
	if (wd != NULL) { /* Only if PMEMBENCH_DIR env var was defined */
		if (chdir(old_wd)) {
			perror("chdir(old_wd)");
			ret = -1;
		}
	}

out_release_clos:
	pmembench_release_clos(bench);
	return ret;
}

/*
 * pmembench_free_benchmarks -- release all benchmarks
 */
static void
__attribute__((destructor))
pmembench_free_benchmarks(void)
{
	while (!LIST_EMPTY(&benchmarks.head)) {
		struct benchmark *bench = LIST_FIRST(&benchmarks.head);
		LIST_REMOVE(bench, next);
		free(bench);
	}
}

/*
 * pmembench_run_scenario -- run single benchmark's scenario
 */
static int
pmembench_run_scenario(struct pmembench *pb, struct scenario *scenario)
{
	struct benchmark *bench = pmembench_get_bench(scenario->benchmark);
	if (NULL == bench) {
		fprintf(stderr, "unknown benchmark: %s\n", scenario->benchmark);
		return -1;
	}
	pb->scenario = scenario;
	return pmembench_run(pb, bench);
}

/*
 * pmembench_run_scenarios -- run all scenarios
 */
static int
pmembench_run_scenarios(struct pmembench *pb, struct scenarios *ss)
{
	struct scenario *scenario;
	FOREACH_SCENARIO(scenario, ss) {
		if (pmembench_run_scenario(pb, scenario) != 0)
			return -1;
	}
	return 0;
}

/*
 * pmembench_run_config -- run one or all scenarios from config file
 */
static int
pmembench_run_config(struct pmembench *pb, const char *config)
{
	struct config_reader *cr = config_reader_alloc();
	assert(cr != NULL);

	int ret = 0;

	if ((ret = config_reader_read(cr, config)))
		goto out;

	struct scenarios *ss = NULL;
	if ((ret = config_reader_get_scenarios(cr, &ss)))
		goto out;

	assert(ss != NULL);

	if (pb->argc == 1) {
		if ((ret = pmembench_run_scenarios(pb, ss)) != 0)
			goto out_scenarios;
	} else {
		/* Skip the config file name in cmd line params */
		int tmp_argc = pb->argc - 1;
		char **tmp_argv = pb->argv + 1;

		if (!contains_scenarios(tmp_argc, tmp_argv, ss)) {
			/* no scenarios in cmd line arguments - parse params */
			pb->override_clos = true;
			if ((ret = pmembench_run_scenarios(pb, ss)) != 0)
				goto out_scenarios;
		} else { /* scenarios in cmd line */
			struct scenarios *cmd_ss = scenarios_alloc();
			assert(cmd_ss != NULL);

			int parsed_scenarios =
				clo_get_scenarios(tmp_argc, tmp_argv,
							ss, cmd_ss);
			if (parsed_scenarios < 0)
				goto out_cmd;

			/*
			 * If there are any cmd line args left, treat
			 * them as config file params override.
			 */
			if (tmp_argc - parsed_scenarios)
				pb->override_clos = true;

			/*
			 * Skip the scenarios in the cmd line,
			 * pmembench_run_scenarios does not expect them and will
			 * fail otherwise.
			 */
			pb->argc -= parsed_scenarios;
			pb->argv += parsed_scenarios;
			if ((ret = pmembench_run_scenarios(pb, cmd_ss)) != 0) {
				goto out_cmd;
			}
out_cmd:
			scenarios_free(cmd_ss);
		}
	}

out_scenarios:
	scenarios_free(ss);
out:
	config_reader_free(cr);
	return ret;
}

int
main(int argc, char *argv[])
{
	int ret = 0;
	struct pmembench *pb = calloc(1, sizeof (*pb));
	assert(pb != NULL);

	/*
	 * Parse common command line arguments and
	 * benchmark's specific ones.
	 */
	if (argc < 2) {
		pmembench_print_usage();
		exit(EXIT_FAILURE);
		return -1;
	}

	pb->argc = --argc;
	pb->argv = ++argv;

	char *bench_name = pb->argv[0];
	if (NULL == bench_name) {
		ret = -1;
		goto out;
	}
	int fexists = access(bench_name, R_OK) == 0;
	struct benchmark *bench = pmembench_get_bench(bench_name);
	if (NULL != bench)
		ret = pmembench_run(pb, bench);
	else if (fexists)
		ret = pmembench_run_config(pb, bench_name);
	else if ((ret = pmembench_parse_opts(pb)) != 0) {
		pmembench_print_usage();
		goto out;
	}

out:
	free(pb);
	return ret;
}
