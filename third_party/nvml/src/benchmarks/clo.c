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
 * clo.c -- command line options module definitions
 */
#include <getopt.h>
#include <string.h>
#include <err.h>
#include <errno.h>
#include <assert.h>
#include <sys/queue.h>

#include "benchmark.h"
#include "scenario.h"
#include "clo_vec.h"
#include "clo.h"

#define	min(a, b)	((a) < (b) ? (a) : (b))
#define	max(a, b)	((a) > (b) ? (a) : (b))

typedef int (*clo_parse_fn)(struct benchmark_clo *clo, const char *arg,
		struct clo_vec *clovec);

typedef int (*clo_parse_single_fn)(struct benchmark_clo *clo, const char *arg,
		void *ptr);

typedef int (*clo_eval_range_fn)(struct benchmark_clo *clo, void *first,
	void *step, void *last, char type, struct clo_vec_vlist *vlist);

typedef const char *(*clo_str_fn)(struct benchmark_clo *clo,
		void *addr, size_t size);

#define	STR_BUFF_SIZE	1024
static char str_buff[STR_BUFF_SIZE];

/*
 * clo_parse_flag -- (internal) parse flag
 */
static int
clo_parse_flag(struct benchmark_clo *clo, const char *arg,
		struct clo_vec *clovec)
{
	bool flag = true;
	if (arg != NULL) {
		if (strcmp(arg, "true") == 0)
			flag = true;
		else if (strcmp(arg, "false") == 0)
			flag = false;
		else
			return -1;
	}

	return clo_vec_memcpy(clovec, clo->off, sizeof (flag), &flag);
}

/*
 * clo_parse_str -- (internal) parse string value
 */
static int
clo_parse_str(struct benchmark_clo *clo, const char *arg,
		struct clo_vec *clovec)
{
	struct clo_vec_vlist *vlist = clo_vec_vlist_alloc();
	assert(vlist != NULL);

	char *str = strdup(arg);
	assert(str != NULL);
	clo_vec_add_alloc(clovec, str);

	char *next = strtok(str, ",");
	while (next) {
		clo_vec_vlist_add(vlist, &next, sizeof (next));
		next = strtok(NULL, ",");
	}

	int ret = clo_vec_memcpy_list(clovec, clo->off, sizeof (str), vlist);

	clo_vec_vlist_free(vlist);

	return ret;
}

/*
 * is_oct -- check if string may be octal number
 */
static int
is_oct(const char *arg, size_t len)
{
	return (arg[0] == '0' || (len > 1 && arg[0] == '-' && arg[1] == '0'));
}

/*
 * is_hex -- check if string may be hexadecimal number
 */
static int
is_hex(const char *arg, size_t len)
{
	if (arg[0] == '-') {
		arg++;
		len--;
	}

	return (len > 2 && arg[0] == '0' && (arg[1] == 'x' || arg[1] == 'X'));
}

/*
 * parse_number_base -- parse string as integer of given sign and base
 */
static int
parse_number_base(const char *arg, void *value, int s, int base)
{
	char *end;
	errno = 0;
	if (s) {
		int64_t *v = value;
		*v = strtoll(arg, &end, base);
	} else {
		uint64_t *v = value;
		*v = strtoull(arg, &end, base);
	}

	if (errno || *end != '\0')
		return -1;
	return 0;
}

/*
 * parse_number -- parse string as integer of given sign and allowed bases
 */
static int
parse_number(const char *arg, size_t len, void *value, int s,
		enum clo_int_base base)
{
	if ((base & CLO_INT_BASE_HEX) && is_hex(arg, len)) {
		if (!parse_number_base(arg, value, s, 16))
			return 0;
	}

	if ((base & CLO_INT_BASE_OCT) && is_oct(arg, len)) {
		if (!parse_number_base(arg, value, s, 8))
			return 0;
	}

	if (base & CLO_INT_BASE_DEC) {
		if (!parse_number_base(arg, value, s, 10))
			return 0;
	}

	return -1;
}

/*
 * clo_parse_single_int -- (internal) parse single int value
 */
static int
clo_parse_single_int(struct benchmark_clo *clo, const char *arg, void *ptr)
{
	int64_t value = 0;
	size_t len = strlen(arg);

	if (parse_number(arg, len, &value, 1, clo->type_int.base)) {
		errno = EINVAL;
		return -1;
	}

	int64_t tmax = ((int64_t)1 << (8 * clo->type_int.size - 1)) - 1;
	int64_t tmin = -((int64_t)1 << (8 * clo->type_int.size - 1));

	tmax = min(tmax, clo->type_int.max);
	tmin = max(tmin, clo->type_int.min);

	if (value > tmax || value < tmin) {
		errno = ERANGE;
		return -1;
	}

	memcpy(ptr, &value, clo->type_int.size);
	return 0;
}

/*
 * clo_parse_single_uint -- (internal) parse single uint value
 */
static int
clo_parse_single_uint(struct benchmark_clo *clo, const char *arg, void *ptr)
{
	if (arg[0] == '-') {
		errno = EINVAL;
		return -1;
	}

	uint64_t value = 0;
	size_t len = strlen(arg);

	if (parse_number(arg, len, &value, 0, clo->type_uint.base)) {
		errno = EINVAL;
		return -1;
	}

	uint64_t tmax = ~0 >> (64 - 8 * clo->type_uint.size);
	uint64_t tmin = 0;

	tmax = min(tmax, clo->type_uint.max);
	tmin = max(tmin, clo->type_uint.min);

	if (value > tmax || value < tmin) {
		errno = ERANGE;
		return -1;
	}

	memcpy(ptr, &value, clo->type_uint.size);
	return 0;
}

/*
 * clo_eval_range_uint -- (internal) evaluate range for uint values
 */
static int
clo_eval_range_uint(struct benchmark_clo *clo, void *first, void *step,
		void *last, char type, struct clo_vec_vlist *vlist)
{
	uint64_t curr = *(uint64_t *)first;
	uint64_t l = *(uint64_t *)last;
	int64_t s = *(int64_t *)step;

	while (1) {
		clo_vec_vlist_add(vlist, &curr, clo->type_uint.size);

		switch (type) {
		case '+':
			curr += s;
			if (curr > l)
				return 0;
			break;
		case '-':
			if (curr < (uint64_t)s)
				return 0;
			curr -= s;
			if (curr < l)
				return 0;
			break;
		case '*':
			curr *= s;
			if (curr > l)
				return 0;
			break;
		case '/':
			curr /= s;
			if (curr < l)
				return 0;
			break;
		default:
			return -1;
		}
	}

	return -1;
}

/*
 * clo_eval_range_int -- (internal) evaluate range for int values
 */
static int
clo_eval_range_int(struct benchmark_clo *clo, void *first, void *step,
		void *last, char type, struct clo_vec_vlist *vlist)
{
	int64_t curr = *(int64_t *)first;
	int64_t l = *(int64_t *)last;
	uint64_t s = *(uint64_t *)step;

	while (1) {
		clo_vec_vlist_add(vlist, &curr, clo->type_int.size);

		switch (type) {
		case '+':
			curr += s;
			if (curr > l)
				return 0;
			break;
		case '-':
			curr -= s;
			if (curr < l)
				return 0;
			break;
		case '*':
			curr *= s;
			if (curr > l)
				return 0;
			break;
		case '/':
			curr /= s;
			if (curr < l)
				return 0;
			break;
		default:
			return -1;
		}
	}

	return -1;
}

/*
 * clo_check_range_params -- (internal) validate step and step type
 */
static int
clo_check_range_params(uint64_t step, char step_type)
{
	switch (step_type) {
	/*
	 * Cannot construct range with step equal to 0
	 * for '+' or '-' range.
	 */
	case '+':
	case '-':
		if (step == 0)
			return -1;
		break;
	/*
	 * Cannot construct range with step equal to 0 or 1
	 * for '*' or '/' range.
	 */
	case '*':
	case '/':
		if (step == 0 || step == 1)
			return -1;
		break;
	default:
		return -1;
	}

	return 0;
}

/*
 * clo_parse_range -- (internal) parse range or value
 *
 * The range may be in the following format:
 * <first>:<step type><step>:<last>
 *
 * Step type must be one of the following: +, -, *, /.
 */
static int
clo_parse_range(struct benchmark_clo *clo, const char *arg,
		clo_parse_single_fn parse_single, clo_eval_range_fn eval_range,
		struct clo_vec_vlist *vlist)
{
	char *str_first = NULL;
	char *str_step = NULL;
	char step_type = '\0';
	char *str_last = NULL;

	int ret = sscanf(arg, "%m[^:]:%c%m[^:]:%m[^:]", &str_first, &step_type,
			&str_step, &str_last);
	if (ret == 1) {
		/* single value */
		uint64_t value;

		if (parse_single(clo, arg, &value)) {
			ret = -1;
		} else {
			clo_vec_vlist_add(vlist, &value, clo->type_uint.size);
			ret = 0;
		}
	} else if (ret == 4) {
		/* range */
		uint64_t first = 0;
		uint64_t last = 0;
		uint64_t step = 0;

		if (parse_single(clo, str_first, &first)) {
			ret = -1;
			goto out;
		}

		char *end;
		errno = 0;
		step = strtoull(str_step, &end, 10);
		if (errno || !end || *end != '\0')
			return -1;

		if (parse_single(clo, str_last, &last)) {
			ret = -1;
			goto out;
		}

		if (clo_check_range_params(step, step_type)) {
			ret = -1;
			goto out;
		}

		/* evaluate the range */
		if (eval_range(clo, &first, &step, &last, step_type, vlist)) {
			ret = -1;
			goto out;
		}

		ret = 0;
	} else {
		ret = -1;
	}

out:
	free(str_first);
	free(str_step);
	free(str_last);

	return ret;
}

/*
 * clo_parse_ranges -- (internal) parse ranges/values separated by commas
 */
static int
clo_parse_ranges(struct benchmark_clo *clo, const char *arg,
	struct clo_vec *clovec, clo_parse_single_fn parse_single,
	clo_eval_range_fn eval_range)
{
	struct clo_vec_vlist *vlist = clo_vec_vlist_alloc();
	assert(vlist != NULL);

	int ret = 0;
	char *args = strdup(arg);
	assert(args != NULL);

	char *curr = args;
	char *next;

	/* iterate through all values separated by comma */
	while ((next = strchr(curr, ',')) != NULL) {
		*next = '\0';
		next++;

		/* parse each comma separated value as range or single value */
		if ((ret = clo_parse_range(clo, curr, parse_single,
				eval_range, vlist)))
			goto out;

		curr = next;
	}

	/* parse each comma separated value as range or single value */
	if ((ret = clo_parse_range(clo, curr, parse_single,
				eval_range, vlist)))
		goto out;

	/* add list of values to CLO vector */
	ret = clo_vec_memcpy_list(clovec, clo->off, clo->type_uint.size, vlist);
out:
	free(args);
	clo_vec_vlist_free(vlist);

	return ret;
}

/*
 * clo_parse_int -- (internal) parse int value
 */
static int
clo_parse_int(struct benchmark_clo *clo, const char *arg,
		struct clo_vec *clovec)
{
	return clo_parse_ranges(clo, arg, clovec, clo_parse_single_int,
			clo_eval_range_int);
}

/*
 * clo_parse_uint -- (internal) parse uint value
 */
static int
clo_parse_uint(struct benchmark_clo *clo, const char *arg,
		struct clo_vec *clovec)
{
	return clo_parse_ranges(clo, arg, clovec, clo_parse_single_uint,
			clo_eval_range_uint);
}

/*
 * clo_str_flag -- (internal) convert flag value to string
 */
static const char *
clo_str_flag(struct benchmark_clo *clo, void *addr, size_t size)
{
	if (clo->off + sizeof (bool) > size)
		return NULL;

	bool flag = *(bool *)((char *)addr + clo->off);

	return flag ? "true" : "false";
}

/*
 * clo_str_str -- (internal) convert str value to string
 */
static const char *
clo_str_str(struct benchmark_clo *clo, void *addr, size_t size)
{
	if (clo->off + sizeof (char *) > size)
		return NULL;

	return *(char **)((char *)addr + clo->off);
}

/*
 * clo_str_int -- (internal) convert int value to string
 */
static const char *
clo_str_int(struct benchmark_clo *clo, void *addr, size_t size)
{
	if (clo->off + clo->type_int.size > size)
		return NULL;

	void *val = (char *)addr + clo->off;

	switch (clo->type_int.size) {
		case 1:
			snprintf(str_buff, STR_BUFF_SIZE, "%d",
					*(int8_t *)val);
			break;
		case 2:
			snprintf(str_buff, STR_BUFF_SIZE, "%d",
					*(int16_t *)val);
			break;
		case 4:
			snprintf(str_buff, STR_BUFF_SIZE, "%d",
					*(int32_t *)val);
			break;
		case 8:
			snprintf(str_buff, STR_BUFF_SIZE, "%ld",
					*(int64_t *)val);
			break;
		default:
			return NULL;
	}

	return str_buff;
}

/*
 * clo_str_uint -- (internal) convert uint value to string
 */
static const char *
clo_str_uint(struct benchmark_clo *clo, void *addr, size_t size)
{
	if (clo->off + clo->type_uint.size > size)
		return NULL;

	void *val = (char *)addr + clo->off;

	switch (clo->type_uint.size) {
		case 1:
			snprintf(str_buff, STR_BUFF_SIZE, "%u",
					*(uint8_t *)val);
			break;
		case 2:
			snprintf(str_buff, STR_BUFF_SIZE, "%u",
					*(uint16_t *)val);
			break;
		case 4:
			snprintf(str_buff, STR_BUFF_SIZE, "%u",
					*(uint32_t *)val);
			break;
		case 8:
			snprintf(str_buff, STR_BUFF_SIZE, "%lu",
					*(uint64_t *)val);
			break;
		default:
			return NULL;
	}

	return str_buff;
}

/*
 * clo_parse -- (internal) array with functions for parsing CLOs
 */
static clo_parse_fn clo_parse[CLO_TYPE_MAX] = {
	[CLO_TYPE_FLAG] = clo_parse_flag,
	[CLO_TYPE_STR] = clo_parse_str,
	[CLO_TYPE_INT] = clo_parse_int,
	[CLO_TYPE_UINT] = clo_parse_uint,
};

/*
 * clo_str -- (internal) array with functions for converting to string
 */
static clo_str_fn clo_str[CLO_TYPE_MAX] = {
	[CLO_TYPE_FLAG] = clo_str_flag,
	[CLO_TYPE_STR] = clo_str_str,
	[CLO_TYPE_INT] = clo_str_int,
	[CLO_TYPE_UINT] = clo_str_uint,
};

/*
 * clo_get_by_short -- (internal) return CLO with specified short opt
 */
static struct benchmark_clo *
clo_get_by_short(struct benchmark_clo *clos, size_t nclo, char opt_short)
{
	size_t i;

	for (i = 0; i < nclo; i++) {
		if (clos[i].opt_short == opt_short)
			return &clos[i];
	}

	return NULL;
}

/*
 * clo_get_by_long -- (internal) return CLO with specified long opt
 */
static struct benchmark_clo *
clo_get_by_long(struct benchmark_clo *clos, size_t nclo, const char *opt_long)
{
	size_t i;

	for (i = 0; i < nclo; i++) {
		if (strcmp(clos[i].opt_long, opt_long) == 0)
			return &clos[i];
	}

	return NULL;
}

/*
 * clo_get_optstr -- (internal) returns option string from CLOs
 *
 * This function returns option string which contains all short
 * options from CLO structure.
 * The returned value must be freed by caller.
 */
static char *
clo_get_optstr(struct benchmark_clo *clos, size_t nclo)
{
	size_t i;
	char *optstr;
	char *ptr;
	/*
	 * In worst case every option requires an argument
	 * so we need space for ':' character + terminating
	 * NULL.
	 */
	size_t optstrlen = nclo * 2 + 1;

	optstr = calloc(1, optstrlen);
	assert(optstr != NULL);

	ptr = optstr;
	for (i = 0; i < nclo; i++) {
		if (clos[i].opt_short) {
			*(ptr++) = clos[i].opt_short;
			if (clos[i].type != CLO_TYPE_FLAG)
				*(ptr++) = ':';
		}
	}

	return optstr;
}

/*
 * clo_get_long_options -- (internal) allocate long options structure
 *
 * This function allocates structure for long options and fills all
 * entries according to values from becnhmark_clo. This is essentially
 * conversion from struct benchmark_clo to struct option.
 * The returned value must be freed by caller.
 */
static struct option *
clo_get_long_options(struct benchmark_clo *clos, size_t nclo)
{
	size_t i;
	struct option *options;

	options = calloc(nclo + 1, sizeof (struct option));
	assert(options != NULL);

	for (i = 0; i < nclo; i++) {
		options[i].name = clos[i].opt_long;
		options[i].val = clos[i].opt_short;
		/* no optional arguments  */
		if (clos[i].type == CLO_TYPE_FLAG) {
			options[i].has_arg = no_argument;
		} else {
			options[i].has_arg = required_argument;
		}
	}

	return options;
}

/*
 * clo_set_defaults -- (internal) set default values
 *
 * Default values are stored as strings in CLO
 * structure so this function parses default values in
 * the same manner as values passed by user. Returns -1
 * if argument was not passed by user and default value
 * is missing.
 */
static int
clo_set_defaults(struct benchmark_clo *clos, size_t nclo,
		struct clo_vec *clovec)
{
	size_t i;

	for (i = 0; i < nclo; i++) {

		if (clos[i].used)
			continue;
		/*
		 * If option was not used and default value
		 * is not specified, return error. Otherwise
		 * parse the default value in the same way as
		 * values passed by user. Except for the flag.
		 * If the flag default value was not specified
		 * assign "false" value to it.
		 */
		if (clos[i].def) {
			if (clo_parse[clos[i].type](&clos[i],
					clos[i].def, clovec))
				return -1;
		} else if (clos[i].type == CLO_TYPE_FLAG) {
			if (clo_parse[clos[i].type](&clos[i], "false", clovec))
				return -1;
		} else {
			printf("'%s' is required option\n", clos[i].opt_long);
			return -1;
		}
	}

	return 0;
}

/*
 * benchmark_clo_parse -- parse CLOs and store values in desired structure
 *
 * This function parses command line arguments according to information
 * from CLOs structure. The parsed values are stored in CLO vector
 * pointed by clovec. If any of command line options are not passed by user,
 * the default value is stored if exists. Otherwise it means the argument is
 * required and error is returned.
 *
 * - argc   - number of command line options passed by user
 * - argv   - command line options passed by user
 * - clos   - description of available command line options
 * - nclos  - number of available command line options
 * - clovec - vector of arguments
 */
int
benchmark_clo_parse(int argc, char *argv[], struct benchmark_clo *clos,
		size_t nclos, struct clo_vec *clovec)
{
	char *optstr;
	struct option *options;
	int ret = 0;
	int opt;
	int optindex;

	/* convert CLOs to option string and long options structure */
	optstr = clo_get_optstr(clos, nclos);
	options = clo_get_long_options(clos, nclos);

	/* parse CLOs as long and/or short options */
	while ((opt = getopt_long(argc, argv, optstr,
				options, &optindex)) != -1) {
		struct benchmark_clo *clo = NULL;
		if (opt) {
			clo = clo_get_by_short(clos, nclos, opt);
		} else {
			assert(optindex < nclos);
			clo = &clos[optindex];
		}
		if (!clo) {
			ret = -1;
			goto out;
		}

		/* invoke parser according to type of CLO */
		assert(clo->type < CLO_TYPE_MAX);
		ret = clo_parse[clo->type](clo, optarg, clovec);
		if (ret)
			goto out;

		/* mark CLO as used */
		clo->used = optarg != NULL || clo->type == CLO_TYPE_FLAG;
	}

	if (optind < argc) {
		fprintf(stderr, "Unknown option: %s\n", argv[optind]);
		ret = -1;
		goto out;
	}
	/* parse unused CLOs with default values */
	ret = clo_set_defaults(clos, nclos, clovec);

out:
	free(options);
	free(optstr);

	if (ret)
		errno = EINVAL;

	return ret;
}

/*
 * benchmark_clo_parse_scenario -- parse CLOs from scenario
 *
 * This function parses command line arguments according to information
 * from CLOs structure. The parsed values are stored in CLO vector
 * pointed by clovec. If any of command line options are not passed by user,
 * the default value is stored if exists. Otherwise it means the argument is
 * required and error is returned.
 *
 * - scenario - scenario with key value arguments
 * - clos     - description of available command line options
 * - nclos    - number of available command line options
 * - clovec   - vector of arguments
 */
int
benchmark_clo_parse_scenario(struct scenario *scenario,
		struct benchmark_clo *clos, size_t nclos,
		struct clo_vec *clovec)
{
	struct kv *kv;

	FOREACH_KV(kv, scenario) {
		struct benchmark_clo *clo =
			clo_get_by_long(clos, nclos, kv->key);
		if (!clo) {
			fprintf(stderr,
				"unrecognized option -- '%s'\n", kv->key);
			return -1;
		}

		assert(clo->type < CLO_TYPE_MAX);
		if (clo_parse[clo->type](clo, kv->value, clovec)) {
			fprintf(stderr, "parsing option -- '%s' failed\n",
				kv->value);
			return -1;
		}

		/* mark CLO as used */
		clo->used = 1;
	}

	return clo_set_defaults(clos, nclos, clovec);
}

/*
 * benchmark_override_clos_in_scenario - parse the command line arguments and
 * override/add the parameters in/to the scenario by replacing/adding the kv
 * struct in/to the scenario.
 *
 * - scenario - scenario with key value arguments
 * - argc     - command line arguments number
 * - argv     - command line arguments vector
 * - clos     - description of available command line options
 * - nclos    - number of available command line options
 */
int
benchmark_override_clos_in_scenario(struct scenario *scenario,
				    int argc, char *argv[],
				    struct benchmark_clo *clos, size_t nclos)
{
	char *optstr;
	struct option *options;
	int ret = 0;
	int opt;
	int optindex;
	const char *true_str = "true";

	/* convert CLOs to option string and long options structure */
	optstr = clo_get_optstr(clos, nclos);
	options = clo_get_long_options(clos, nclos);

	/* parse CLOs as long and/or short options */
	while ((opt = getopt_long(argc, argv, optstr,
				options, &optindex)) != -1) {
		struct benchmark_clo *clo = NULL;
		if (opt) {
			clo = clo_get_by_short(clos, nclos, opt);
		} else {
			assert(optindex < nclos);
			clo = &clos[optindex];
		}
		if (!clo) {
			ret = -1;
			goto out;
		}

		/* Check if the given clo is defined in the scenario */
		struct kv *kv = find_kv_in_scenario(clo->opt_long, scenario);
		if (kv) { /* replace the value in the scenario */
			if (optarg != NULL && clo->type != CLO_TYPE_FLAG) {
				free(kv->value);
				kv->value = strdup(optarg);
			} else if (optarg == NULL &&
					clo->type == CLO_TYPE_FLAG) {
				free(kv->value);
				kv->value = strdup(true_str);
			} else {
				ret = -1;
				goto out;
			}
		} else { /* add a new param to the scenario */
			if (optarg != NULL && clo->type != CLO_TYPE_FLAG) {
				kv = kv_alloc(clo->opt_long, optarg);
				TAILQ_INSERT_TAIL(&scenario->head, kv, next);
			} else if (optarg == NULL &&
					clo->type == CLO_TYPE_FLAG) {
				kv = kv_alloc(clo->opt_long, true_str);
				TAILQ_INSERT_TAIL(&scenario->head, kv, next);
			} else {
				ret = -1;
				goto out;
			}
		}
	}

	if (optind < argc) {
		fprintf(stderr, "Unknown option: %s\n", argv[optind]);
		ret = -1;
		goto out;
	}

out:
	free(options);
	free(optstr);

	if (ret)
		errno = EINVAL;

	return ret;
}


/*
 * benchmark_clo_str -- converts command line option to string
 *
 * According to command line option type and parameters, converts
 * the value from structure pointed by args of size size.
 */
const char *
benchmark_clo_str(struct benchmark_clo *clo, void *args, size_t size)
{
	assert(clo->type < CLO_TYPE_MAX);
	return clo_str[clo->type](clo, args, size);
}

/*
 * clo_get_scenarios - search the command line arguments for scenarios listed in
 * available_scenarios and put them in found_scenarios. Returns the number of
 * found scenarios in the cmd line or -1 on error. The passed cmd line
 * args should contain the scenario name(s) as the first argument(s) - starting
 * from index 0
 */
int
clo_get_scenarios(int argc, char *argv[],
			struct scenarios *available_scenarios,
			struct scenarios *found_scenarios)
{
	assert(argv != NULL);
	assert(available_scenarios != NULL);
	assert(found_scenarios != NULL);

	if (argc <= 0) {
		fprintf(stderr,
			"clo get scenarios, argc invalid value: %d\n", argc);
		return -1;
	}
	int tmp_argc = argc;
	char **tmp_argv = argv;

	do {
		struct scenario *scenario =
			scenarios_get_scenario(available_scenarios, *tmp_argv);

		if (!scenario) {
			fprintf(stderr, "unknown scenario: %s\n", *tmp_argv);
			return -1;
		}

		struct scenario *new_scenario = clone_scenario(scenario);
		assert(new_scenario != NULL);

		TAILQ_INSERT_TAIL(&found_scenarios->head,
					new_scenario, next);
		tmp_argc--;
		tmp_argv++;
	} while (tmp_argc &&
		contains_scenarios(tmp_argc, tmp_argv, available_scenarios));

	return argc - tmp_argc;
}
