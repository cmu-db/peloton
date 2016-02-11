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
 * set.c -- pool set utilities
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <endian.h>
#include <errno.h>
#include <stddef.h>
#include <time.h>
#include <uuid/uuid.h>
#include <ctype.h>
#include <linux/limits.h>

#include "libpmem.h"
#include "util.h"
#include "out.h"
#include "valgrind_internal.h"

extern unsigned long Pagesize;

/* reserve space for size, path and some whitespace and/or comment */
#define	PARSER_MAX_LINE (PATH_MAX + 1024)

enum parser_codes {
	PARSER_CONTINUE = 0,
	PARSER_PMEMPOOLSET,
	PARSER_REPLICA,
	PARSER_SIZE_PATH_EXPECTED,
	PARSER_WRONG_SIZE,
	PARSER_WRONG_PATH,
	PARSER_SET_NO_PARTS,
	PARSER_REP_NO_PARTS,
	PARSER_SIZE_MISMATCH,
	PARSER_FORMAT_OK,
	PARSER_MAX_CODE
};

static const char *parser_errstr[PARSER_MAX_CODE] = {
	"", /* parsing */
	"the first line must be exactly 'PMEMPOOLSET'",
	"exactly 'REPLICA' expected",
	"size and path expected",
	"incorrect format of size",
	"incorrect path (must be an absolute path)",
	"no pool set parts",
	"no replica parts",
	"sizes of pool set and replica mismatch",
	"" /* format correct */
};

/*
 * util_map_part -- (internal) map a header of a pool set
 */
static int
util_map_hdr(struct pool_set_part *part, size_t size, int flags)
{
	LOG(3, "part %p size %zu flags %d", part, size, flags);

	ASSERTne(size, 0);
	ASSERTeq(size % Pagesize, 0);

	void *hdrp = mmap(NULL, size,
		PROT_READ|PROT_WRITE, flags, part->fd, 0);

	if (hdrp == MAP_FAILED) {
		ERR("!mmap: %s", part->path);
		return -1;
	}

	part->hdrsize = size;
	part->hdr = hdrp;

	VALGRIND_REGISTER_PMEM_MAPPING(part->hdr, part->hdrsize);
	VALGRIND_REGISTER_PMEM_FILE(part->fd, part->hdr, part->hdrsize, 0);

	return 0;
}

/*
 * util_unmap_hdr -- unmap pool set part header
 */
static int
util_unmap_hdr(struct pool_set_part *part)
{
	if (part->hdr != NULL && part->hdrsize != 0) {
		LOG(4, "munmap: addr %p size %zu", part->hdr, part->hdrsize);
		if (munmap(part->hdr, part->hdrsize) != 0) {
			ERR("!munmap: %s", part->path);
		}
		VALGRIND_REMOVE_PMEM_MAPPING(part->hdr, part->hdrsize);
		part->hdr = NULL;
		part->hdrsize = 0;
	}

	return 0;
}


/*
 * util_map_part -- (internal) map a part of a pool set
 */
static int
util_map_part(struct pool_set_part *part, void *addr, size_t size,
	size_t offset, int flags)
{
	LOG(3, "part %p addr %p size %zu offset %zu flags %d",
		part, addr, size, offset, flags);

	ASSERTeq((uintptr_t)addr % Pagesize, 0);
	ASSERTeq(offset % Pagesize, 0);
	ASSERTeq(size % Pagesize, 0);
	ASSERT(((off_t)offset) >= 0);

	if (!size)
		size = (part->filesize & ~(Pagesize - 1)) - offset;

	void *addrp = mmap(addr, size,
		PROT_READ|PROT_WRITE, flags, part->fd, (off_t)offset);

	if (addrp == MAP_FAILED) {
		ERR("!mmap: %s", part->path);
		return -1;
	}

	part->addr = addrp;
	part->size = size;

	if (addr != NULL && (flags & MAP_FIXED) && part->addr != addr) {
		ERR("!mmap: %s", part->path);
		munmap(addr, size);
		return -1;
	}

	VALGRIND_REGISTER_PMEM_MAPPING(part->addr, part->size);
	VALGRIND_REGISTER_PMEM_FILE(part->fd, part->addr, part->size, offset);

	return 0;
}

/*
 * util_unmap_part -- (internal) unmap a part of a pool set
 */
static int
util_unmap_part(struct pool_set_part *part)
{
	LOG(3, "part %p", part);

	if (part->addr != NULL && part->size != 0) {
		LOG(4, "munmap: addr %p size %zu", part->addr, part->size);
		if (munmap(part->addr, part->size) != 0) {
			ERR("!munmap: %s", part->path);
		}

		VALGRIND_REMOVE_PMEM_MAPPING(part->addr, part->size);
		part->addr = NULL;
		part->size = 0;
	}

	return 0;
}

/*
 * util_poolset_free -- free pool set info
 */
void
util_poolset_free(struct pool_set *set)
{
	LOG(3, "set %p", set);

	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			Free((void *)(rep->part[p].path));
		}
		Free(set->replica[r]);
	}

	Free(set);
}

/*
 * util_poolset_close -- unmap and close all the parts of the pool set
 *
 * Optionally, it also unlinks the newly created pool set files.
 */
void
util_poolset_close(struct pool_set *set, int del)
{
	LOG(3, "set %p del %d", set, del);

	int oerrno = errno;

	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		/* it's enough to unmap part[0] only */
		util_unmap_part(&rep->part[0]);
		for (unsigned p = 0; p < rep->nparts; p++) {
			if (rep->part[p].fd != -1)
				(void) close(rep->part[p].fd);
			if (del && rep->part[p].created) {
				LOG(4, "unlink %s", rep->part[p].path);
				unlink(rep->part[p].path);
			}
		}
	}

	util_poolset_free(set);
	errno = oerrno;
}

/*
 * util_poolset_chmod -- change mode for all created files related to pool set
 */
int
util_poolset_chmod(struct pool_set *set, mode_t mode)
{
	LOG(3, "set %p mode %o", set, mode);

	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];

		for (unsigned p = 0; p < rep->nparts; p++) {
			struct pool_set_part *part = &rep->part[p];

			if (!part->created)
				continue;

			struct stat st;
			if (fstat(part->fd, &st)) {
				ERR("!fstat");
				return -1;
			}

			if (st.st_mode & ~(unsigned)S_IFMT) {
				LOG(1, "file permissions changed during pool "
					"initialization, file: %s (%o)",
					part->path,
					st.st_mode & ~(unsigned)S_IFMT);
			}

			if (fchmod(part->fd, mode)) {
				ERR("!fchmod %u/%u/%s", r, p,
						rep->part[p].path);
				return -1;
			}
		}
	}

	return 0;
}

/*
 * util_poolset_fdclose -- close file descriptors related to pool set
 */
void
util_poolset_fdclose(struct pool_set *set)
{
	LOG(3, "set %p", set);

	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];

		for (unsigned p = 0; p < rep->nparts; p++) {
			struct pool_set_part *part = &rep->part[p];

			if (part->fd != -1)
				close(part->fd);
		}
	}
}

/*
 * parser_get_next_token -- (internal) extract token from string
 */
static char *
parser_get_next_token(char **line)
{
	while (*line && isblank(**line))
		(*line)++;

	return strsep(line, " \t");
}

/*
 * parser_read_line -- (internal) read line and validate size and path
 *                      from a pool set file
 */
static enum parser_codes
parser_read_line(char *line, size_t *size, char **path)
{
	char *size_str, *path_str, *endptr;

	size_str = parser_get_next_token(&line);
	path_str = parser_get_next_token(&line);

	if (!size_str || !path_str)
		return PARSER_SIZE_PATH_EXPECTED;

	LOG(10, "size '%s' path '%s'", size_str, path_str);

	/*
	 * A format of the size is checked in detail. As regards the path,
	 * it is checked only if the read path is an absolute path.
	 * The rest should be checked during creating/opening the file.
	 */

	/* check if the read path is an absolute path */
	if (path_str[0] != '/')
		return PARSER_WRONG_PATH; /* must be an absolute path */

	*path = Strdup(path_str);

	/* check format of size */
	int ufound = 0;
	*size = strtoull(size_str, &endptr, 10);
	while (endptr && endptr[0] != '\0') {
		if ((endptr[0] == 'b' || endptr[0] == 'B') &&
		    (endptr[1] == '\0'))
			return PARSER_CONTINUE;

		if (ufound) {
			Free(*path);
			*path = NULL;
			return PARSER_WRONG_SIZE;
		}

		/* multiply size by a unit */
		switch (endptr[0]) {
			case 'k':
			case 'K':
				*size *= 1ULL << 10; /* 1 KB */
				break;
			case 'm':
			case 'M':
				*size *= 1ULL << 20; /* 1 MB */
				break;
			case 'g':
			case 'G':
				*size *= 1ULL << 30; /* 1 GB */
				break;
			case 't':
			case 'T':
				*size *= 1ULL << 40; /* 1 TB */
				break;
			default:
				Free(*path);
				*path = NULL;
				return PARSER_WRONG_SIZE;
		}

		ufound = 1;
		endptr++;
	}

	return PARSER_CONTINUE;
}

/*
 * util_parse_add_part -- (internal) add a new part file to the replica info
 */
static int
util_parse_add_part(struct pool_set *set, const char *path, size_t filesize)
{
	LOG(3, "set %p path %s filesize %zu", set, path, filesize);

	ASSERTne(set, NULL);

	struct pool_replica *rep = set->replica[set->nreplicas - 1];
	ASSERTne(rep, NULL);

	/* XXX - pre-allocate space for X parts, and reallocate every X parts */
	rep = Realloc(rep, sizeof (struct pool_replica) +
			(rep->nparts + 1) * sizeof (struct pool_set_part));
	if (rep == NULL) {
		ERR("!Realloc");
		return -1;
	}
	set->replica[set->nreplicas - 1] = rep;

	unsigned p = rep->nparts++;

	rep->part[p].path = path;
	rep->part[p].filesize = filesize;
	rep->part[p].fd = -1;
	rep->part[p].created = 0;
	rep->part[p].hdr = NULL;
	rep->part[p].addr = NULL;

	return 0;
}

/*
 * util_parse_add_replica -- (internal) add a new replica to the pool set info
 */
static int
util_parse_add_replica(struct pool_set **setp)
{
	LOG(3, "setp %p", setp);

	ASSERTne(setp, NULL);

	struct pool_set *set = *setp;
	ASSERTne(set, NULL);

	set = Realloc(set, sizeof (struct pool_set) +
			(set->nreplicas + 1) * sizeof (struct pool_replica *));
	if (set == NULL) {
		ERR("!Realloc");
		return -1;
	}
	*setp = set;

	struct pool_replica *rep;
	rep = Malloc(sizeof (struct pool_replica));
	if (rep == NULL) {
		ERR("!Malloc");
		return -1;
	}
	memset(rep, 0, sizeof (struct pool_replica));

	unsigned r = set->nreplicas++;

	set->replica[r] = rep;

	return 0;
}

/*
 * util_poolset_parse -- (internal) parse pool set config file
 *
 * Returns 1 if the file is a valid pool set config file, 0 if the file
 * is not a pool set header, and -1 in case of any error.
 *
 * XXX: use memory mapped file
 */
int
util_poolset_parse(const char *path, int fd, struct pool_set **setp)
{
	LOG(3, "path %s fd %d setp %p", path, fd, setp);

	struct pool_set *set;
	enum parser_codes result;
	char line[PARSER_MAX_LINE];
	char *s;
	char *ppath;
	char *cp;
	size_t psize;
	FILE *fs;

	if (lseek(fd, 0, SEEK_SET) != 0) {
		ERR("!lseek %d", fd);
		return -1;
	}

	fd = dup(fd);
	if (fd < 0) {
		ERR("!dup");
		return -1;
	}

	/* associate a stream with the file descriptor */
	if ((fs = fdopen(fd, "r")) == NULL) {
		ERR("!fdopen %d", fd);
		close(fd);
		return -1;
	}

	unsigned nlines = 0;
	unsigned nparts = 0; /* number of parts in current replica */

	/* read the first line */
	s = fgets(line, PARSER_MAX_LINE, fs);
	nlines++;

	set = Malloc(sizeof (struct pool_set));
	if (set == NULL) {
		ERR("!Malloc for pool set");
		goto err;
	}
	memset(set, 0, sizeof (struct pool_set));

	/* check also if the last character is '\n' */
	if (s && strncmp(line, POOLSET_HDR_SIG, POOLSET_HDR_SIG_LEN) == 0 &&
	    line[POOLSET_HDR_SIG_LEN] == '\n') {
		/* 'PMEMPOOLSET' signature detected */
		LOG(10, "PMEMPOOLSET");

		int ret = util_parse_add_replica(&set);
		if (ret != 0)
			goto err;

		nparts = 0;
		result = PARSER_CONTINUE;
	} else {
		result = PARSER_PMEMPOOLSET;
	}

	while (result == PARSER_CONTINUE) {
		/* read next line */
		s = fgets(line, PARSER_MAX_LINE, fs);
		nlines++;

		if (s) {
			/* chop off newline and comments */
			if ((cp = strchr(line, '\n')) != NULL)
				*cp = '\0';
			if (cp != s && (cp = strchr(line, '#')) != NULL)
				*cp = '\0';

			/* skip comments and blank lines */
			if (cp == s)
				continue;
		}

		if (!s) {
			if (nparts >= 1) {
				result = PARSER_FORMAT_OK;
			} else {
				if (set->nreplicas == 1)
					result = PARSER_SET_NO_PARTS;
				else
					result = PARSER_REP_NO_PARTS;
			}
		} else if (strncmp(line, POOLSET_REPLICA_SIG,
					POOLSET_REPLICA_SIG_LEN) == 0) {
			if (line[POOLSET_REPLICA_SIG_LEN] != '\0') {
				/* something more than 'REPLICA' */
				result = PARSER_REPLICA;
			} else if (nparts >= 1) {
				/* 'REPLICA' signature detected */
				LOG(10, "REPLICA");

				int ret = util_parse_add_replica(&set);
				if (ret != 0)
					goto err;

				nparts = 0;
				result = PARSER_CONTINUE;
			} else {
				if (set->nreplicas == 1)
					result = PARSER_SET_NO_PARTS;
				else
					result = PARSER_REP_NO_PARTS;
			}
		} else {
			/* read size and path */
			result = parser_read_line(line, &psize, &ppath);
			if (result == PARSER_CONTINUE) {
				/* add a new pool's part to the list */
				int ret = util_parse_add_part(set,
					ppath, psize);
				if (ret != 0)
					goto err;
				nparts++;
			}
		}
	}

	if (result == PARSER_FORMAT_OK) {
		LOG(4, "set file format correct (%s)", path);
		(void) fclose(fs);
		*setp = set;
		return 0;
	} else {
		ERR("%s [%s:%d]", path, parser_errstr[result], nlines);
	}

err:
	(void) fclose(fs);
	if (set)
		util_poolset_free(set);
	return -1;
}

/*
 * util_poolset_single -- (internal) create a one-part pool set
 *
 * On success returns a pointer to a newly allocated and initialized
 * pool set structure.  Otherwise, NULL is returned.
 */
static struct pool_set *
util_poolset_single(const char *path, size_t filesize, int fd, int create)
{
	LOG(3, "path %s filesize %zu fd %d create %d",
			path, filesize, fd, create);

	struct pool_set *set;
	set = Malloc(sizeof (struct pool_set) +
			sizeof (struct pool_replica *));
	if (set == NULL) {
		ERR("!Malloc for pool set");
		return NULL;
	}
	memset(set, 0, sizeof (struct pool_set) +
			sizeof (struct pool_replica *));

	struct pool_replica *rep;
	rep = Malloc(sizeof (struct pool_replica) +
			sizeof (struct pool_set_part));
	if (rep == NULL) {
		ERR("!Malloc for pool set replica");
		Free(set);
		return NULL;
	}

	set->replica[0] = rep;

	rep->part[0].filesize = filesize;
	rep->part[0].path = Strdup(path);
	rep->part[0].fd = fd;
	rep->part[0].created = create;
	rep->part[0].hdr = NULL;
	rep->part[0].addr = NULL;

	rep->nparts = 1;
	/* round down to the nearest page boundary */
	rep->repsize = rep->part[0].filesize & ~(Pagesize - 1);

	set->nreplicas = 1;

	return set;
}

/*
 * util_poolset_file -- (internal) open or create a single part file
 */
static int
util_poolset_file(struct pool_set_part *part, size_t minsize, int create)
{
	LOG(3, "part %p minsize %zu create %d", part, minsize, create);

	/* check if file exists */
	if (access(part->path, F_OK) == 0)
		create = 0;

	size_t size;

	if (create) {
		size = part->filesize;
		part->fd = util_file_create(part->path, size, minsize);
		part->created = 1;
		if (part->fd == -1) {
			LOG(2, "failed to create file: %s", part->path);
			return -1;
		}
	} else {
		size = 0;
		part->fd = util_file_open(part->path, &size, minsize, O_RDWR);
		part->created = 0;
		if (part->fd == -1) {
			LOG(2, "failed to open file: %s", part->path);
			return -1;
		}

		/* check if filesize matches */
		if (part->filesize != size) {
			ERR("file size does not match config: %s, %zu != %zu",
				part->path, size, part->filesize);
			errno = EINVAL;
			return -1;
		}
	}

	return 0;
}

/*
 * util_poolset_files -- (internal) open or create all the part files
 *                       of a pool set and replica sets
 */
static int
util_poolset_files(struct pool_set *set, size_t minsize, int create)
{
	LOG(3, "set %p minsize %zu create %d", set, minsize, create);

	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			if (util_poolset_file(&rep->part[p], minsize, create))
				return -1;

			rep->repsize +=
				(rep->part[p].filesize & ~(Pagesize - 1));
		}
	}

	return 0;
}

/*
 * util_poolset_create -- (internal) create a new memory pool set
 *
 * On success returns 0 and a pointer to a newly allocated structure
 * containing the info of all the parts of the pool set and replicas.
 */
static int
util_poolset_create(struct pool_set **setp, const char *path, size_t poolsize,
	size_t minsize)
{
	LOG(3, "setp %p path %s poolsize %zu minsize %zu",
		setp, path, poolsize, minsize);

	int oerrno;
	int ret = 0;
	int fd;
	size_t size = 0;

	if (poolsize != 0) {
		/* create a new file */
		fd = util_file_create(path, poolsize, minsize);
		if (fd == -1)
			return -1;

		/* close the file and open with O_RDWR */
		*setp = util_poolset_single(path, poolsize, fd, 1);
		if (*setp == NULL) {
			ret = -1;
			goto err;
		}

		/* do not close the file */
		return 0;
	}

	/* do not check minsize */
	if ((fd = util_file_open(path, &size, 0, O_RDONLY)) == -1)
		return -1;

	char signature[POOLSET_HDR_SIG_LEN];
	/*
	 * read returns ssize_t, but we know it will return value between -1
	 * and POOLSET_HDR_SIG_LEN (11), so we can safely cast it to int
	 */
	ret = (int)read(fd, signature, POOLSET_HDR_SIG_LEN);
	if (ret < 0) {
		ERR("!read %d", fd);
		goto err;
	}

	if (ret < POOLSET_HDR_SIG_LEN ||
	    strncmp(signature, POOLSET_HDR_SIG, POOLSET_HDR_SIG_LEN)) {
		LOG(4, "not a pool set header");

		if (size < minsize) {
			ERR("size %zu smaller than %zu", size, minsize);
			errno = EINVAL;
			ret = -1;
			goto err;
		}

		(void) close(fd);
		size = 0;
		if ((fd = util_file_open(path, &size, 0, O_RDWR)) == -1)
			return -1;

		*setp = util_poolset_single(path, size, fd, 0);
		if (*setp == NULL) {
			ret = -1;
			goto err;
		}

		/* do not close the file */
		return 0;
	}

	ret = util_poolset_parse(path, fd, setp);
	if (ret != 0)
		goto err;

	ret = util_poolset_files(*setp, minsize, 1);
	if (ret != 0)
		util_poolset_close(*setp, 1);

err:
	oerrno = errno;
	(void) close(fd);
	errno = oerrno;
	return ret;
}

/*
 * util_poolset_open -- (internal) open memory pool set
 *
 * On success returns 0 and a pointer to a newly allocated structure
 * containing the info of all the parts of the pool set and replicas.
 */
static int
util_poolset_open(struct pool_set **setp, const char *path, size_t minsize)
{
	LOG(3, "setp %p path %s minsize %zu", setp, path, minsize);

	int oerrno;
	int ret = 0;
	int fd;
	size_t size = 0;

	/* do not check minsize */
	if ((fd = util_file_open(path, &size, 0, O_RDONLY)) == -1)
		return -1;

	char signature[POOLSET_HDR_SIG_LEN];
	/*
	 * read returns ssize_t, but we know it will return value between -1
	 * and POOLSET_HDR_SIG_LEN (11), so we can safely cast it to int
	 */
	ret = (int)read(fd, signature, POOLSET_HDR_SIG_LEN);
	if (ret < 0) {
		ERR("!read %d", fd);
		goto err;
	}

	if (ret < POOLSET_HDR_SIG_LEN ||
	    strncmp(signature, POOLSET_HDR_SIG, POOLSET_HDR_SIG_LEN)) {
		LOG(4, "not a pool set header");

		if (size < minsize) {
			ERR("size %zu smaller than %zu", size, minsize);
			errno = EINVAL;
			ret = -1;
			goto err;
		}

		/* close the file and open with O_RDWR */
		(void) close(fd);
		size = 0;
		if ((fd = util_file_open(path, &size, 0, O_RDWR)) == -1)
			return -1;

		*setp = util_poolset_single(path, size, fd, 0);
		if (*setp == NULL) {
			ret = -1;
			goto err;
		}

		/* do not close the file */
		return 0;
	}

	ret = util_poolset_parse(path, fd, setp);
	if (ret != 0)
		goto err;

	ret = util_poolset_files(*setp, minsize, 0);
	if (ret != 0)
		util_poolset_close(*setp, 0);

err:
	oerrno = errno;
	(void) close(fd);
	errno = oerrno;
	return ret;
}

#define	REP(set, r)\
	((set)->replica[((set)->nreplicas + (r)) % (set)->nreplicas])

#define	PART(rep, p)\
	((rep)->part[((rep)->nparts + (p)) % (rep)->nparts])

#define	HDR(rep, p)\
	((struct pool_hdr *)(PART(rep, p).hdr))

/*
 * util_header_create -- (internal) create header of a single pool set file
 */
static int
util_header_create(struct pool_set *set, unsigned repidx, unsigned partidx,
	size_t hdrsize, const char *sig, uint32_t major, uint32_t compat,
	uint32_t incompat, uint32_t ro_compat)
{
	LOG(3, "set %p repidx %u partidx %u hdrsize %zu sig %.8s major %u "
		"compat %#x incompat %#x ro_comapt %#x",
		set, repidx, partidx, hdrsize,
		sig, major, compat, incompat, ro_compat);

	struct pool_replica *rep = set->replica[repidx];

	/* opaque info lives at the beginning of mapped memory pool */
	struct pool_hdr *hdrp = rep->part[partidx].hdr;

	/* check if the pool header is all zeros */
	if (!util_is_zeroed(hdrp, sizeof (*hdrp))) {
		ERR("Non-empty file detected");
		errno = EINVAL;
		return -1;
	}

	/*
	 * Zero out the pool descriptor - just in case we fail right after
	 * header checksum is stored.
	 */
	void *descp = (void *)((uintptr_t)hdrp + sizeof (*hdrp));
	memset(descp, 0, hdrsize - sizeof (*hdrp));
	pmem_msync(descp, hdrsize - sizeof (*hdrp));

	/* create pool's header */
	memcpy(hdrp->signature, sig, POOL_HDR_SIG_LEN);
	hdrp->major = htole32(major);
	hdrp->compat_features = htole32(compat);
	hdrp->incompat_features = htole32(incompat);
	hdrp->ro_compat_features = htole32(ro_compat);

	memcpy(hdrp->poolset_uuid, set->uuid, POOL_HDR_UUID_LEN);

	memcpy(hdrp->uuid, PART(rep, partidx).uuid, POOL_HDR_UUID_LEN);

	/* link parts */
	memcpy(hdrp->prev_part_uuid, PART(rep, partidx - 1).uuid,
							POOL_HDR_UUID_LEN);
	memcpy(hdrp->next_part_uuid, PART(rep, partidx + 1).uuid,
							POOL_HDR_UUID_LEN);

	/* link replicas */
	memcpy(hdrp->prev_repl_uuid, PART(REP(set, repidx - 1), 0).uuid,
							POOL_HDR_UUID_LEN);
	memcpy(hdrp->next_repl_uuid, PART(REP(set, repidx + 1), 0).uuid,
							POOL_HDR_UUID_LEN);

	hdrp->crtime = htole64((uint64_t)time(NULL));

	if (util_get_arch_flags(&hdrp->arch_flags)) {
		ERR("Reading architecture flags failed\n");
		errno = EINVAL;
		return -1;
	}

	hdrp->arch_flags.alignment_desc =
		htole64(hdrp->arch_flags.alignment_desc);
	hdrp->arch_flags.e_machine =
		htole16(hdrp->arch_flags.e_machine);

	util_checksum(hdrp, sizeof (*hdrp), &hdrp->checksum, 1);

	/* store pool's header */
	pmem_msync(hdrp, sizeof (*hdrp));

	return 0;
}

/*
 * util_header_check -- (internal) validate header of a single pool set file
 */
static int
util_header_check(struct pool_set *set, unsigned repidx, unsigned partidx,
	const char *sig, uint32_t major, uint32_t compat, uint32_t incompat,
	uint32_t ro_compat)
{
	LOG(3, "set %p repidx %u partidx %u sig %.8s major %u "
		"compat %#x incompat %#x ro_comapt %#x",
		set, repidx, partidx, sig, major, compat, incompat, ro_compat);

	struct pool_replica *rep = set->replica[repidx];

	/* opaque info lives at the beginning of mapped memory pool */
	struct pool_hdr *hdrp = rep->part[partidx].hdr;
	struct pool_hdr hdr;

	memcpy(&hdr, hdrp, sizeof (hdr));

	if (!util_convert_hdr(&hdr)) {
		errno = EINVAL;
		return -1;
	}

	/* valid header found */
	if (memcmp(hdr.signature, sig, POOL_HDR_SIG_LEN)) {
		ERR("wrong pool type: \"%.8s\"", hdr.signature);
		errno = EINVAL;
		return -1;
	}

	if (hdr.major != major) {
		ERR("pool version %d (library expects %d)",
			hdr.major, major);
		errno = EINVAL;
		return -1;
	}

	if (util_check_arch_flags(&hdr.arch_flags)) {
		ERR("wrong architecture flags");
		errno = EINVAL;
		return -1;
	}

	/* check pool set UUID */
	if (memcmp(HDR(REP(set, 0), 0)->poolset_uuid, hdr.poolset_uuid,
						POOL_HDR_UUID_LEN)) {
		ERR("wrong pool set UUID");
		errno = EINVAL;
		return -1;
	}

	/* check pool set linkage */
	if (memcmp(HDR(rep, partidx - 1)->uuid, hdr.prev_part_uuid,
						POOL_HDR_UUID_LEN) ||
	    memcmp(HDR(rep, partidx + 1)->uuid, hdr.next_part_uuid,
						POOL_HDR_UUID_LEN)) {
		ERR("wrong part UUID");
		errno = EINVAL;
		return -1;
	}

	/* check format version */
	if (HDR(rep, 0)->major != hdrp->major) {
		ERR("incompatible pool format");
		errno = EINVAL;
		return -1;
	}

	/* check compatibility features */
	if (HDR(rep, 0)->compat_features != hdrp->compat_features ||
	    HDR(rep, 0)->incompat_features != hdrp->incompat_features ||
	    HDR(rep, 0)->ro_compat_features != hdrp->ro_compat_features) {
		ERR("incompatible feature flags");
		errno = EINVAL;
		return -1;
	}

	rep->part[partidx].rdonly = 0;

	int retval = util_feature_check(&hdr, incompat, ro_compat, compat);
	if (retval < 0)
		return -1;
	else if (retval == 0)
		rep->part[partidx].rdonly = 1;

	return 0;
}

/*
 * util_replica_create -- (internal) create a new memory pool replica
 */
static int
util_replica_create(struct pool_set *set, unsigned repidx, int flags,
	size_t hdrsize, const char *sig,
	uint32_t major, uint32_t compat, uint32_t incompat, uint32_t ro_compat)
{
	LOG(3, "set %p repidx %u flags %d hdrsize %zu sig %.8s major %u "
		"compat %#x incompat %#x ro_comapt %#x",
		set, repidx, flags, hdrsize, sig, major,
		compat, incompat, ro_compat);

	struct pool_replica *rep = set->replica[repidx];

	rep->repsize -= (rep->nparts - 1) * hdrsize;

	/* determine a hint address for mmap() */
	void *addr = util_map_hint(rep->repsize);
	if (addr == NULL) {
		ERR("cannot find a contiguous region of given size");
		return -1;
	}

	/* map the first part and reserve space for remaining parts */
	if (util_map_part(&rep->part[0], addr, rep->repsize, 0, flags) != 0) {
		LOG(2, "pool mapping failed - part #0");
		return -1;
	}

	VALGRIND_REGISTER_PMEM_MAPPING(rep->part[0].addr, rep->part[0].size);
	VALGRIND_REGISTER_PMEM_FILE(rep->part[0].fd,
				rep->part[0].addr, rep->part[0].size, 0);

	/* map all headers - don't care about the address */
	for (unsigned p = 0; p < rep->nparts; p++) {
		if (util_map_hdr(&rep->part[p], hdrsize, flags) != 0) {
			LOG(2, "header mapping failed - part #%d", p);
			goto err;
		}
	}

	/* create headers, set UUID's */
	for (unsigned p = 0; p < rep->nparts; p++) {
		if (util_header_create(set, repidx, p, hdrsize, sig, major,
				compat, incompat, ro_compat) != 0) {
			LOG(2, "header creation failed - part #%d", p);
			goto err;
		}
	}

	/* unmap all headers */
	for (unsigned p = 0; p < rep->nparts; p++)
		util_unmap_hdr(&rep->part[p]);

	set->zeroed &= rep->part[0].created;

	size_t mapsize = rep->part[0].filesize & ~(Pagesize - 1);
	addr = (char *)rep->part[0].addr + mapsize;

	/*
	 * map the remaining parts of the usable pool space (4K-aligned)
	 */
	for (unsigned p = 1; p < rep->nparts; p++) {
		/* map data part */
		if (util_map_part(&rep->part[p], addr, 0, hdrsize,
				flags | MAP_FIXED) != 0) {
			LOG(2, "usable space mapping failed - part #%d", p);
			goto err;
		}

		VALGRIND_REGISTER_PMEM_FILE(rep->part[p].fd,
			rep->part[p].addr, rep->part[p].size, hdrsize);

		mapsize += rep->part[p].size;
		set->zeroed &= rep->part[p].created;
		addr = (char *)addr + rep->part[p].size;
	}

	rep->is_pmem = pmem_is_pmem(rep->part[0].addr, rep->part[0].size);

	ASSERTeq(mapsize, rep->repsize);

	/* calculate pool size - choose the smallest replica size */
	if (rep->repsize < set->poolsize)
		set->poolsize = rep->repsize;

	LOG(3, "replica addr %p", rep->part[0].addr);

	return 0;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	for (unsigned p = 0; p < rep->nparts; p++)
		util_unmap_hdr(&rep->part[p]);
	util_unmap_part(&rep->part[0]);
	errno = oerrno;
	return -1;
}

/*
 * util_replica_close -- (internal) close a memory pool replica
 *
 * This function unmaps all mapped memory regions.
 */
static int
util_replica_close(struct pool_set *set, unsigned repidx)
{
	LOG(3, "set %p repidx %u\n", set, repidx);
	struct pool_replica *rep = set->replica[repidx];

	for (unsigned p = 0; p < rep->nparts; p++)
		util_unmap_hdr(&rep->part[p]);

	util_unmap_part(&rep->part[0]);

	return 0;
}


/*
 * util_pool_create -- create a new memory pool (set or a single file)
 *
 * On success returns 0 and a pointer to a newly allocated structure
 * containing the info of all the parts of the pool set and replicas.
 */
int
util_pool_create(struct pool_set **setp, const char *path, size_t poolsize,
	size_t minsize, size_t hdrsize, const char *sig,
	uint32_t major, uint32_t compat, uint32_t incompat, uint32_t ro_compat)
{
	LOG(3, "setp %p path %s poolsize %zu minsize %zu "
		"hdrsize %zu sig %.8s major %u "
		"compat %#x incompat %#x ro_comapt %#x",
		setp, path, poolsize, minsize, hdrsize,
		sig, major, compat, incompat, ro_compat);

	int flags = MAP_SHARED;

	int ret = util_poolset_create(setp, path, poolsize, minsize);
	if (ret < 0) {
		LOG(2, "cannot create pool set");
		return -1;
	}

	struct pool_set *set = *setp;

	ASSERT(set->nreplicas > 0);

	set->zeroed = 1;
	set->poolsize = SIZE_MAX;

	/* generate pool set UUID */
	uuid_generate(set->uuid);

	/* generate UUID's for all the parts */
	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		for (unsigned i = 0; i < rep->nparts; i++)
			uuid_generate(rep->part[i].uuid);
	}

	for (unsigned r = 0; r < set->nreplicas; r++) {
		if (util_replica_create(set, r, flags, hdrsize, sig,
				major, compat, incompat, ro_compat) != 0) {
			LOG(2, "replica creation failed");
			goto err;
		}
	}

	return 0;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	for (unsigned r = 0; r < set->nreplicas; r++)
		util_replica_close(set, r);
	util_poolset_close(set, 1);
	errno = oerrno;
	return -1;
}

/*
 * util_replica_open -- (internal) open a memory pool replica
 */
static int
util_replica_open(struct pool_set *set, unsigned repidx, int flags,
	size_t hdrsize)
{
	LOG(3, "set %p repidx %u flags %d hdrsize %zu\n",
		set, repidx, flags, hdrsize);

	struct pool_replica *rep = set->replica[repidx];

	rep->repsize -= (rep->nparts - 1) * hdrsize;

	/* determine a hint address for mmap() */
	void *addr = util_map_hint(rep->repsize);
	if (addr == NULL) {
		ERR("cannot find a contiguous region of given size");
		return -1;
	}

	/* map the first part and reserve space for remaining parts */
	if (util_map_part(&rep->part[0], addr, rep->repsize, 0, flags) != 0) {
		LOG(2, "pool mapping failed - part #0");
		return -1;
	}

	VALGRIND_REGISTER_PMEM_MAPPING(rep->part[0].addr, rep->part[0].size);
	VALGRIND_REGISTER_PMEM_FILE(rep->part[0].fd,
				rep->part[0].addr, rep->part[0].size, 0);

	/* map all headers - don't care about the address */
	for (unsigned p = 0; p < rep->nparts; p++) {
		if (util_map_hdr(&rep->part[p], hdrsize, flags) != 0) {
			LOG(2, "header mapping failed - part #%d", p);
			goto err;
		}
	}

	size_t mapsize = rep->part[0].filesize & ~(Pagesize - 1);
	addr = (char *)rep->part[0].addr + mapsize;

	/*
	 * map the remaining parts of the usable pool space
	 * (4K-aligned)
	 */
	for (unsigned p = 1; p < rep->nparts; p++) {
		/* map data part */
		if (util_map_part(&rep->part[p], addr, 0, hdrsize,
				flags | MAP_FIXED) != 0) {
			LOG(2, "usable space mapping failed - part #%d", p);
			goto err;
		}

		VALGRIND_REGISTER_PMEM_FILE(rep->part[p].fd,
			rep->part[p].addr, rep->part[p].size, hdrsize);

		mapsize += rep->part[p].size;
		addr = (char *)addr + rep->part[p].size;
	}

	rep->is_pmem = pmem_is_pmem(rep->part[0].addr, rep->part[0].size);

	ASSERTeq(mapsize, rep->repsize);

	/* calculate pool size - choose the smallest replica size */
	if (rep->repsize < set->poolsize)
		set->poolsize = rep->repsize;

	LOG(3, "replica addr %p", rep->part[0].addr);

	return 0;
err:
	LOG(4, "error clean up");
	int oerrno = errno;
	for (unsigned p = 0; p < rep->nparts; p++)
		util_unmap_hdr(&rep->part[p]);
	util_unmap_part(&rep->part[0]);
	errno = oerrno;
	return -1;
}

/*
 * util_pool_open_nocheck -- open a memory pool (set or a single file)
 *
 * This function opens opens a pool set without checking the header values.
 */
int
util_pool_open_nocheck(struct pool_set **setp, const char *path, int rdonly,
		size_t hdrsize)
{
	LOG(3, "setp %p path %s", setp, path);

	int flags = rdonly ? MAP_PRIVATE|MAP_NORESERVE : MAP_SHARED;

	int ret = util_poolset_open(setp, path, 0);
	if (ret < 0) {
		LOG(2, "cannot open pool set");
		return -1;
	}

	struct pool_set *set = *setp;

	ASSERT(set->nreplicas > 0);

	set->rdonly = 0;
	set->poolsize = SIZE_MAX;

	for (unsigned r = 0; r < set->nreplicas; r++) {
		if (util_replica_open(set, r, flags, hdrsize) != 0) {
			LOG(2, "replica open failed");
			goto err;
		}
	}

	/* unmap all headers */
	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++)
			util_unmap_hdr(&rep->part[p]);
	}

	return 0;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	for (unsigned r = 0; r < set->nreplicas; r++)
		util_replica_close(set, r);

	util_poolset_close(set, 0);
	errno = oerrno;
	return -1;
}

/*
 * util_pool_open -- open a memory pool (set or a single file)
 *
 * This routine does all the work, but takes a rdonly flag so internal
 * calls can map a read-only pool if required.
 */
int
util_pool_open(struct pool_set **setp, const char *path, int rdonly,
	size_t minsize, size_t hdrsize, const char *sig,
	uint32_t major, uint32_t compat, uint32_t incompat, uint32_t ro_compat)
{
	LOG(3, "setp %p path %s rdonly %d minsize %zu "
		"hdrsize %zu sig %.8s major %u "
		"compat %#x incompat %#x ro_comapt %#x",
		setp, path, rdonly, minsize, hdrsize,
		sig, major, compat, incompat, ro_compat);

	int flags = rdonly ? MAP_PRIVATE|MAP_NORESERVE : MAP_SHARED;

	int ret = util_poolset_open(setp, path, minsize);
	if (ret < 0) {
		LOG(2, "cannot open pool set");
		return -1;
	}

	struct pool_set *set = *setp;

	ASSERT(set->nreplicas > 0);

	set->rdonly = 0;
	set->poolsize = SIZE_MAX;

	for (unsigned r = 0; r < set->nreplicas; r++) {
		if (util_replica_open(set, r, flags, hdrsize) != 0) {
			LOG(2, "replica open failed");
			goto err;
		}
	}

	/* check headers, check UUID's, check replicas linkage */
	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			if (util_header_check(set, r, p,  sig, major,
					compat, incompat, ro_compat) != 0) {
				LOG(2, "header check failed - part #%d", p);
				goto err;
			}

			set->rdonly |= rep->part[p].rdonly;
		}

		if (memcmp(HDR(REP(set, r - 1), 0)->uuid,
					HDR(REP(set, r), 0)->prev_repl_uuid,
					POOL_HDR_UUID_LEN) ||
		    memcmp(HDR(REP(set, r + 1), 0)->uuid,
					HDR(REP(set, r), 0)->next_repl_uuid,
					POOL_HDR_UUID_LEN)) {
			ERR("wrong replica UUID");
			errno = EINVAL;
			goto err;
		}
	}

	/* unmap all headers */
	for (unsigned r = 0; r < set->nreplicas; r++) {
		struct pool_replica *rep = set->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++)
			util_unmap_hdr(&rep->part[p]);
	}

	return 0;

err:
	LOG(4, "error clean up");
	int oerrno = errno;
	for (unsigned r = 0; r < set->nreplicas; r++)
		util_replica_close(set, r);

	util_poolset_close(set, 0);
	errno = oerrno;
	return -1;
}
