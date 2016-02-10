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
 * pmemobjfs.c -- simple filesystem based on libpmemobj tx API
 */
#include <stdio.h>
#include <fuse.h>
#include <stdlib.h>
#include <libpmemobj.h>
#include <libpmem.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <libgen.h>
#include <err.h>
#include <sys/ioctl.h>
#include <linux/kdev_t.h>

#include <map.h>
#include <map_ctree.h>

#ifndef	PMEMOBJFS_TRACK_BLOCKS
#define	PMEMOBJFS_TRACK_BLOCKS	1
#endif

#if DEBUG
static FILE *log_fh;
static uint64_t log_cnt;
#define	log(fmt, args...) do {\
if (log_fh) {\
	fprintf(log_fh, "[%016lx] %s: " fmt "\n", log_cnt, __func__, ## args);\
	log_cnt++;\
	fflush(log_fh);\
}\
} while (0)
#else
#define	log(fmt, args...) do {} while (0)
#endif

#define	PMEMOBJFS_MOUNT "pmemobjfs"
#define	PMEMOBJFS_MKFS	"mkfs.pmemobjfs"
#define	PMEMOBJFS_TX_BEGIN "pmemobjfs.tx_begin"
#define	PMEMOBJFS_TX_COMMIT "pmemobjfs.tx_commit"
#define	PMEMOBJFS_TX_ABORT  "pmemobjfs.tx_abort"

#define	PMEMOBJFS_TMP_TEMPLATE	"/.tx_XXXXXX"

#define	PMEMOBJFS_CTL		'I'
#define	PMEMOBJFS_CTL_TX_BEGIN	_IO(PMEMOBJFS_CTL, 1)
#define	PMEMOBJFS_CTL_TX_COMMIT	_IO(PMEMOBJFS_CTL, 2)
#define	PMEMOBJFS_CTL_TX_ABORT	_IO(PMEMOBJFS_CTL, 3)

/*
 * struct pmemobjfs -- volatile state of pmemobjfs
 */
struct pmemobjfs {
	PMEMobjpool *pop;
	struct map_ctx *mapc;
	uint64_t pool_uuid_lo;
	int ioctl_cmd;
	uint64_t ioctl_off;
	uint64_t block_size;
	uint64_t max_name;
};

#define	PMEMOBJFS (struct pmemobjfs *)fuse_get_context()->private_data

#define	PDLL_ENTRY(type)\
struct {\
	TOID(type) next;\
	TOID(type) prev;\
}

#define	PDLL_HEAD(type)\
struct {\
	TOID(type) first;\
	TOID(type) last;\
}

#define	PDLL_HEAD_INIT(head) do {\
	((head)).first = ((typeof ((head).first))OID_NULL);\
	((head)).last = ((typeof ((head).first))OID_NULL);\
} while (0)

#define	PDLL_FOREACH(entry, head, field)\
for (entry = ((head)).first; !TOID_IS_NULL(entry);\
		entry = D_RO(entry)->field.next)

#define	PDLL_FOREACH_SAFE(entry, next, head, field)\
for (entry = ((head)).first; !TOID_IS_NULL(entry) &&\
		(next = D_RO(entry)->field.next, 1);\
		entry = next)

#define	PDLL_INSERT_HEAD(head, entry, field) do {\
	pmemobj_tx_add_range_direct(&head.first, sizeof (head.first));\
	TX_ADD_FIELD(entry, field);\
	D_RW(entry)->field.next = head.first;\
	D_RW(entry)->field.prev =\
		(typeof (D_RW(entry)->field.prev))OID_NULL;\
	head.first = entry;\
	if (TOID_IS_NULL(head.last)) {\
		pmemobj_tx_add_range_direct(&head.last, sizeof (head.last));\
		head.last = entry;\
	}\
	typeof (entry) next = D_RO(entry)->field.next;\
	if (!TOID_IS_NULL(next)) {\
		pmemobj_tx_add_range_direct(&D_RW(next)->field.prev,\
				sizeof (D_RW(next)->field.prev));\
		D_RW(next)->field.prev = entry;\
	}\
} while (0)

#define	PDLL_REMOVE(head, entry, field) do {\
	if (TOID_EQUALS(head.first, entry) &&\
		TOID_EQUALS(head.last, entry)) {\
		pmemobj_tx_add_range_direct(&head.first, sizeof (head.first));\
		pmemobj_tx_add_range_direct(&head.last, sizeof (head.last));\
		head.first = (typeof (D_RW(entry)->field.prev))OID_NULL;\
		head.last = (typeof (D_RW(entry)->field.prev))OID_NULL;\
	} else if (TOID_EQUALS(head.first, entry)) {\
		typeof (entry) next = D_RW(entry)->field.next;\
		pmemobj_tx_add_range_direct(&D_RW(next)->field.prev,\
				sizeof (D_RW(next)->field.prev));\
		pmemobj_tx_add_range_direct(&head.first, sizeof (head.first));\
		head.first = D_RO(entry)->field.next;\
		D_RW(next)->field.prev.oid = OID_NULL;\
	} else if (TOID_EQUALS(head.last, entry)) {\
		typeof (entry) prev = D_RW(entry)->field.prev;\
		pmemobj_tx_add_range_direct(&D_RW(prev)->field.next,\
				sizeof (D_RW(prev)->field.next));\
		pmemobj_tx_add_range_direct(&head.last, sizeof (head.last));\
		head.last = D_RO(entry)->field.prev;\
		D_RW(prev)->field.next.oid = OID_NULL;\
	} else {\
		typeof (entry) prev = D_RW(entry)->field.prev;\
		typeof (entry) next = D_RW(entry)->field.next;\
		pmemobj_tx_add_range_direct(&D_RW(prev)->field.next,\
				sizeof (D_RW(prev)->field.next));\
		pmemobj_tx_add_range_direct(&D_RW(next)->field.prev,\
				sizeof (D_RW(next)->field.prev));\
		D_RW(prev)->field.next = D_RO(entry)->field.next;\
		D_RW(next)->field.prev = D_RO(entry)->field.prev;\
	}\
} while (0)

typedef uint8_t objfs_block_t;

/*
 * pmemobjfs persistent layout
 */
POBJ_LAYOUT_BEGIN(pmemobjfs);
POBJ_LAYOUT_ROOT(pmemobjfs, struct objfs_super);
POBJ_LAYOUT_TOID(pmemobjfs, struct objfs_inode);
POBJ_LAYOUT_TOID(pmemobjfs, struct objfs_dir_entry);
POBJ_LAYOUT_TOID(pmemobjfs, objfs_block_t);
POBJ_LAYOUT_TOID(pmemobjfs, char);
POBJ_LAYOUT_END(pmemobjfs);

#define	PMEMOBJFS_MIN_BLOCK_SIZE ((size_t)(512 - 64))

/*
 * struct objfs_super -- pmemobjfs super (root) object
 */
struct objfs_super {
	TOID(struct objfs_inode) root_inode;	/* root dir inode */
	TOID(struct map) opened;		/* map of opened files / dirs */
	uint64_t block_size;			/* size of data block */
};

/*
 * struct objfs_dir_entry -- pmemobjfs directory entry structure
 */
struct objfs_dir_entry {
	PDLL_ENTRY(struct objfs_dir_entry) pdll; /* list entry */
	TOID(struct objfs_inode) inode;	/* pointer to inode */
	char name[];			/* name */
};

/*
 * struct objfs_dir -- pmemobjfs directory structure
 */
struct objfs_dir {
	PDLL_HEAD(struct objfs_dir_entry) entries; /* directory entries */
};

/*
 * key == 0 for ctree_map is not allowed
 */
#define	GET_KEY(off) ((off) + 1)

/*
 * struct objfs_file -- pmemobjfs file structure
 */
struct objfs_file {
	TOID(struct map) blocks;	/* blocks map */
};

/*
 * struct objfs_symlink -- symbolic link
 */
struct objfs_symlink {
	uint64_t len;	/* length of symbolic link */
	TOID(char) name; /* symbolic link data */
};

/*
 * struct objfs_inode -- pmemobjfs inode structure
 */
struct objfs_inode {
	uint64_t size;	/* size of file */
	uint64_t flags;	/* file flags */
	uint64_t dev;	/* device info */
	uint32_t ctime;	/* time of last status change */
	uint32_t mtime; /* time of last modification */
	uint32_t atime; /* time of last access */
	uint32_t uid;	/* user ID */
	uint32_t gid;	/* group ID */
	uint32_t ref;	/* reference counter */
	struct objfs_file file; /* file specific data */
	struct objfs_dir dir;	/* directory specific data */
	struct objfs_symlink symlink; /* symlink specific data */
};

/*
 * pmemobjfs_ioctl -- do the ioctl command
 */
static void
pmemobjfs_ioctl(struct pmemobjfs *objfs)
{
	switch (objfs->ioctl_cmd) {
	case PMEMOBJFS_CTL_TX_BEGIN:
		pmemobj_tx_begin(objfs->pop, NULL, TX_LOCK_NONE);
		break;
	case PMEMOBJFS_CTL_TX_ABORT:
		pmemobj_tx_abort(-1);
		pmemobj_tx_end();
		break;
	case PMEMOBJFS_CTL_TX_COMMIT:
		pmemobj_tx_commit();
		pmemobj_tx_end();
		break;
	default:
		break;
	}

	/* clear deferred inode offset and command */
	objfs->ioctl_cmd = 0;
	objfs->ioctl_off = 0;
}

/*
 * pmemobjfs_inode_alloc -- allocate inode structure
 */
static TOID(struct objfs_inode)
pmemobjfs_inode_alloc(struct pmemobjfs *objfs, uint64_t flags,
		uint32_t uid, uint32_t gid, uint64_t dev)
{
	TOID(struct objfs_inode) inode = TOID_NULL(struct objfs_inode);
	TX_BEGIN(objfs->pop) {
		inode = TX_ZNEW(struct objfs_inode);
		time_t cur_time = time(NULL);

		D_RW(inode)->flags = flags;
		D_RW(inode)->dev = dev;
		D_RW(inode)->ctime = cur_time;
		D_RW(inode)->mtime = cur_time;
		D_RW(inode)->atime = cur_time;
		D_RW(inode)->uid = uid;
		D_RW(inode)->gid = gid;
		D_RW(inode)->ref = 0;
	} TX_ONABORT {
		inode = TOID_NULL(struct objfs_inode);
	} TX_END

	return inode;
}

/*
 * pmemobjfs_inode_init_dir -- initialize directory in inode
 */
static void
pmemobjfs_inode_init_dir(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode)
{
	TX_BEGIN(objfs->pop) {
		PDLL_HEAD_INIT(D_RW(inode)->dir.entries);
	} TX_END;
}

/*
 * pmemobjfs_inode_destroy_dir -- destroy directory from inode
 */
static void
pmemobjfs_inode_destroy_dir(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode)
{
	/* nothing to do */
}

/*
 * pmemobjfs_file_alloc -- allocate file structure
 */
static void
pmemobjfs_inode_init_file(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode)
{
	TX_BEGIN(objfs->pop) {
		map_new(objfs->mapc, &D_RW(inode)->file.blocks, NULL);
	} TX_END
}

/*
 * pmemobjfs_file_free -- free file structure
 */
static void
pmemobjfs_inode_destroy_file(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode)
{
	TX_BEGIN(objfs->pop) {
		map_delete(objfs->mapc, &D_RW(inode)->file.blocks);
	} TX_END
}

/*
 * pmemobjfs_inode_hold -- increase reference counter of inode
 */
static void
pmemobjfs_inode_hold(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode)
{
	if (TOID_IS_NULL(inode))
		return;

	TX_BEGIN(objfs->pop) {
		/* update number of references */
		TX_ADD_FIELD(inode, ref);
		D_RW(inode)->ref++;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = time(NULL);
	} TX_END
}

/*
 * pmemobjfs_dir_entry_alloc -- allocate directory entry structure
 */
static TOID(struct objfs_dir_entry)
pmemobjfs_dir_entry_alloc(struct pmemobjfs *objfs, const char *name,
		TOID(struct objfs_inode) inode)
{
	TOID(struct objfs_dir_entry) entry = TOID_NULL(struct objfs_dir_entry);
	TX_BEGIN(objfs->pop) {
		size_t len = strlen(name) + 1;
		entry = TX_ALLOC(struct objfs_dir_entry, objfs->block_size);

		memcpy(D_RW(entry)->name, name, len);

		D_RW(entry)->inode = inode;
		pmemobjfs_inode_hold(objfs, inode);
	} TX_ONABORT {
		entry = TOID_NULL(struct objfs_dir_entry);
	} TX_END

	return entry;
}

/*
 * pmemobjfs_dir_entry_free -- free dir entry structure
 */
static void
pmemobjfs_dir_entry_free(struct pmemobjfs *objfs,
		TOID(struct objfs_dir_entry) entry)
{
	TX_BEGIN(objfs->pop) {
		TX_FREE(entry);
	} TX_END
}

/*
 * pmemobjfs_inode_init_symlink -- initialize symbolic link
 */
static void
pmemobjfs_inode_init_symlink(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		const char *name)
{
	TX_BEGIN(objfs->pop) {
		size_t len = strlen(name) + 1;
		D_RW(inode)->symlink.len = len;
		TOID_ASSIGN(D_RW(inode)->symlink.name,
			TX_STRDUP(name, TOID_TYPE_NUM(char)));
	} TX_END
}

/*
 * pmemobjfs_inode_destroy_symlink -- destroy symbolic link
 */
static void
pmemobjfs_inode_destroy_symlink(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode)
{
	TX_BEGIN(objfs->pop) {
		TX_FREE(D_RO(inode)->symlink.name);
	} TX_END
}

/*
 * pmemobjfs_symlink_read -- read symlink to buffer
 */
static int
pmemobjfs_symlink_read(TOID(struct objfs_inode) inode,
		char *buff, size_t size)
{
	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFLNK:
		break;
	case S_IFDIR:
		return -EISDIR;
	case S_IFREG:
	default:
		return -EINVAL;
	}

	char *name = D_RW(D_RW(inode)->symlink.name);
	strncpy(buff, name, size);

	return 0;
}

/*
 * pmemobjfs_symlink_size -- get size of symlink
 */
static size_t
pmemobjfs_symlink_size(TOID(struct objfs_inode) inode)
{
	return D_RO(inode)->symlink.len - 1;
}

/*
 * pmemobjfs_inode_free -- free inode structure
 */
static void
pmemobjfs_inode_free(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode)
{
	TX_BEGIN(objfs->pop) {
		/* release data specific for inode type */
		if (S_ISREG(D_RO(inode)->flags)) {
			pmemobjfs_inode_destroy_file(objfs, inode);
		} else if (S_ISDIR(D_RO(inode)->flags)) {
			pmemobjfs_inode_destroy_dir(objfs, inode);
		} else if (S_ISLNK(D_RO(inode)->flags)) {
			pmemobjfs_inode_destroy_symlink(objfs, inode);
		}

		TX_FREE(inode);
	} TX_END
}

/*
 * pmemobjfs_inode_put -- decrease reference counter of inode and free
 */
static void
pmemobjfs_inode_put(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode)
{
	if (TOID_IS_NULL(inode))
		return;

	TX_BEGIN(objfs->pop) {
		/* update number of references */
		TX_ADD_FIELD(inode, ref);
		D_RW(inode)->ref--;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = time(NULL);

		if (!D_RO(inode)->ref)
			pmemobjfs_inode_free(objfs, inode);
	} TX_END
}

/*
 * pmemobjfs_dir_get_inode -- get inode from dir of given name
 */
static TOID(struct objfs_inode)
pmemobjfs_dir_get_inode(TOID(struct objfs_inode) inode, const char *name)
{
	log("%s", name);
	TOID(struct objfs_dir_entry) entry;
	PDLL_FOREACH(entry, D_RW(inode)->dir.entries, pdll) {
		if (strcmp(name, D_RO(entry)->name) == 0)
			return D_RO(entry)->inode;
	}

	return TOID_NULL(struct objfs_inode);
}

/*
 * pmemobjfs_get_dir_entry -- get dir entry from dir of given name
 */
static TOID(struct objfs_dir_entry)
pmemobjfs_get_dir_entry(TOID(struct objfs_inode) inode, const char *name)
{
	log("%s", name);
	TOID(struct objfs_dir_entry) entry;
	PDLL_FOREACH(entry, D_RW(inode)->dir.entries, pdll) {
		if (strcmp(name, D_RO(entry)->name) == 0)
			return entry;
	}

	return TOID_NULL(struct objfs_dir_entry);
}

/*
 * pmemobjfs_inode_lookup_parent -- lookup for parent inode and child name
 */
static int
pmemobjfs_inode_lookup_parent(struct pmemobjfs *objfs,
		const char *path, TOID(struct objfs_inode) *inodep,
		const char **child)
{
	log("%s", path);
	TOID(struct objfs_super) super =
		POBJ_ROOT(objfs->pop, struct objfs_super);
	TOID(struct objfs_inode) cur = D_RO(super)->root_inode;
	TOID(struct objfs_inode) par = TOID_NULL(struct objfs_inode);

	if (path[0] == '/')
		path++;

	int ret = 0;
	char *p = strdup(path);
	char *name = p;
	char *ch = NULL;
	while (name && *name != '\0' && !TOID_IS_NULL(cur)) {
		char *slash = strchr(name, '/');
		if (slash) {
			*slash = '\0';
			slash++;
		}

		if (!S_ISDIR(D_RO(cur)->flags)) {
			ret = -ENOTDIR;
			goto out;
		}

		if (strlen(name) > objfs->max_name) {
			ret = -ENAMETOOLONG;
			goto out;
		}

		par = cur;
		cur = pmemobjfs_dir_get_inode(cur, name);
		ch = name;
		name = slash;
	}

	if (child) {
		if (strchr(ch, '/')) {
			ret = -ENOENT;
			goto out;
		}

		if (TOID_IS_NULL(par)) {
			ret = -ENOENT;
			goto out;
		}

		cur = par;
		size_t parent_len = ch - p;
		*child = path + parent_len;
	} else {
		if (TOID_IS_NULL(cur))
			ret = -ENOENT;
	}

	if (inodep)
		*inodep = cur;
out:
	free(p);

	return ret;
}

/*
 * pmemobjfs_inode_lookup -- get inode for given path
 */
static int
pmemobjfs_inode_lookup(struct pmemobjfs *objfs, const char *path,
		TOID(struct objfs_inode) *inodep)
{
	log("%s", path);
	return pmemobjfs_inode_lookup_parent(objfs, path, inodep, NULL);
}

/*
 * pmemobjfs_file_get_block -- get block at given offset
 */
static TOID(objfs_block_t)
pmemobjfs_file_get_block(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		uint64_t offset)
{
	TOID(objfs_block_t) block;
	PMEMoid block_oid =
		map_get(objfs->mapc, D_RO(inode)->file.blocks, GET_KEY(offset));
	TOID_ASSIGN(block, block_oid);
	return block;
}

/*
 * pmemobjfs_file_get_block_for_write -- get or allocate block at given offset
 */
static TOID(objfs_block_t)
pmemobjfs_file_get_block_for_write(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode, uint64_t offset)
{
	TOID(objfs_block_t) block =
		pmemobjfs_file_get_block(objfs, inode, offset);
	if (TOID_IS_NULL(block)) {
		TX_BEGIN(objfs->pop) {
			block = TX_ALLOC(objfs_block_t,
					objfs->block_size);
			map_insert(objfs->mapc, D_RW(inode)->file.blocks,
					GET_KEY(offset), block.oid);
		} TX_ONABORT {
			block = TOID_NULL(objfs_block_t);
		} TX_END
	} else {
#if PMEMOBJFS_TRACK_BLOCKS
		TX_ADD(block);
#endif
	}

	return block;
}

/*
 * pmemobjfs_truncate -- truncate file
 */
static int
pmemobjfs_truncate(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode, off_t off)
{
	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFREG:
		break;
	case S_IFDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		uint64_t old_off = D_RO(inode)->size;
		if (old_off > off) {
			/* release blocks */
			uint64_t old_boff = (old_off - 1) / objfs->block_size;
			uint64_t boff = (off + 1) / objfs->block_size;

			for (uint64_t o = boff; o <= old_boff; o++) {
				map_remove_free(objfs->mapc,
					D_RW(inode)->file.blocks, GET_KEY(o));
			}
		}

		time_t t = time(NULL);
		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = t;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = t;

		/* update size */
		TX_ADD_FIELD(inode, size);
		D_RW(inode)->size = off;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_read -- read from file
 */
static int
pmemobjfs_read(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		char *buff, size_t size, off_t offset)
{
	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFREG:
		break;
	case S_IFDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}

	uint64_t fsize = D_RO(inode)->size;

	size_t sz = size;
	size_t off = offset;
	while (sz > 0) {
		if (off >= fsize)
			break;

		uint64_t block_id = off / objfs->block_size;
		uint64_t block_off = off % objfs->block_size;
		uint64_t block_size = sz < objfs->block_size ?
			sz : objfs->block_size;

		TOID(objfs_block_t) block =
			pmemobjfs_file_get_block(objfs, inode, block_id);

		if (block_off + block_size > objfs->block_size)
			block_size = objfs->block_size - block_off;

		if (TOID_IS_NULL(block)) {
			memset(buff, 0, block_size);
		} else {
			memcpy(buff, &D_RW(block)[block_off], block_size);
		}


		buff += block_size;
		off += block_size;
		sz -= block_size;
	}

	return size - sz;
}

/*
 * pmemobjfs_write -- write to file
 */
static int
pmemobjfs_write(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		const char *buff, size_t size, off_t offset)
{
	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFREG:
		break;
	case S_IFDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		size_t sz = size;
		off_t off = offset;
		while (sz > 0) {
			uint64_t block_id = off / objfs->block_size;
			uint64_t block_off = off % objfs->block_size;
			uint64_t block_size = sz < objfs->block_size ?
				sz : objfs->block_size;

			TOID(objfs_block_t) block =
				pmemobjfs_file_get_block_for_write(objfs,
						inode, block_id);

			if (TOID_IS_NULL(block))
				return -ENOSPC;

			if (block_off + block_size > objfs->block_size)
				block_size = objfs->block_size - block_off;

			memcpy(&D_RW(block)[block_off], buff, block_size);

			buff += block_size;
			off += block_size;
			sz -= block_size;
		}

		time_t t = time(NULL);

		if (offset + size > D_RO(inode)->size) {
			/* update size */
			TX_ADD_FIELD(inode, size);
			D_RW(inode)->size = offset + size;

			/* update status change time */
			TX_ADD_FIELD(inode, ctime);
			D_RW(inode)->ctime = t;
		}

		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = t;
	} TX_ONCOMMIT {
		ret = size;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_fallocate -- allocate blocks for file
 */
static int
pmemobjfs_fallocate(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode, off_t offset, off_t size)
{
	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFREG:
		break;
	case S_IFDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		/* allocate blocks from requested range */
		uint64_t b_off = offset / objfs->block_size;
		uint64_t e_off = (offset + size) / objfs->block_size;
		for (uint64_t off = b_off; off <= e_off; off++)
			pmemobjfs_file_get_block_for_write(objfs, inode, off);

		time_t t = time(NULL);
		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = t;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = t;

		/* update inode size */
		D_RW(inode)->size = offset + size;
		TX_ADD_FIELD(inode, size);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_remove_dir_entry -- remove dir entry from directory
 */
static void
pmemobjfs_remove_dir_entry(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		TOID(struct objfs_dir_entry) entry)
{
	TX_BEGIN(objfs->pop) {
		pmemobjfs_inode_put(objfs, D_RO(entry)->inode);

		PDLL_REMOVE(D_RW(inode)->dir.entries, entry, pdll);

		pmemobjfs_dir_entry_free(objfs, entry);
	} TX_END
}

/*
 * pmemobjfs_remove_dir_entry_name -- remove dir entry of given name
 */
static void
pmemobjfs_remove_dir_entry_name(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode, const char *name)
{
	TX_BEGIN(objfs->pop) {
		TOID(struct objfs_dir_entry) entry =
			pmemobjfs_get_dir_entry(inode, name);

		pmemobjfs_remove_dir_entry(objfs, inode, entry);
	} TX_END
}


/*
 * pmemobjfs_add_dir_entry -- add new directory entry
 */
static int
pmemobjfs_add_dir_entry(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		TOID(struct objfs_dir_entry) entry)
{
	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	int ret = 0;
	TX_BEGIN(objfs->pop) {
		/* insert new dir entry to list */
		PDLL_INSERT_HEAD(D_RW(inode)->dir.entries, entry, pdll);

		/* update dir size */
		TX_ADD_FIELD(inode, size);
		D_RW(inode)->size++;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_unlink_dir_entry -- unlink directory entry
 */
static int
pmemobjfs_unlink_dir_entry(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		TOID(struct objfs_dir_entry) entry)
{
	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		pmemobjfs_remove_dir_entry(objfs, inode, entry);

		/* update dir size */
		TX_ADD_FIELD(inode, size);
		D_RW(inode)->size--;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_new_dir -- create new directory
 */
static TOID(struct objfs_inode)
pmemobjfs_new_dir(struct pmemobjfs *objfs, TOID(struct objfs_inode) parent,
		const char *name, uint64_t flags, uint32_t uid, uint32_t gid)
{
	TOID(struct objfs_inode) inode = TOID_NULL(struct objfs_inode);

	TX_BEGIN(objfs->pop) {
		inode = pmemobjfs_inode_alloc(objfs, flags, uid, gid, 0);

		pmemobjfs_inode_init_dir(objfs, inode);

		/* add . and .. to new directory */
		TOID(struct objfs_dir_entry) dot =
			pmemobjfs_dir_entry_alloc(objfs, ".", inode);
		TOID(struct objfs_dir_entry) dotdot =
			pmemobjfs_dir_entry_alloc(objfs, "..", parent);

		pmemobjfs_add_dir_entry(objfs, inode, dot);
		pmemobjfs_add_dir_entry(objfs, inode, dotdot);
	} TX_ONABORT {
		inode = TOID_NULL(struct objfs_inode);
	} TX_END

	return inode;
}

/*
 * pmemobjfs_mkdir -- make new directory
 */
static int
pmemobjfs_mkdir(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		const char *name, uint64_t flags, uint32_t uid, uint32_t gid)
{
	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	int ret = 0;
	TX_BEGIN(objfs->pop) {
		TOID(struct objfs_inode) new_inode =
			pmemobjfs_new_dir(objfs, inode, name, flags, uid, gid);

		TOID(struct objfs_dir_entry) entry =
			pmemobjfs_dir_entry_alloc(objfs, name, new_inode);

		pmemobjfs_add_dir_entry(objfs, inode, entry);

		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = time(NULL);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}


/*
 * pmemobjfs_remove_dir -- remove directory from directory
 */
static void
pmemobjfs_remove_dir(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		TOID(struct objfs_dir_entry) entry)
{
	/* removing entry inode */
	TOID(struct objfs_inode) rinode = D_RO(entry)->inode;
	TX_BEGIN(objfs->pop) {
		/* remove . and .. from removing dir */
		pmemobjfs_remove_dir_entry_name(objfs, rinode, ".");
		pmemobjfs_remove_dir_entry_name(objfs, rinode, "..");
		/* remove dir entry from parent */
		pmemobjfs_remove_dir_entry(objfs, inode, entry);
	} TX_END
}

/*
 * pmemobjfs_rmdir -- remove directory of given name
 */
static int
pmemobjfs_rmdir(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		const char *name)
{
	/* check parent inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	TOID(struct objfs_dir_entry) entry =
		pmemobjfs_get_dir_entry(inode, name);

	if (TOID_IS_NULL(entry))
		return -ENOENT;

	TOID(struct objfs_inode) entry_inode =
		D_RO(entry)->inode;

	/* check removing dir type */
	if (!S_ISDIR(D_RO(entry_inode)->flags))
		return -ENOTDIR;

	/* check if dir is empty (contains only . and ..) */
	if (D_RO(entry_inode)->size > 2)
		return -ENOTEMPTY;

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		pmemobjfs_remove_dir(objfs, inode, entry);

		/* update dir size */
		TX_ADD_FIELD(inode, size);
		D_RW(inode)->size--;

		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = time(NULL);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_create -- create new file in directory
 */
static int
pmemobjfs_create(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		const char *name, mode_t mode, uid_t uid, gid_t gid,
		TOID(struct objfs_inode) *inodep)
{
	int ret = 0;

	uint64_t flags = mode | S_IFREG;

	TOID(struct objfs_dir_entry) entry = TOID_NULL(struct objfs_dir_entry);
	TX_BEGIN(objfs->pop) {
		TOID(struct objfs_inode) new_file=
			pmemobjfs_inode_alloc(objfs, flags, uid, gid, 0);
		pmemobjfs_inode_init_file(objfs, new_file);

		entry = pmemobjfs_dir_entry_alloc(objfs, name, new_file);
		pmemobjfs_add_dir_entry(objfs, inode, entry);

		time_t t = time(NULL);
		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = t;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = t;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_ONCOMMIT {
		if (inodep)
			*inodep = D_RO(entry)->inode;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_open -- open inode
 */
static int
pmemobjfs_open(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode)
{
	TOID(struct objfs_super) super =
		POBJ_ROOT(objfs->pop, struct objfs_super);

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		/* insert inode to opened inodes map */
		map_insert(objfs->mapc, D_RW(super)->opened,
				inode.oid.off, inode.oid);
		/* hold inode */
		pmemobjfs_inode_hold(objfs, inode);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_close -- release inode
 */
static int
pmemobjfs_close(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode)
{
	TOID(struct objfs_super) super =
		POBJ_ROOT(objfs->pop, struct objfs_super);

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		/* remove inode from opened inodes map */
		map_remove(objfs->mapc, D_RW(super)->opened,
				inode.oid.off);
		/* release inode */
		pmemobjfs_inode_put(objfs, inode);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_rename -- rename/move inode
 */
static int
pmemobjfs_rename(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) src_parent,
		const char *src_name,
		TOID(struct objfs_inode) dst_parent,
		const char *dst_name)
{
	/* check source and destination inodes type */
	if (!S_ISDIR(D_RO(src_parent)->flags))
		return -ENOTDIR;

	if (!S_ISDIR(D_RO(dst_parent)->flags))
		return -ENOTDIR;

	/* get source dir entry */
	TOID(struct objfs_dir_entry) src_entry =
		pmemobjfs_get_dir_entry(src_parent, src_name);

	TOID(struct objfs_inode) src_inode = D_RO(src_entry)->inode;

	if (TOID_IS_NULL(src_entry))
		return -ENOENT;

	int ret = 0;
	TX_BEGIN(objfs->pop) {
		/*
		 * Allocate new dir entry with destination name
		 * and source inode.
		 * NOTE:
		 * This *must* be called before removing dir entry from
		 * source directory because otherwise the source inode
		 * could be released before inserting to new dir entry.
		 */
		TOID(struct objfs_dir_entry) dst_entry =
			pmemobjfs_dir_entry_alloc(objfs, dst_name, src_inode);

		/* remove old dir entry from source */
		pmemobjfs_unlink_dir_entry(objfs, src_parent, src_entry);
		/* add new dir entry to destination */
		pmemobjfs_add_dir_entry(objfs, dst_parent, dst_entry);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_symlink -- create symbolic link
 */
static int
pmemobjfs_symlink(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode,
		const char *name, const char *path,
		uid_t uid, gid_t gid)
{
	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	/* set 0777 permissions for symbolic links */
	uint64_t flags = 0777 | S_IFLNK;

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		TOID(struct objfs_inode) symlink =
			pmemobjfs_inode_alloc(objfs, flags, uid, gid, 0);

		pmemobjfs_inode_init_symlink(objfs, symlink, path);

		D_RW(symlink)->size = pmemobjfs_symlink_size(symlink);

		TOID(struct objfs_dir_entry) entry =
			pmemobjfs_dir_entry_alloc(objfs, name, symlink);

		pmemobjfs_add_dir_entry(objfs, inode, entry);

		time_t t = time(NULL);
		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = t;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = t;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_mknod -- create node
 */
static int
pmemobjfs_mknod(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		const char *name, mode_t mode, uid_t uid, gid_t gid, dev_t dev)
{
	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		TOID(struct objfs_inode) node =
			pmemobjfs_inode_alloc(objfs, mode, uid, gid, dev);
		D_RW(node)->size = 0;

		TOID(struct objfs_dir_entry) entry =
			pmemobjfs_dir_entry_alloc(objfs, name, node);

		pmemobjfs_add_dir_entry(objfs, inode, entry);

		time_t t = time(NULL);
		/* update modification time */
		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = t;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = t;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_chmod -- change mode of inode
 */
static int
pmemobjfs_chmod(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		mode_t mode)
{
	int ret = 0;

	TX_BEGIN(objfs->pop) {
		TX_ADD_FIELD(inode, flags);

		/* mask file type bit fields */
		uint64_t flags = D_RO(inode)->flags;
		flags = flags & S_IFMT;
		D_RW(inode)->flags = flags | (mode & ~S_IFMT);

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = time(NULL);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_chown -- change owner and group of inode
 */
static int
pmemobjfs_chown(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		uid_t uid, gid_t gid)
{
	int ret = 0;

	TX_BEGIN(objfs->pop) {
		TX_ADD_FIELD(inode, uid);
		D_RW(inode)->uid = uid;

		TX_ADD_FIELD(inode, gid);
		D_RW(inode)->gid = gid;

		/* update status change time */
		TX_ADD_FIELD(inode, ctime);
		D_RW(inode)->ctime = time(NULL);
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_getattr -- get inode's attributes
 */
static int
pmemobjfs_getattr(TOID(struct objfs_inode) inode, struct stat *statbuf)
{
	memset(statbuf, 0, sizeof (*statbuf));

	statbuf->st_size = D_RO(inode)->size;
	statbuf->st_ctime = D_RO(inode)->ctime;
	statbuf->st_mtime = D_RO(inode)->mtime;
	statbuf->st_atime = D_RO(inode)->atime;
	statbuf->st_mode = D_RO(inode)->flags;
	statbuf->st_uid = D_RO(inode)->uid;
	statbuf->st_gid = D_RO(inode)->gid;
	statbuf->st_rdev = D_RO(inode)->dev;

	return 0;
}

/*
 * pmemobjfs_utimens -- set atime and mtime
 */
static int
pmemobjfs_utimens(struct pmemobjfs *objfs,
		TOID(struct objfs_inode) inode, const struct timespec tv[2])
{
	int ret = 0;

	TX_BEGIN(objfs->pop) {
		TX_ADD_FIELD(inode, atime);
		D_RW(inode)->atime = tv[0].tv_sec;

		TX_ADD_FIELD(inode, mtime);
		D_RW(inode)->mtime = tv[0].tv_sec;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_unlink -- unlink file from inode
 */
static int
pmemobjfs_unlink(struct pmemobjfs *objfs, TOID(struct objfs_inode) inode,
		const char *name)
{
	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	TOID(struct objfs_dir_entry) entry =
		pmemobjfs_get_dir_entry(inode, name);

	if (TOID_IS_NULL(entry))
		return -ENOENT;

	TOID(struct objfs_inode) entry_inode = D_RO(entry)->inode;

	/* check unlinking inode type */
	if (S_ISDIR(D_RO(entry_inode)->flags))
		return -EISDIR;

	int ret = 0;

	TX_BEGIN(objfs->pop) {
		pmemobjfs_remove_dir_entry(objfs, inode, entry);

		TX_ADD_FIELD(inode, size);
		D_RW(inode)->size--;
	} TX_ONABORT {
		ret = -ECANCELED;
	} TX_END

	return ret;
}

/*
 * pmemobjfs_put_opened_cb -- release all opened inodes
 */
static int
pmemobjfs_put_opened_cb(uint64_t key, PMEMoid value, void *arg)
{
	struct pmemobjfs *objfs = arg;

	TOID(struct objfs_inode) inode;
	TOID_ASSIGN(inode, value);

	TOID(struct objfs_super) super =
		POBJ_ROOT(objfs->pop, struct objfs_super);

	/*
	 * Set current value to OID_NULL so the tree_map_clear won't
	 * free this inode and release the inode.
	 */
	map_insert(objfs->mapc, D_RW(super)->opened,
			key, OID_NULL);
	pmemobjfs_inode_put(objfs, inode);

	return 0;
}

/*
 * pmemobjfs_fuse_getattr -- (FUSE) get file attributes
 */
static int
pmemobjfs_fuse_getattr(const char *path, struct stat *statbuf)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	return pmemobjfs_getattr(inode, statbuf);
}

/*
 * pmemobjfs_fuse_opendir -- (FUSE) open directory
 */
static int
pmemobjfs_fuse_opendir(const char *path, struct fuse_file_info *fi)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFDIR:
		break;
	case S_IFREG:
		return -ENOTDIR;
	default:
		return -EINVAL;
	}

	/* add inode to opened inodes map */
	ret = pmemobjfs_open(objfs, inode);
	if (!ret)
		fi->fh = inode.oid.off;

	return ret;
}

/*
 * pmemobjfs_fuse_releasedir -- (FUSE) release opened dir
 */
static int
pmemobjfs_fuse_releasedir(const char *path, struct fuse_file_info *fi)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	/* remove inode from opened inodes map */
	int ret = pmemobjfs_close(objfs, inode);

	fi->fh = 0;

	return ret;
}

/*
 * pmemobjfs_fuse_readdir -- (FUSE) read directory entries
 */
static int
pmemobjfs_fuse_readdir(const char *path, void *buff, fuse_fill_dir_t fill,
		off_t off, struct fuse_file_info *fi)
{
	log("%s off = %lu", path, off);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	if (!TOID_VALID(inode))
		return -EINVAL;

	/* check inode type */
	if (!S_ISDIR(D_RO(inode)->flags))
		return -ENOTDIR;

	/* walk through all dir entries and fill fuse buffer */
	int ret;
	TOID(struct objfs_dir_entry) entry;
	PDLL_FOREACH(entry, D_RW(inode)->dir.entries, pdll) {
		ret = fill(buff, D_RW(entry)->name, NULL, 0);
		if (ret)
			return ret;
	}

	return 0;
}

/*
 * pmemobjfs_fuse_mkdir -- (FUSE) create directory
 */
static int
pmemobjfs_fuse_mkdir(const char *path, mode_t mode)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	const char *name;
	int ret = pmemobjfs_inode_lookup_parent(objfs, path, &inode, &name);
	if (ret)
		return ret;

	uid_t uid = fuse_get_context()->uid;
	uid_t gid = fuse_get_context()->gid;

	return pmemobjfs_mkdir(objfs, inode, name, mode | S_IFDIR, uid, gid);
}

/*
 * pmemobjfs_fuse_rmdir -- (FUSE) remove directory
 */
static int
pmemobjfs_fuse_rmdir(const char *path)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	const char *name;
	int ret = pmemobjfs_inode_lookup_parent(objfs, path, &inode, &name);
	if (ret)
		return ret;

	return pmemobjfs_rmdir(objfs, inode, name);
}

/*
 * pmemobjfs_fuse_chmod -- (FUSE) change file permissions
 */
static int
pmemobjfs_fuse_chmod(const char *path, mode_t mode)
{
	log("%s 0%o", path, mode);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	return pmemobjfs_chmod(objfs, inode, mode);
}

/*
 * pmemobjfs_fuse_chown -- (FUSE) change owner
 */
static int
pmemobjfs_fuse_chown(const char *path, uid_t uid, gid_t gid)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	return pmemobjfs_chown(objfs, inode, uid, gid);
}

/*
 * pmemobjfs_fuse_create -- (FUSE) create file
 */
static int
pmemobjfs_fuse_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	log("%s mode %o", path, mode);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	const char *name;
	int ret = pmemobjfs_inode_lookup_parent(objfs, path, &inode, &name);
	if (ret)
		return ret;

	if (!S_ISDIR(D_RO(inode)->flags))
		return -EINVAL;

	uid_t uid = fuse_get_context()->uid;
	uid_t gid = fuse_get_context()->gid;

	TOID(struct objfs_inode) new_file;
	ret = pmemobjfs_create(objfs, inode, name, mode, uid, gid,
			&new_file);
	if (ret)
		return ret;

	/* add new inode to opened inodes */
	ret = pmemobjfs_open(objfs, new_file);
	if (ret)
		return ret;

	fi->fh = new_file.oid.off;

	return 0;
}

/*
 * pmemobjfs_fuse_utimens -- (FUSE) update access and modification times
 */
static int
pmemobjfs_fuse_utimens(const char *path, const struct timespec tv[2])
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	return pmemobjfs_utimens(objfs, inode, tv);
}

/*
 * pmemobjfs_fuse_open -- (FUSE) open file
 */
static int
pmemobjfs_fuse_open(const char *path, struct fuse_file_info *fi)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFREG:
		break;
	case S_IFDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}

	ret = pmemobjfs_open(objfs, inode);
	if (!ret)
		fi->fh = inode.oid.off;

	return ret;
}

/*
 * pmemobjfs_fuse_release -- (FUSE) release opened file
 */
static int
pmemobjfs_fuse_release(const char *path, struct fuse_file_info *fi)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	int ret = pmemobjfs_close(objfs, inode);

	/* perform deferred ioctl operation */
	if (!ret && objfs->ioctl_off && objfs->ioctl_off == fi->fh)
		pmemobjfs_ioctl(objfs);

	fi->fh = 0;

	return ret;
}

/*
 * pmemobjfs_fuse_write -- (FUSE) write to file
 */
static int
pmemobjfs_fuse_write(const char *path, const char *buff, size_t size,
		off_t offset, struct fuse_file_info *fi)
{
	log("%s size = %lu off = %lu", path, size, offset);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	if (!TOID_VALID(inode))
		return -EINVAL;

	return pmemobjfs_write(objfs, inode, buff, size, offset);
}

/*
 * pmemobjfs_fuse_read -- (FUSE) read from file
 */
static int
pmemobjfs_fuse_read(const char *path, char *buff, size_t size, off_t off,
		struct fuse_file_info *fi)
{
	log("%s size = %lu off = %lu", path, size, off);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	if (!TOID_VALID(inode))
		return -EINVAL;

	return pmemobjfs_read(objfs, inode, buff, size, off);
}

/*
 * pmemobjfs_fuse_truncate -- (FUSE) truncate file
 */
static int
pmemobjfs_fuse_truncate(const char *path, off_t off)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	return pmemobjfs_truncate(objfs, inode, off);
}

/*
 * pmemobjfs_fuse_ftruncate -- (FUSE) truncate file
 */
static int
pmemobjfs_fuse_ftruncate(const char *path, off_t off, struct fuse_file_info *fi)
{
	log("%s off = %lu", path, off);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	if (!TOID_VALID(inode))
		return -EINVAL;

	return pmemobjfs_truncate(objfs, inode, off);
}

/*
 * pmemobjfs_fuse_unlink -- (FUSE) unlink inode
 */
static int
pmemobjfs_fuse_unlink(const char *path)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	const char *name;
	int ret = pmemobjfs_inode_lookup_parent(objfs, path, &inode, &name);
	if (ret)
		return ret;

	return pmemobjfs_unlink(objfs, inode, name);
}

/*
 * pmemobjfs_fuse_flush -- (FUSE) flush file
 */
static int
pmemobjfs_fuse_flush(const char *path, struct fuse_file_info *fi)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	if (!TOID_VALID(inode))
		return -EINVAL;

	/* check inode type */
	switch (D_RO(inode)->flags & S_IFMT) {
	case S_IFREG:
		break;
	case S_IFDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}

	/* nothing to do */
	return 0;
}

/*
 * pmemobjfs_fuse_ioctl -- (FUSE) ioctl for file
 */
static int
pmemobjfs_fuse_ioctl(const char *path, int cmd, void *arg,
		struct fuse_file_info *fi, unsigned int flags, void *data)
{
	log("%s cmd %d", path, _IOC_NR(cmd));

	struct pmemobjfs *objfs = PMEMOBJFS;

	/* check transaction stage */
	switch (cmd) {
	case PMEMOBJFS_CTL_TX_BEGIN:
		if (pmemobj_tx_stage() != TX_STAGE_NONE)
			return -EINPROGRESS;
		break;
	case PMEMOBJFS_CTL_TX_ABORT:
		if (pmemobj_tx_stage() != TX_STAGE_WORK)
			return -EBADFD;
		break;
	case PMEMOBJFS_CTL_TX_COMMIT:
		if (pmemobj_tx_stage() != TX_STAGE_WORK)
			return -EBADFD;
		break;
	default:
		return -EINVAL;
	}

	/*
	 * Store the inode offset and command and defer ioctl
	 * execution to releasing the file. This is required
	 * to avoid unlinking .tx_XXXXXX file inside transaction
	 * because one would be rolled back if transaction abort
	 * would occur.
	 */
	objfs->ioctl_off = fi->fh;
	objfs->ioctl_cmd = cmd;

	return 0;
}

/*
 * pmemobjfs_fuse_rename -- (FUSE) rename file or directory
 */
static int
pmemobjfs_fuse_rename(const char *path, const char *dest)
{
	log("%s dest %s\n", path, dest);
	struct pmemobjfs *objfs = PMEMOBJFS;

	int ret;

	/* get source inode's parent and name */
	TOID(struct objfs_inode) src_parent;
	const char *src_name;
	ret = pmemobjfs_inode_lookup_parent(objfs, path,
			&src_parent, &src_name);
	if (ret)
		return ret;

	/* get destination inode's parent and name */
	TOID(struct objfs_inode) dst_parent;
	const char *dst_name;
	ret = pmemobjfs_inode_lookup_parent(objfs, dest,
			&dst_parent, &dst_name);
	if (ret)
		return ret;

	return pmemobjfs_rename(objfs,
			src_parent, src_name,
			dst_parent, dst_name);
}

/*
 * pmemobjfs_fuse_symlink -- (FUSE) create symbolic link
 */
static int
pmemobjfs_fuse_symlink(const char *path, const char *link)
{
	log("%s link %s", path, link);

	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	const char *name;
	int ret = pmemobjfs_inode_lookup_parent(objfs, link, &inode, &name);
	if (ret)
		return ret;

	uid_t uid = fuse_get_context()->uid;
	uid_t gid = fuse_get_context()->gid;

	return pmemobjfs_symlink(objfs, inode, name, path, uid, gid);
}

/*
 * pmemobjfs_fuse_readlink -- (FUSE) read symbolic link
 */
static int
pmemobjfs_fuse_readlink(const char *path, char *buff, size_t size)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	int ret = pmemobjfs_inode_lookup(objfs, path, &inode);
	if (ret)
		return ret;

	return pmemobjfs_symlink_read(inode, buff, size);
}

/*
 * pmemobjfs_fuse_mknod -- (FUSE) create node
 */
static int
pmemobjfs_fuse_mknod(const char *path, mode_t mode, dev_t dev)
{
	log("%s mode %o major %u minor %u", path, mode,
			(unsigned)MAJOR(dev), (unsigned)MINOR(dev));
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_inode) inode;
	const char *name;
	int ret = pmemobjfs_inode_lookup_parent(objfs, path, &inode, &name);
	if (ret)
		return ret;

	uid_t uid = fuse_get_context()->uid;
	uid_t gid = fuse_get_context()->gid;

	return pmemobjfs_mknod(objfs, inode, name, mode, uid, gid, dev);
}

/*
 * pmemobjfs_fuse_fallocate -- (FUSE) allocate blocks for file
 */
static int
pmemobjfs_fuse_fallocate(const char *path, int mode, off_t offset, off_t size,
		struct fuse_file_info *fi)
{
	log("%s mode %d offset %lu size %lu", path, mode, offset, size);
	struct pmemobjfs *objfs = PMEMOBJFS;

	if (!fi->fh)
		return -EINVAL;

	TOID(struct objfs_inode) inode;
	inode.oid.off = fi->fh;
	inode.oid.pool_uuid_lo = objfs->pool_uuid_lo;

	if (!TOID_VALID(inode))
		return -EINVAL;

	return pmemobjfs_fallocate(objfs, inode, offset, size);
}

/*
 * pmemobjfs_fuse_statvfs -- (FUSE) get filesystem info
 */
static int
pmemobjfs_fuse_statvfs(const char *path, struct statvfs *buff)
{
	log("%s", path);
	struct pmemobjfs *objfs = PMEMOBJFS;

	memset(buff, 0, sizeof (*buff));

	/*
	 * Some fields are ignored by FUSE.
	 * Some fields cannot be set due to the nature of pmemobjfs.
	 */
	buff->f_bsize = objfs->block_size;
	/* ignored buff->f_frsize */
	/* unknown buff->f_blocks */
	/* unknown buff->f_bfree */
	/* unknown buff->f_bavail */
	/* unknown buff->f_files */
	/* unknown buff->f_ffree */
	/* ignored buff->f_favail */
	/* ignored buff->f_fsid */
	/* ignored buff->f_flag */
	buff->f_namemax = objfs->max_name;

	return 0;
}

/*
 * pmemobjfs_fuse_init -- (FUSE) initialization
 */
static void *
pmemobjfs_fuse_init(struct fuse_conn_info *conn)
{
	log("");
	struct pmemobjfs *objfs = PMEMOBJFS;

	TOID(struct objfs_super) super =
		POBJ_ROOT(objfs->pop, struct objfs_super);

	/* fill some runtime information */
	objfs->block_size = D_RO(super)->block_size;
	objfs->max_name = objfs->block_size - sizeof (struct objfs_dir_entry);
	objfs->pool_uuid_lo = super.oid.pool_uuid_lo;

	TX_BEGIN(objfs->pop) {
		/* release all opened inodes */
		map_foreach(objfs->mapc, D_RW(super)->opened,
				pmemobjfs_put_opened_cb, objfs);
		/* clear opened inodes map */
		map_clear(objfs->mapc, D_RW(super)->opened);
	} TX_ONABORT {
		objfs = NULL;
	} TX_END

	return objfs;
}

/*
 * pmemobjfs_ops -- fuse operations
 */
static struct fuse_operations pmemobjfs_ops = {
	/* filesystem operations */
	.init		= pmemobjfs_fuse_init,
	.statfs		= pmemobjfs_fuse_statvfs,

	/* inode operations */
	.getattr	= pmemobjfs_fuse_getattr,
	.chmod		= pmemobjfs_fuse_chmod,
	.chown		= pmemobjfs_fuse_chown,
	.utimens	= pmemobjfs_fuse_utimens,
	.ioctl		= pmemobjfs_fuse_ioctl,

	/* directory operations */
	.opendir	= pmemobjfs_fuse_opendir,
	.releasedir	= pmemobjfs_fuse_releasedir,
	.readdir	= pmemobjfs_fuse_readdir,
	.mkdir		= pmemobjfs_fuse_mkdir,
	.rmdir		= pmemobjfs_fuse_rmdir,
	.rename		= pmemobjfs_fuse_rename,
	.mknod		= pmemobjfs_fuse_mknod,
	.symlink	= pmemobjfs_fuse_symlink,
	.create		= pmemobjfs_fuse_create,
	.unlink		= pmemobjfs_fuse_unlink,

	/* regular file operations */
	.open		= pmemobjfs_fuse_open,
	.release	= pmemobjfs_fuse_release,
	.write		= pmemobjfs_fuse_write,
	.read		= pmemobjfs_fuse_read,
	.flush		= pmemobjfs_fuse_flush,
	.truncate	= pmemobjfs_fuse_truncate,
	.ftruncate	= pmemobjfs_fuse_ftruncate,
	.fallocate	= pmemobjfs_fuse_fallocate,

	/* symlink operations */
	.readlink	= pmemobjfs_fuse_readlink,
};

/*
 * pmemobjfs_mkfs -- create pmemobjfs filesystem
 */
static int
pmemobjfs_mkfs(const char *fname, size_t size, size_t bsize, mode_t mode)
{
	/* remove file if exists */
	if (access(fname, F_OK) == 0)
		remove(fname);

	struct pmemobjfs *objfs = calloc(1, sizeof (*objfs));
	if (!objfs)
		return -1;

	int ret = 0;

	objfs->block_size = bsize;

	objfs->pop = pmemobj_create(fname, POBJ_LAYOUT_NAME(pmemobjfs),
			size, mode);
	if (!objfs->pop) {
		fprintf(stderr, "error: %s\n", pmemobj_errormsg());
		ret = -1;
		goto out_free_objfs;
	}

	objfs->mapc = map_ctx_init(MAP_CTREE, objfs->pop);
	if (!objfs->mapc) {
		perror("map_ctx_init");
		ret = -1;
		goto out_close_pop;
	}

	TOID(struct objfs_super) super =
		POBJ_ROOT(objfs->pop, struct objfs_super);

	uid_t uid = getuid();
	gid_t gid = getgid();
	mode_t mask = umask(0);
	umask(mask);

	TX_BEGIN(objfs->pop) {
		/* inherit permissions from umask */
		uint64_t root_flags = S_IFDIR | (~mask & 0777);
		TX_ADD(super);

		/* create an opened files map */
		map_new(objfs->mapc, &D_RW(super)->opened, NULL);

		/* create root inode, inherit uid and gid from current user */
		D_RW(super)->root_inode =
			pmemobjfs_new_dir(objfs, TOID_NULL(struct objfs_inode),
					"/", root_flags, uid, gid);

		D_RW(super)->block_size = bsize;
	} TX_ONABORT {
		fprintf(stderr, "error: creating pmemobjfs aborted\n");
		ret = -ECANCELED;
	} TX_END

	map_ctx_free(objfs->mapc);
out_close_pop:
	pmemobj_close(objfs->pop);
out_free_objfs:
	free(objfs);

	return ret;
}

/*
 * parse_size -- parse size from string
 */
static int
parse_size(const char *str, uint64_t *sizep)
{
	uint64_t size = 0;
	int shift = 0;
	char unit[3] = {0};
	int ret = sscanf(str, "%lu%3s", &size, unit);
	if (ret <= 0)
		return -1;
	if (ret == 2) {
		if ((unit[1] != '\0' && unit[1] != 'B') ||
			unit[2] != '\0')
			return -1;
		switch (unit[0]) {
		case 'K':
		case 'k':
			shift = 10;
			break;
		case 'M':
			shift = 20;
			break;
		case 'G':
			shift = 30;
			break;
		case 'T':
			shift = 40;
			break;
		case 'P':
			shift = 50;
			break;
		default:
			return -1;
		}
	}

	if (sizep)
		*sizep = size << shift;

	return 0;
}

/*
 * pmemobjfs_mkfs_main -- parse arguments and create pmemobjfs
 */
static int
pmemobjfs_mkfs_main(int argc, char *argv[])
{
	static const char *usage_str =
		"usage: %s "
		"[-h] "
		"[-s <size>] "
		"[-b <block_size>] "
		"<file>\n";

	if (argc < 2) {
		fprintf(stderr, usage_str, argv[0]);
		return -1;
	}

	uint64_t size = PMEMOBJ_MIN_POOL;
	uint64_t bsize = PMEMOBJFS_MIN_BLOCK_SIZE;
	int opt;
	const char *optstr = "hs:b:";
	while ((opt = getopt(argc, argv, optstr)) != -1) {
		switch (opt) {
		case 'h':
			printf(usage_str, argv[0]);
			return 0;
		case 'b':
			if (parse_size(optarg, &bsize)) {
				fprintf(stderr, "error: invalid block size "
					"value specified -- '%s'\n", optarg);
				return -1;
			}
			break;
		case 's':
			if (parse_size(optarg, &size)) {
				fprintf(stderr, "error: invalid size "
					"value specified  -- '%s'\n", optarg);
				return -1;
			}
			break;
		}
	}

	if (optind >= argc) {
		fprintf(stderr, usage_str, argv[0]);
		return -1;
	}

	if (size < PMEMOBJ_MIN_POOL) {
		fprintf(stderr, "error: minimum size is %lu\n",
				PMEMOBJ_MIN_POOL);
		return -1;
	}

	if (bsize < PMEMOBJFS_MIN_BLOCK_SIZE) {
		fprintf(stderr, "error: minimum block size is %lu\n",
				PMEMOBJFS_MIN_BLOCK_SIZE);
		return -1;
	}

	const char *path = argv[optind];

	return pmemobjfs_mkfs(path, size, bsize, 0777);

}

/*
 * pmemobjfs_tx_ioctl -- transaction ioctl
 *
 * In order to call the ioctl we need to create a temporary file in
 * specified directory and call the ioctl on that file. After calling the
 * ioctl the file is unlinked. The actual action is performed after unlinking
 * the file so if the operation was to start a transaction the temporary file
 * won't be unlinked within the transaction.
 */
static int
pmemobjfs_tx_ioctl(const char *dir, int req)
{
	int ret = 0;

	/* append temporary file template to specified path */
	size_t len = strlen(dir) + strlen(PMEMOBJFS_TMP_TEMPLATE) + 1;
	char *path = malloc(len);
	if (!path)
		return -1;

	strncpy(path, dir, len);
	strncat(path, PMEMOBJFS_TMP_TEMPLATE, len);

	/* create temporary file */
	int fd = mkstemp(path);
	if (fd < 0) {
		perror(path);
		ret = -1;
		goto out_free;
	}

	/* perform specified ioctl command */
	ret = ioctl(fd, req);
	if (ret) {
		perror(path);
		goto out_unlink;
	}

out_unlink:
	/* unlink temporary file */
	ret = unlink(path);
	if (ret)
		perror(path);
	close(fd);
out_free:
	free(path);
	return ret;
}

int
main(int argc, char *argv[])
{
	char *bname = basename(argv[0]);
	if (strcmp(PMEMOBJFS_MKFS, bname) == 0) {
		return pmemobjfs_mkfs_main(argc, argv);
	} else if (strcmp(PMEMOBJFS_TX_BEGIN, bname) == 0) {
		if (argc != 2) {
			fprintf(stderr, "usage: %s <dir>\n", bname);
			return -1;
		}
		char *arg = argv[1];
		return pmemobjfs_tx_ioctl(arg, PMEMOBJFS_CTL_TX_BEGIN);
	} else if (strcmp(PMEMOBJFS_TX_COMMIT, bname) == 0) {
		if (argc != 2) {
			fprintf(stderr, "usage: %s <dir>\n", bname);
			return -1;
		}
		char *arg = argv[1];
		return pmemobjfs_tx_ioctl(arg, PMEMOBJFS_CTL_TX_COMMIT);
	} else if (strcmp(PMEMOBJFS_TX_ABORT, bname) == 0) {
		if (argc != 2) {
			fprintf(stderr, "usage: %s <dir>\n", bname);
			return -1;
		}
		char *arg = argv[1];
		return pmemobjfs_tx_ioctl(arg, PMEMOBJFS_CTL_TX_ABORT);
	}
#if DEBUG
	log_fh = fopen("pmemobjfs.log", "w+");
	if (!log_fh)
		err(-1, "pmemobjfs.log");

	log("\n\n\nPMEMOBJFS\n");
#endif


	const char *fname = argv[argc - 2];
	struct pmemobjfs *objfs = calloc(1, sizeof (*objfs));
	if (!objfs) {
		perror("malloc");
		return -1;
	}

	int ret = 0;
	objfs->mapc = map_ctx_init(MAP_CTREE, objfs->pop);
	if (!objfs->mapc) {
		perror("map_ctx_init");
		ret = -1;
		goto out;
	}

	objfs->pop = pmemobj_open(fname, POBJ_LAYOUT_NAME(pmemobjfs));
	if (objfs->pop == NULL) {
		perror("pmemobj_open");
		ret = -1;
		goto out;
	}
	argv[argc - 2] = argv[argc - 1];
	argv[argc - 1] = NULL;
	argc--;

	ret = fuse_main(argc, argv, &pmemobjfs_ops, objfs);

	pmemobj_close(objfs->pop);
out:
	free(objfs);
	log("ret = %d", ret);
	return ret;
}
