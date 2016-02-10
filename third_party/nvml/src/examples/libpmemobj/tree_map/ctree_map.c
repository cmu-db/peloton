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
 * ctree_map.c -- Crit-bit trie implementation
 */

#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include "ctree_map.h"

#define	BIT_IS_SET(n, i) (!!((n) & (1L << (i))))

TOID_DECLARE(struct tree_map_node, CTREE_MAP_TYPE_OFFSET + 1);

struct tree_map_entry {
	uint64_t key;
	PMEMoid slot;
};

struct tree_map_node {
	int diff; /* most significant differing bit */
	struct tree_map_entry entries[2];
};

struct ctree_map {
	struct tree_map_entry root;
};

/*
 * find_crit_bit -- (internal) finds the most significant differing bit
 */
static int
find_crit_bit(uint64_t lhs, uint64_t rhs)
{
	return 64 - __builtin_clzll(lhs ^ rhs) - 1;
}

/*
 * ctree_map_new -- allocates a new crit-bit tree instance
 */
int
ctree_map_new(PMEMobjpool *pop, TOID(struct ctree_map) *map, void *arg)
{
	int ret = 0;

	TX_BEGIN(pop) {
		pmemobj_tx_add_range_direct(map, sizeof (*map));
		*map = TX_ZNEW(struct ctree_map);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * ctree_map_clear_node -- (internal) clears this node and its children
 */
static void
ctree_map_clear_node(PMEMoid p)
{
	if (OID_INSTANCEOF(p, struct tree_map_node)) {
		TOID(struct tree_map_node) node;
		TOID_ASSIGN(node, p);

		ctree_map_clear_node(D_RW(node)->entries[0].slot);
		ctree_map_clear_node(D_RW(node)->entries[1].slot);
	}

	pmemobj_tx_free(p);
}


/*
 * ctree_map_clear -- removes all elements from the map
 */
int
ctree_map_clear(PMEMobjpool *pop, TOID(struct ctree_map) map)
{
	TX_BEGIN(pop) {
		ctree_map_clear_node(D_RW(map)->root.slot);
		TX_ADD_FIELD(map, root);
		D_RW(map)->root.slot = OID_NULL;
	} TX_END

	return 0;
}

/*
 * ctree_map_delete -- cleanups and frees crit-bit tree instance
 */
int
ctree_map_delete(PMEMobjpool *pop, TOID(struct ctree_map) *map)
{
	int ret = 0;
	TX_BEGIN(pop) {
		ctree_map_clear(pop, *map);
		pmemobj_tx_add_range_direct(map, sizeof (*map));
		TX_FREE(*map);
		*map = TOID_NULL(struct ctree_map);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * ctree_map_insert_leaf -- (internal) inserts a new leaf at the position
 */
static void
ctree_map_insert_leaf(struct tree_map_entry *p,
	struct tree_map_entry e, int diff)
{
	TOID(struct tree_map_node) new_node = TX_NEW(struct tree_map_node);
	D_RW(new_node)->diff = diff;

	int d = BIT_IS_SET(e.key, D_RO(new_node)->diff);

	/* insert the leaf at the direction based on the critical bit */
	D_RW(new_node)->entries[d] = e;

	/* find the appropriate position in the tree to insert the node */
	TOID(struct tree_map_node) node;
	while (OID_INSTANCEOF(p->slot, struct tree_map_node)) {
		TOID_ASSIGN(node, p->slot);

		/* the critical bits have to be sorted */
		if (D_RO(node)->diff < D_RO(new_node)->diff)
			break;

		p = &D_RW(node)->entries[BIT_IS_SET(e.key, D_RO(node)->diff)];
	}

	/* insert the found destination in the other slot */
	D_RW(new_node)->entries[!d] = *p;

	pmemobj_tx_add_range_direct(p, sizeof (*p));
	p->key = 0;
	p->slot = new_node.oid;
}

/*
 * ctree_map_insert_new -- allocates a new object and inserts it into the tree
 */
int
ctree_map_insert_new(PMEMobjpool *pop, TOID(struct ctree_map) map,
		uint64_t key, size_t size, unsigned int type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	int ret = 0;

	TX_BEGIN(pop) {
		PMEMoid n = pmemobj_tx_alloc(size, type_num);
		constructor(pop, pmemobj_direct(n), arg);
		ctree_map_insert(pop, map, key, n);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}


/*
 * ctree_map_insert -- inserts a new key-value pair into the map
 */
int
ctree_map_insert(PMEMobjpool *pop, TOID(struct ctree_map) map,
	uint64_t key, PMEMoid value)
{
	struct tree_map_entry *p = &D_RW(map)->root;
	int ret = 0;

	/* descend the path until a best matching key is found */
	TOID(struct tree_map_node) node;
	while (OID_INSTANCEOF(p->slot, struct tree_map_node)) {
		TOID_ASSIGN(node, p->slot);
		p = &D_RW(node)->entries[BIT_IS_SET(key, D_RW(node)->diff)];
	}

	struct tree_map_entry e = {key, value};
	TX_BEGIN(pop) {
		if (p->key == 0 || p->key == key) {
			pmemobj_tx_add_range_direct(p, sizeof (*p));
			*p = e;
		} else {
			ctree_map_insert_leaf(&D_RW(map)->root, e,
					find_crit_bit(p->key, key));
		}
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * ctree_map_get_leaf -- (internal) searches for a leaf of the key
 */
static struct tree_map_entry *
ctree_map_get_leaf(TOID(struct ctree_map) map, uint64_t key,
	struct tree_map_entry **parent)
{
	struct tree_map_entry *n = &D_RW(map)->root;
	struct tree_map_entry *p = NULL;

	TOID(struct tree_map_node) node;
	while (OID_INSTANCEOF(n->slot, struct tree_map_node)) {
		TOID_ASSIGN(node, n->slot);

		p = n;
		n = &D_RW(node)->entries[BIT_IS_SET(key, D_RW(node)->diff)];
	}

	if (n->key == key) {
		if (parent)
			*parent = p;

		return n;
	}

	return NULL;
}

/*
 * ctree_map_remove_free -- removes and frees an object from the tree
 */
int
ctree_map_remove_free(PMEMobjpool *pop, TOID(struct ctree_map) map,
		uint64_t key)
{
	int ret = 0;

	TX_BEGIN(pop) {
		PMEMoid val = ctree_map_remove(pop, map, key);
		pmemobj_free(&val);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * ctree_map_remove -- removes key-value pair from the map
 */
PMEMoid
ctree_map_remove(PMEMobjpool *pop, TOID(struct ctree_map) map, uint64_t key)
{
	struct tree_map_entry *parent = NULL;
	struct tree_map_entry *leaf = ctree_map_get_leaf(map, key, &parent);
	if (leaf == NULL)
		return OID_NULL;

	PMEMoid ret = leaf->slot;

	if (parent == NULL) { /* root */
		TX_BEGIN(pop) {
			pmemobj_tx_add_range_direct(leaf, sizeof (*leaf));
			leaf->key = 0;
			leaf->slot = OID_NULL;
		} TX_END
	} else {
		/*
		 * In this situation:
		 *	 parent
		 *	/     \
		 *   LEFT   RIGHT
		 * there's no point in leaving the parent internal node
		 * so it's swapped with the remaining node and then also freed.
		 */
		TX_BEGIN(pop) {
			struct tree_map_entry *dest = parent;
			TOID(struct tree_map_node) node;
			TOID_ASSIGN(node, parent->slot);
			pmemobj_tx_add_range_direct(dest, sizeof (*dest));
			*dest = D_RW(node)->entries[
				D_RO(node)->entries[0].key == leaf->key];

			TX_FREE(node);
		} TX_END
	}

	return ret;
}

/*
 * ctree_map_get -- searches for a value of the key
 */
PMEMoid
ctree_map_get(PMEMobjpool *pop, TOID(struct ctree_map) map, uint64_t key)
{
	struct tree_map_entry *entry = ctree_map_get_leaf(map, key, NULL);
	return entry ? entry->slot : OID_NULL;
}

/*
 * ctree_map_lookup -- searches if a key exists
 */
int
ctree_map_lookup(PMEMobjpool *pop, TOID(struct ctree_map) map,
		uint64_t key)
{
	struct tree_map_entry *entry = ctree_map_get_leaf(map, key, NULL);
	return entry != NULL;
}

/*
 * ctree_map_foreach_node -- (internal) recursively traverses tree
 */
static int
ctree_map_foreach_node(struct tree_map_entry e,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	int ret = 0;

	if (OID_INSTANCEOF(e.slot, struct tree_map_node)) {
		TOID(struct tree_map_node) node;
		TOID_ASSIGN(node, e.slot);

		if (ctree_map_foreach_node(D_RO(node)->entries[0],
					cb, arg) == 0)
			ctree_map_foreach_node(D_RO(node)->entries[1], cb, arg);
	} else { /* leaf */
		ret = cb(e.key, e.slot, arg);
	}

	return ret;
}

/*
 * ctree_map_foreach -- initiates recursive traversal
 */
int
ctree_map_foreach(PMEMobjpool *pop, TOID(struct ctree_map) map,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	if (OID_IS_NULL(D_RO(map)->root.slot))
		return 0;

	return ctree_map_foreach_node(D_RO(map)->root, cb, arg);
}

/*
 * ctree_map_is_empty -- checks whether the tree map is empty
 */
int
ctree_map_is_empty(PMEMobjpool *pop, TOID(struct ctree_map) map)
{
	return D_RO(map)->root.key == 0;
}

/*
 * ctree_map_check -- check if given persistent object is a tree map
 */
int
ctree_map_check(PMEMobjpool *pop, TOID(struct ctree_map) map)
{
	return !TOID_VALID(map);
}
