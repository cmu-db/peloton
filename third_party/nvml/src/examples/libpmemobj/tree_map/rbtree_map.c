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
 * rbtree.c -- red-black tree implementation /w sentinel nodes
 */

#include <assert.h>
#include <errno.h>
#include "rbtree_map.h"

TOID_DECLARE(struct tree_map_node, RBTREE_MAP_TYPE_OFFSET + 1);

#define	NODE_P(_n)\
D_RW(_n)->parent

#define	NODE_GRANDP(_n)\
NODE_P(NODE_P(_n))

#define	NODE_PARENT_AT(_n, _rbc)\
D_RW(NODE_P(_n))->slots[_rbc]

#define	NODE_PARENT_RIGHT(_n)\
NODE_PARENT_AT(_n, RB_RIGHT)

#define	NODE_IS(_n, _rbc)\
TOID_EQUALS(_n, NODE_PARENT_AT(_n, _rbc))

#define	NODE_IS_RIGHT(_n)\
TOID_EQUALS(_n, NODE_PARENT_RIGHT(_n))

#define	NODE_LOCATION(_n)\
NODE_IS_RIGHT(_n)

#define	RB_FIRST(_m)\
D_RW(D_RW(_m)->root)->slots[RB_LEFT]

#define	NODE_IS_NULL(_n)\
TOID_EQUALS(_n, s)

enum rb_color {
	COLOR_BLACK,
	COLOR_RED,

	MAX_COLOR
};

enum rb_children {
	RB_LEFT,
	RB_RIGHT,

	MAX_RB
};

struct tree_map_node {
	uint64_t key;
	PMEMoid value;
	enum rb_color color;
	TOID(struct tree_map_node) parent;
	TOID(struct tree_map_node) slots[MAX_RB];
};

struct rbtree_map {
	TOID(struct tree_map_node) sentinel;
	TOID(struct tree_map_node) root;
};

/*
 * rbtree_map_new -- allocates a new red-black tree instance
 */
int
rbtree_map_new(PMEMobjpool *pop, TOID(struct rbtree_map) *map, void *arg)
{
	int ret = 0;
	TX_BEGIN(pop) {
		pmemobj_tx_add_range_direct(map, sizeof (*map));
		*map = TX_ZNEW(struct rbtree_map);

		TOID(struct tree_map_node) s = TX_ZNEW(struct tree_map_node);
		D_RW(s)->color = COLOR_BLACK;
		D_RW(s)->parent = s;
		D_RW(s)->slots[RB_LEFT] = s;
		D_RW(s)->slots[RB_RIGHT] = s;

		TOID(struct tree_map_node) r = TX_ZNEW(struct tree_map_node);
		D_RW(r)->color = COLOR_BLACK;
		D_RW(r)->parent = s;
		D_RW(r)->slots[RB_LEFT] = s;
		D_RW(r)->slots[RB_RIGHT] = s;

		D_RW(*map)->sentinel = s;
		D_RW(*map)->root = r;
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * rbtree_map_clear_node -- (internal) clears this node and its children
 */
static void
rbtree_map_clear_node(TOID(struct tree_map_node) p)
{
	rbtree_map_clear_node(D_RO(p)->slots[RB_LEFT]);
	rbtree_map_clear_node(D_RO(p)->slots[RB_RIGHT]);

	pmemobj_tx_free(p.oid);
}


/*
 * rbtree_map_clear -- removes all elements from the map
 */
int
rbtree_map_clear(PMEMobjpool *pop, TOID(struct rbtree_map) map)
{
	TX_BEGIN(pop) {
		rbtree_map_clear_node(D_RW(map)->root);
		TX_ADD_FIELD(map, root);
		D_RW(map)->root = TOID_NULL(struct tree_map_node);
	} TX_END

	return 0;
}


/*
 * rbtree_map_delete -- cleanups and frees crit-bit tree instance
 */
int
rbtree_map_delete(PMEMobjpool *pop, TOID(struct rbtree_map) *map)
{
	int ret = 0;
	TX_BEGIN(pop) {
		rbtree_map_clear(pop, *map);
		pmemobj_tx_add_range_direct(map, sizeof (*map));
		TX_FREE(*map);
		*map = TOID_NULL(struct rbtree_map);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * rbtree_map_rotate -- (internal) performs a left/right rotation around a node
 */
static void
rbtree_map_rotate(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) node, enum rb_children c)
{
	TOID(struct tree_map_node) child = D_RO(node)->slots[!c];
	TOID(struct tree_map_node) s = D_RO(map)->sentinel;

	TX_ADD(node);
	TX_ADD(child);

	D_RW(node)->slots[!c] = D_RO(child)->slots[c];

	if (!TOID_EQUALS(D_RO(child)->slots[c], s))
		TX_SET(D_RW(child)->slots[c], parent, node);

	NODE_P(child) = NODE_P(node);

	TX_SET(NODE_P(node), slots[NODE_LOCATION(node)], child);

	D_RW(child)->slots[c] = node;
	D_RW(node)->parent = child;
}

/*
 * rbtree_map_insert_bst -- (internal) inserts a node in regular BST fashion
 */
static void
rbtree_map_insert_bst(TOID(struct rbtree_map) map, TOID(struct tree_map_node) n)
{
	TOID(struct tree_map_node) parent = D_RO(map)->root;
	TOID(struct tree_map_node) *dst = &RB_FIRST(map);
	TOID(struct tree_map_node) s = D_RO(map)->sentinel;

	D_RW(n)->slots[RB_LEFT] = s;
	D_RW(n)->slots[RB_RIGHT] = s;

	while (!NODE_IS_NULL(*dst)) {
		parent = *dst;
		dst = &D_RW(*dst)->slots[D_RO(n)->key > D_RO(*dst)->key];
	}

	TX_SET(n, parent, parent);
	*dst = n;
}

/*
 * rbtree_map_recolor -- (internal) restores red-black tree properties
 */
static TOID(struct tree_map_node)
rbtree_map_recolor(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) n, enum rb_children c)
{
	TOID(struct tree_map_node) uncle = D_RO(NODE_GRANDP(n))->slots[!c];

	if (D_RO(uncle)->color == COLOR_RED) {
		TX_SET(uncle, color, COLOR_BLACK);
		TX_SET(NODE_P(n), color, COLOR_BLACK);
		TX_SET(NODE_GRANDP(n), color, COLOR_RED);
		return NODE_GRANDP(n);
	} else {
		if (NODE_IS(n, !c)) {
			n = NODE_P(n);
			rbtree_map_rotate(map, n, c);
		}

		TX_SET(NODE_P(n), color, COLOR_BLACK);
		TX_SET(NODE_GRANDP(n), color, COLOR_RED);
		rbtree_map_rotate(map, NODE_GRANDP(n), !c);
	}

	return n;
}

/*
 * rbtree_map_insert -- inserts a new key-value pair into the map
 */
int
rbtree_map_insert(PMEMobjpool *pop, TOID(struct rbtree_map) map,
	uint64_t key, PMEMoid value)
{
	int ret = 0;

	TX_BEGIN(pop) {
		TOID(struct tree_map_node) n = TX_ZNEW(struct tree_map_node);
		D_RW(n)->key = key;
		D_RW(n)->value = value;

		rbtree_map_insert_bst(map, n);

		D_RW(n)->color = COLOR_RED;
		while (D_RO(NODE_P(n))->color == COLOR_RED)
			n = rbtree_map_recolor(map, n,
					NODE_LOCATION(NODE_P(n)));

		TX_SET(RB_FIRST(map), color, COLOR_BLACK);
	} TX_END

	return ret;
}

/*
 * rbtree_map_successor -- (internal) returns the successor of a node
 */
static TOID(struct tree_map_node)
rbtree_map_successor(TOID(struct rbtree_map) map, TOID(struct tree_map_node) n)
{
	TOID(struct tree_map_node) dst = D_RO(n)->slots[RB_RIGHT];
	TOID(struct tree_map_node) s = D_RO(map)->sentinel;

	if (!TOID_EQUALS(s, dst)) {
		while (!NODE_IS_NULL(D_RO(dst)->slots[RB_LEFT]))
			dst = D_RO(dst)->slots[RB_LEFT];
	} else {
		dst = D_RO(n)->parent;
		while (TOID_EQUALS(n, D_RO(dst)->slots[RB_RIGHT])) {
			n = dst;
			dst = NODE_P(dst);
		}
		if (TOID_EQUALS(dst, D_RO(map)->root))
			return s;
	}

	return dst;
}

/*
 * rbtree_map_find_node -- (internal) returns the node that contains the key
 */
static TOID(struct tree_map_node)
rbtree_map_find_node(TOID(struct rbtree_map) map, uint64_t key)
{
	TOID(struct tree_map_node) dst = RB_FIRST(map);
	TOID(struct tree_map_node) s = D_RO(map)->sentinel;

	while (!NODE_IS_NULL(dst)) {
		if (D_RO(dst)->key == key)
			return dst;

		dst = D_RO(dst)->slots[key > D_RO(dst)->key];
	}

	return TOID_NULL(struct tree_map_node);
}

/*
 * rbtree_map_repair_branch -- (internal) restores red-black tree in one branch
 */
static TOID(struct tree_map_node)
rbtree_map_repair_branch(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) n, enum rb_children c)
{
	TOID(struct tree_map_node) sb = NODE_PARENT_AT(n, !c); /* sibling */
	if (D_RO(sb)->color == COLOR_RED) {
		TX_SET(sb, color, COLOR_BLACK);
		TX_SET(NODE_P(n), color, COLOR_RED);
		rbtree_map_rotate(map, NODE_P(n), c);
		sb = NODE_PARENT_AT(n, !c);
	}

	if (D_RO(D_RO(sb)->slots[RB_RIGHT])->color == COLOR_BLACK &&
		D_RO(D_RO(sb)->slots[RB_LEFT])->color == COLOR_BLACK) {
		TX_SET(sb, color, COLOR_RED);
		return D_RO(n)->parent;
	} else {
		if (D_RO(D_RO(sb)->slots[!c])->color == COLOR_BLACK) {
			TX_SET(D_RW(sb)->slots[c], color, COLOR_BLACK);
			TX_SET(sb, color, COLOR_RED);
			rbtree_map_rotate(map, sb, !c);
			sb = NODE_PARENT_AT(n, !c);
		}
		TX_SET(sb, color, D_RO(NODE_P(n))->color);
		TX_SET(NODE_P(n), color, COLOR_BLACK);
		TX_SET(D_RW(sb)->slots[!c], color, COLOR_BLACK);
		rbtree_map_rotate(map, NODE_P(n), c);

		return RB_FIRST(map);
	}

	return n;
}

/*
 * rbtree_map_repair -- (internal) restores red-black tree properties
 * after remove
 */
static void
rbtree_map_repair(TOID(struct rbtree_map) map, TOID(struct tree_map_node) n)
{
	/* if left, repair right sibling, otherwise repair left sibling. */
	while (!TOID_EQUALS(n, RB_FIRST(map)) && D_RO(n)->color == COLOR_BLACK)
		n = rbtree_map_repair_branch(map, n, NODE_LOCATION(n));

	TX_SET(n, color, COLOR_BLACK);
}

/*
 * rbtree_map_remove -- removes key-value pair from the map
 */
PMEMoid
rbtree_map_remove(PMEMobjpool *pop, TOID(struct rbtree_map) map, uint64_t key)
{
	PMEMoid ret = OID_NULL;

	TOID(struct tree_map_node) n = rbtree_map_find_node(map, key);
	if (TOID_IS_NULL(n))
		return ret;

	ret = D_RO(n)->value;

	TOID(struct tree_map_node) s = D_RO(map)->sentinel;
	TOID(struct tree_map_node) r = D_RO(map)->root;

	TOID(struct tree_map_node) y = (NODE_IS_NULL(D_RO(n)->slots[RB_LEFT]) ||
					NODE_IS_NULL(D_RO(n)->slots[RB_RIGHT]))
					? n : rbtree_map_successor(map, n);

	TOID(struct tree_map_node) x = NODE_IS_NULL(D_RO(y)->slots[RB_LEFT]) ?
			D_RO(y)->slots[RB_RIGHT] : D_RO(y)->slots[RB_LEFT];

	TX_BEGIN(pop) {
		TX_SET(x, parent, NODE_P(y));
		if (TOID_EQUALS(NODE_P(x), r)) {
			TX_SET(r, slots[RB_LEFT], x);
		} else {
			TX_SET(NODE_P(y), slots[NODE_LOCATION(y)], x);
		}

		if (D_RO(y)->color == COLOR_BLACK)
			rbtree_map_repair(map, x);

		if (!TOID_EQUALS(y, n)) {
			TX_ADD(y);
			D_RW(y)->slots[RB_LEFT] = D_RO(n)->slots[RB_LEFT];
			D_RW(y)->slots[RB_RIGHT] = D_RO(n)->slots[RB_RIGHT];
			D_RW(y)->parent = D_RO(n)->parent;
			D_RW(y)->color = D_RO(n)->color;
			TX_SET(D_RW(n)->slots[RB_LEFT], parent, y);
			TX_SET(D_RW(n)->slots[RB_RIGHT], parent, y);

			TX_SET(NODE_P(n), slots[NODE_LOCATION(n)], y);
		}
		TX_FREE(n);
	} TX_END

	return ret;
}

/*
 * rbtree_map_get -- searches for a value of the key
 */
PMEMoid
rbtree_map_get(PMEMobjpool *pop, TOID(struct rbtree_map) map, uint64_t key)
{
	TOID(struct tree_map_node) node = rbtree_map_find_node(map, key);
	if (TOID_IS_NULL(node))
		return OID_NULL;

	return D_RO(node)->value;
}

/*
 * rbtree_map_lookup -- searches if key exists
 */
int
rbtree_map_lookup(PMEMobjpool *pop, TOID(struct rbtree_map) map, uint64_t key)
{
	TOID(struct tree_map_node) node = rbtree_map_find_node(map, key);
	if (TOID_IS_NULL(node))
		return 0;

	return 1;
}

/*
 * rbtree_map_foreach_node -- (internal) recursively traverses tree
 */
static int
rbtree_map_foreach_node(TOID(struct rbtree_map) map,
	TOID(struct tree_map_node) p,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	int ret = 0;

	if (TOID_EQUALS(p, D_RO(map)->sentinel))
		return 0;

	if ((ret = rbtree_map_foreach_node(map,
		D_RO(p)->slots[RB_LEFT], cb, arg)) == 0) {
		if ((ret = cb(D_RO(p)->key, D_RO(p)->value, arg)) == 0)
			rbtree_map_foreach_node(map,
				D_RO(p)->slots[RB_RIGHT], cb, arg);
	}

	return ret;
}

/*
 * rbtree_map_foreach -- initiates recursive traversal
 */
int
rbtree_map_foreach(PMEMobjpool *pop, TOID(struct rbtree_map) map,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	return rbtree_map_foreach_node(map, RB_FIRST(map), cb, arg);
}

/*
 * rbtree_map_is_empty -- checks whether the tree map is empty
 */
int
rbtree_map_is_empty(PMEMobjpool *pop, TOID(struct rbtree_map) map)
{
	return TOID_IS_NULL(RB_FIRST(map));
}

/*
 * rbtree_map_check -- check if given persistent object is a tree map
 */
int
rbtree_map_check(PMEMobjpool *pop, TOID(struct rbtree_map) map)
{
	return !TOID_VALID(map);
}

/*
 * rbtree_map_insert_new -- allocates a new object and inserts it into the tree
 */
int
rbtree_map_insert_new(PMEMobjpool *pop, TOID(struct rbtree_map) map,
		uint64_t key, size_t size, unsigned int type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	int ret = 0;

	TX_BEGIN(pop) {
		PMEMoid n = pmemobj_tx_alloc(size, type_num);
		constructor(pop, pmemobj_direct(n), arg);
		rbtree_map_insert(pop, map, key, n);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}

/*
 * rbtree_map_remove_free -- removes and frees an object from the tree
 */
int
rbtree_map_remove_free(PMEMobjpool *pop, TOID(struct rbtree_map) map,
		uint64_t key)
{
	int ret = 0;

	TX_BEGIN(pop) {
		PMEMoid val = rbtree_map_remove(pop, map, key);
		pmemobj_free(&val);
	} TX_ONABORT {
		ret = 1;
	} TX_END

	return ret;
}
