/**
 * @brief CONCURRENT B+Tree
 *
 * References : https://code.google.com/p/high-concurrency-btree/
 * References : https://github.com/malbrain/Btree-source-code
 *
 * @author : karl malbrain, malbrain@cal.berkeley.edu
 */

// btree version threadskv8 sched_yield version
//	with reworked bt_deletekey code,
//	phase-fair reader writer lock,
//	librarian page split code,
//	duplicate key management
//	bi-directional cursors
//	traditional buffer pool manager
//	and ACID batched key-value updates

/*
This work, including the source code, documentation
and related data, is placed into the public domain.

The orginal author is Karl Malbrain.

THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
RESULTING FROM THE USE, MODIFICATION, OR
REDISTRIBUTION OF THIS SOFTWARE.
 */

#pragma once

#define _FILE_OFFSET_BITS 64

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>

#include <memory.h>
#include <string.h>
#include <stddef.h>

#include "backend/catalog/schema.h"
#include "backend/common/synch.h"

namespace peloton {
namespace index {

typedef unsigned long long uid;

#define BT_ro 0x6f72  // ro
#define BT_rw 0x7772  // rw

#define BT_maxbits 24                 // maximum page size in bits
#define BT_minbits 9                  // minimum page size in bits
#define BT_minpage (1 << BT_minbits)  // minimum page size
#define BT_maxpage (1 << BT_maxbits)  // maximum page size

//  BTree page number constants
#define ALLOC_page 0  // allocation page
#define ROOT_page 1   // root of the btree
#define LEAF_page 2   // first page of leaves

//	Number of levels to create in a new BTree

#define MIN_lvl 2

/*
There are six lock types for each node in four independent sets:
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with
NodeDelete.
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with
AccessIntent.
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock.
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and
other WriteLocks.
5. (set 3) ParentModification: Exclusive. Change the node's parent keys.
Incompatible with another ParentModification.
6. (set 4) AtomicModification: Exclusive. Atomic Update including node is
underway. Incompatible with another AtomicModification.
 */

typedef enum {
  BtLockAccess = 1,
  BtLockDelete = 2,
  BtLockRead = 4,
  BtLockWrite = 8,
  BtLockParent = 16,
  BtLockAtomic = 32,
  BtLockAtomicOrBtLockRead = 36
} BtLock;

//	definition for phase-fair reader/writer lock implementation
/*
typedef struct {
  ushort rin[1];
  ushort rout[1];
  ushort ticket[1];
  ushort serving[1];
} RWLock;
*/

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4

//	definition for spin latch implementation

// exclusive is set for write access
// share is count of read accessors
// grant write lock when share == 0

volatile typedef struct BtSpinLatchType {
  ushort exclusive : 1;
  ushort pending : 1;
  ushort share : 14;
} BtSpinLatch;

#define XCL 1
#define PEND 2
#define BOTH 3
#define SHARE 4

//	write only reentrant lock

/*
typedef struct {
  BtSpinLatch xcl[1];
  volatile ushort tid[1];
  volatile ushort dup[1];
} WOLock;
*/

//  hash table entries

typedef struct {
  volatile uint slot;  // Latch table entry at head of chain
  BtSpinLatch latch[1];
} BtHashEntry;

//	latch manager table structure

typedef struct {
  uid page_no;              // latch set page number
  RWLock readwr[1];         // read/write page lock
  RWLock access[1];         // Access Intent/Page delete
  RecursiveLock parent[1];  // Posting of fence key in parent
  RecursiveLock atomic[1];  // Atomic update in progress
  uint split;               // right split page atomic insert
  uint entry;               // entry slot in latch table
  uint next;                // next entry in hash table chain
  uint prev;                // prev entry in hash table chain
  volatile ushort pin;      // number of outstanding threads
  ushort dirty : 1;         // page in cache is dirty
} BtLatchSet;

//	Define the length of the page record numbers

#define BtId 6

//	Page key slot definition.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence key (highest key) for
//	a leaf page is always present, even after cleanup.

//	Slot types

//	In addition to the Unique keys that occupy slots
//	there are Librarian and Duplicate key
//	slots occupying the key slot array.

//	The Librarian slots are dead keys that
//	serve as filler, available to add new Unique
//	or Dup slots that are inserted into the B-tree.

//	The Duplicate slots have had their key bytes extended
//	by 6 bytes to contain a binary duplicate key uniqueifier.

typedef enum { Unique, Librarian, Duplicate, Delete, Update } BtSlotType;

typedef struct {
  uint off : BT_maxbits;  // page offset for key start
  uint type : 3;          // type of slot
  uint dead : 1;          // set for deleted slot
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the key
//	bytes.

typedef struct {
  unsigned char len;  // this can be changed to a ushort or uint
  char key[0];
} BtKey;

//	the value structure also occupies space at the upper
//	end of the page. Each key is immediately followed by a value.

typedef struct {
  unsigned char len;  // this can be changed to a ushort or uint
  unsigned char value[0];
} BtVal;

#define BT_maxkey 255  // maximum number of bytes in a key
#define BT_keyarray (BT_maxkey + sizeof(BtKey))

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

//	note that this structure size
//	must be a multiple of 8 bytes
//	in order to place dups correctly.

typedef struct BtPage_ {
  uint cnt;                   // count of keys in page
  uint act;                   // count of active keys
  uint min;                   // next key offset
  uint garbage;               // page garbage in bytes
  unsigned char bits : 7;     // page size in bits
  unsigned char free : 1;     // page is on free chain
  unsigned char lvl : 7;      // level of page
  unsigned char kill : 1;     // page is being deleted
  unsigned char right[BtId];  // page number to right
  unsigned char left[BtId];   // page number to left
  unsigned char filler[2];    // padding to multiple of 8
} * BtPage;

//  The loadpage interface object

typedef struct {
  BtPage page;        // current page pointer
  BtLatchSet *latch;  // current page latch set
} BtPageSet;

//	structure for latch manager on ALLOC_page

typedef struct {
  struct BtPage_ alloc[1];     // next page_no in right ptr
  unsigned long long dups[1];  // global duplicate key uniqueifier
  unsigned char chain[BtId];   // head of free page_nos chain
} BtPageZero;

//	The object structure for Btree access

typedef struct {
  uint page_size;  // page size
  uint page_bits;  // page size in bits
  int idx;
  BtPageZero *pagezero;     // mapped allocation page
  BtSpinLatch lock[1];      // allocation area lite latch
  uint latchdeployed;       // highest number of latch entries deployed
  uint nlatchpage;          // number of latch pages at BT_latch
  uint latchtotal;          // number of page latch entries
  uint latchhash;           // number of latch hash table slots
  uint latchvictim;         // next latch entry to examine
  ushort thread_no[1];      // next thread number
  BtHashEntry *hashtable;   // the buffer pool hash table entries
  BtLatchSet *latchsets;    // mapped latch set from buffer pool
  unsigned char *pagepool;  // mapped to the buffer pool pages
} BtMgr;

typedef struct {
  BtMgr *mgr;             // buffer manager for thread
  BtPage cursor;          // cached frame for start/next (never mapped)
  BtPage frame;           // spare frame for the page split (never mapped)
  uid cursor_page;        // current cursor page number
  unsigned char *mem;     // frame, cursor, page memory buffer
  char key[BT_keyarray];  // last found complete key
  int found;              // last delete or insert was found
  int err;                // last error
  int reads, writes;      // number of reads and writes from the btree
  ushort thread_no;       // thread number

  // key schema for comparison
  const catalog::Schema *key_schema;
} BtDb;

typedef enum {
  BTERR_ok = 0,
  BTERR_struct,
  BTERR_ovflw,
  BTERR_lock,
  BTERR_map,
  BTERR_read,
  BTERR_wrt,
  BTERR_atomic
} BTERR;

#define CLOCK_bit 0x8000

// B-Tree functions
extern void bt_close(BtDb *bt);
extern BtDb *bt_open(BtMgr *mgr);
extern BTERR bt_insertkey(BtDb *bt, char *key, uint len, uint lvl, void *value,
                          uint vallen, uint unique);
extern BTERR bt_deletekey(BtDb *bt, char *key, uint len, uint lvl, uint unique);
extern int bt_findkey(BtDb *bt, char *key, uint keylen, unsigned char *value,
                      uint valmax);
extern BtKey *bt_foundkey(BtDb *bt);
extern uint bt_startkey(BtDb *bt, char *key, uint len);
extern uint bt_nextkey(BtDb *bt, uint slot);

//	manager functions
extern BtMgr *bt_mgr(char *name, uint bits, uint poolsize);
void bt_mgrclose(BtMgr *mgr);

//  Helper functions to return slot values
//	from the cursor page.

extern BtKey *bt_key(BtDb *bt, uint slot);
extern BtVal *bt_val(BtDb *bt, uint slot);

//  The page is allocated from low and hi ends.
//  The key slots are allocated from the bottom,
//	while the text and value of the key
//  are allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65535), and up to 253 bytes
//  of key value.

//  Associated with each key is a value byte string
//	containing any value desired.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	The b-tree pages are linked with next
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup. The fence key for a leaf node is
//	always present

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse. It also
//	contains the latch manager hash table.

//	The ParentModification lock on a node is obtained to serialize posting
//	or changing the fence key for a node.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page
//	Page slots use 1 based indexing.

#define slotptr(page, slot) (((BtSlot *)(page + 1)) + (slot - 1))
#define keyptr(page, slot) \
  ((BtKey *)((char *)(page) + slotptr(page, slot)->off))
#define valptr(page, slot)                              \
  ((BtVal *)((unsigned char *)keyptr(page, slot)->key + \
             keyptr(page, slot)->len))

void bt_putid(unsigned char *dest, uid id);

uid bt_getid(unsigned char *src);

uid bt_newdup(BtDb *bt);

void bt_spinreleasewrite(BtSpinLatch *latch);
void bt_spinwritelock(BtSpinLatch *latch);

//  Spin Latch Manager
//  wait until write lock mode is clear
//  and add 1 to the share count
void bt_spinreadlock(BtSpinLatch *latch);

//  wait for other read and write latches to relinquish
void bt_spinwritelock(BtSpinLatch *latch);

//  try to obtain write lock
//  return 1 if obtained,
//    0 otherwise
int bt_spinwritetry(BtSpinLatch *latch);

//  clear write mode
void bt_spinreleasewrite(BtSpinLatch *latch);

//  decrement reader count
void bt_spinreleaseread(BtSpinLatch *latch);

//  read page from permanent location in Btree file
BTERR bt_readpage(BtMgr *mgr, BtPage page, uid page_no);

//  write page to permanent location in Btree file
//  clear the dirty bit
BTERR bt_writepage(BtMgr *mgr, BtPage page, uid page_no);

//  link latch table entry into head of latch hash table
BTERR bt_latchlink(BtDb *bt, uint hashidx, uint slot, uid page_no, uint loadit);

//  set CLOCK bit in latch
//  decrement pin count
void bt_unpinlatch(BtLatchSet *latch);

//  return the btree cached page address
BtPage bt_mappage(BtDb *bt, BtLatchSet *latch);

//  find existing latchset or inspire new one
//  return with latchset pinned
BtLatchSet *bt_pinlatch(BtDb *bt, uid page_no, uint loadit);

void bt_mgrclose(BtMgr *mgr);

//  close and release memory
void bt_close(BtDb *bt);

//  open/create new btree buffer manager

//  call with file_name, BT_openmode, bits in page size (e.g. 16),
//    size of page pool (e.g. 262144)
BtMgr *bt_mgr(char *name, uint bits, uint nodemax);

//  open BTree access method
//  based on buffer manager
BtDb *bt_open(BtMgr *mgr);

//  compare two keys, return > 0, = 0, or < 0
//  =0: keys are same
//  -1: key2 > key1
//  +1: key2 < key1
//  as the comparison value
int keycmp(BtKey *key1, char *key2, const catalog::Schema *key_schema);

int keycmp(BtKey *key1, char *key2, catalog::Schema *key_schema);

// place write, read, or parent lock on requested page_no.
void bt_lockpage(BtDb *bt, BtLock mode, BtLatchSet *latch);

// remove write, read, or parent lock on requested page
void bt_unlockpage(BtDb *bt, BtLock mode, BtLatchSet *latch);

//  allocate a new page
//  return with page latched, but unlocked.
int bt_newpage(BtDb *bt, BtPageSet *set, BtPage contents);

//  find slot in page for given key at a given level
int bt_findslot(BtPage page, char *key, uint len);

//  find and load page at given level for given key
//  leave page rd or wr locked as requested
int bt_loadpage(BtDb *bt, BtPageSet *set, char *key, uint lvl, BtLock lock);

//  return page to free list
//  page must be delete & write locked
void bt_freepage(BtDb *bt, BtPageSet *set);

//  a fence key was deleted from a page
//  push new fence value upwards
BTERR bt_fixfence(BtDb *bt, BtPageSet *set, uint lvl);

//  root has a single child
//  collapse a level from the tree
BTERR bt_collapseroot(BtDb *bt, BtPageSet *root);

//  delete a page and manage keys
//  call with page writelocked
//  returns with page unpinned
BTERR bt_deletepage(BtDb *bt, BtPageSet *set);

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree
BTERR bt_deletekey(BtDb *bt, char *key, uint lvl, uint unique);

BtKey *bt_foundkey(BtDb *bt);

//  advance to next slot
uint bt_findnext(BtDb *bt, BtPageSet *set, uint slot);

//  find unique key or first duplicate key in
//  leaf level and return number of value bytes
//  or (-1) if not found.  Setup key for bt_foundkey
int bt_findkey(BtDb *bt, char *key, uint keylen, unsigned char *value,
               uint valmax);

//  check page for space available,
//  clean if necessary and return
//  0 - page needs splitting
//  >0  new slot value
uint bt_cleanpage(BtDb *bt, BtPageSet *set, uint keylen, uint slot,
                  uint vallen);

// split the root and raise the height of the btree
BTERR bt_splitroot(BtDb *bt, BtPageSet *root, BtLatchSet *right);

//  split already locked full node
//  leave it locked.
//  return pool entry for new right
//  page, unlocked
uint bt_splitpage(BtDb *bt, BtPageSet *set);

//  fix keys for newly split page
//  call with page locked, return
//  unlocked
BTERR bt_splitkeys(BtDb *bt, BtPageSet *set, BtLatchSet *right, uint unique);

//  install new key and value onto page
//  page must already be checked for
//  adequate space
BTERR bt_insertslot(BtDb *bt, BtPageSet *set, uint slot, char *key, uint keylen,
                    unsigned char *value, uint vallen, uint type, uint release);

//  Insert new key into the btree at given level.
//  either add a new key or update/add an existing one
BTERR bt_insertkey(BtDb *bt, char *key, uint keylen, uint lvl, void *value,
                   uint vallen, uint unique);

typedef struct {
  uint entry;      // latch table entry number
  uint slot : 31;  // page slot number
  uint reuse : 1;  // reused previous page
} AtomicTxn;

typedef struct {
  uid page_no;        // page number for split leaf
  void *next;         // next key to insert
  uint entry : 29;    // latch table entry number
  uint type : 2;      // 0 == insert, 1 == delete, 2 == free
  uint nounlock : 1;  // don't unlock ParentModification
  char leafkey[BT_keyarray];
} AtomicKey;

//  determine actual page where key is located
//  return slot number
uint bt_atomicpage(BtDb *bt, BtPage source, AtomicTxn *locks, uint src,
                   BtPageSet *set);

BTERR bt_atomicinsert(BtDb *bt, BtPage source, AtomicTxn *locks, uint src);

BTERR bt_atomicdelete(BtDb *bt, BtPage source, AtomicTxn *locks, uint src);

//  delete an empty master page for a transaction

//  note that the far right page never empties because
//  it always contains (at least) the infinite stopper key
//  and that all pages that don't contain any keys are
//  deleted, or are being held under Atomic lock.
BTERR bt_atomicfree(BtDb *bt, BtPageSet *prev, uint unique);

//  atomic modification of a batch of keys.

//  return -1 if BTERR is set
//  otherwise return slot number
//  causing the key constraint violation
//  or zero on successful completion.
int bt_atomictxn(BtDb *bt, BtPage source, uint unique);

//  set cursor to highest slot on highest page
uint bt_lastkey(BtDb *bt);

//  return previous slot on cursor page
uint bt_prevkey(BtDb *bt, uint slot);

//  return next slot on cursor page
//  or slide cursor right into next page
uint bt_nextkey(BtDb *bt, uint slot);

//  cache page of keys into cursor and return starting slot for given key
uint bt_startkey(BtDb *bt, char *key, uint len);

BtKey *bt_key(BtDb *bt, uint slot);

BtVal *bt_val(BtDb *bt, uint slot);

}  // End index namespace
}  // End peloton namespace
