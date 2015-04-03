/*-------------------------------------------------------------------------
 *
 * tester.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/third_party/btree/tester.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "index/concurrent_btree.h"

#include <sstream>
#include <iostream>

#include "storage/tuple.h"

namespace nstore {
namespace index {

void bt_putid(unsigned char *dest, uid id)
{
  int i = BtId;

  while( i-- )
    dest[i] = (unsigned char)id, id >>= 8;
}

uid bt_getid(unsigned char *src)
{
  uid id = 0;
  int i;

  for( i = 0; i < BtId; i++ )
    id <<= 8, id |= *src++;

  return id;
}

uid bt_newdup (BtDb *bt)
{
#ifdef unix
  return __sync_fetch_and_add ((unsigned long long *) bt->mgr->pagezero->dups, 1) + 1;
#else
  return _InterlockedIncrement64(bt->mgr->pagezero->dups, 1);
#endif
}

void bt_spinreleasewrite(BtSpinLatch *latch);
void bt_spinwritelock(BtSpinLatch *latch);

//  Write-Only Reentrant Lock

void WriteOLock (WOLock *lock, ushort tid)
{
  while( 1 ) {
    bt_spinwritelock(lock->xcl);

    if( *lock->tid == tid ) {
      *lock->dup += 1;
      bt_spinreleasewrite(lock->xcl);
      return;
    }
    if( !*lock->tid ) {
      *lock->tid = tid;
      bt_spinreleasewrite(lock->xcl);
      return;
    }
    bt_spinreleasewrite(lock->xcl);
    sched_yield();
  }
}

void WriteORelease (WOLock *lock)
{
  if( *lock->dup ) {
    *lock->dup -= 1;
    return;
  }

  *lock->tid = 0;
}

//  Phase-Fair reader/writer lock implementation

void WriteLock (RWLock *lock)
{
  ushort w, r, tix;

#ifdef unix
  tix = __sync_fetch_and_add ((int *) (ushort *) lock->ticket, 1);
#else
  tix = _InterlockedExchangeAdd16 (lock->ticket, 1);
#endif
  // wait for our ticket to come up

  while( tix != lock->serving[0] )
#ifdef unix
    sched_yield();
#else
  SwitchToThread ();
#endif

  w = PRES | (tix & PHID);
#ifdef  unix
  r = __sync_fetch_and_add ((int *) (ushort *) lock->rin, w);
#else
  r = _InterlockedExchangeAdd16 (lock->rin, w);
#endif
  while( r != *lock->rout )
#ifdef unix
    sched_yield();
#else
  SwitchToThread();
#endif
}

void WriteRelease (RWLock *lock)
{
#ifdef unix
  __sync_fetch_and_and ((int *) (ushort *)lock->rin, ~MASK);
#else
  _InterlockedAnd16 (lock->rin, ~MASK);
#endif
  lock->serving[0]++;
}

void ReadLock (RWLock *lock)
{
  ushort w;
#ifdef unix
  w = __sync_fetch_and_add ((int *) (ushort *) lock->rin, RINC) & MASK;
#else
  w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
#endif
  if( w )
    while( w == (*lock->rin & MASK) )
#ifdef unix
      sched_yield ();
#else
  SwitchToThread ();
#endif
}

void ReadRelease (RWLock *lock)
{
#ifdef unix
  __sync_fetch_and_add ((int *) (ushort *) lock->rout, RINC);
#else
  _InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}

//  Spin Latch Manager

//  wait until write lock mode is clear
//  and add 1 to the share count

void bt_spinreadlock(BtSpinLatch *latch)
{
  ushort prev;

  do {
#ifdef unix
    prev = __sync_fetch_and_add ((int *) (ushort *)latch, SHARE);
#else
    prev = _InterlockedExchangeAdd16((ushort *)latch, SHARE);
#endif
    //  see if exclusive request is granted or pending

    if( !(prev & BOTH) )
      return;
#ifdef unix
    prev = __sync_fetch_and_add ((int *) (ushort *)latch, -SHARE);
#else
    prev = _InterlockedExchangeAdd16((ushort *)latch, -SHARE);
#endif
#ifdef  unix
  } while( sched_yield(), 1 );
#else
} while( SwitchToThread(), 1 );
#endif
}

//  wait for other read and write latches to relinquish

void bt_spinwritelock(BtSpinLatch *latch)
{
  ushort prev;

  do {

#ifdef  unix
    prev = __sync_fetch_and_or((int *) (ushort *)latch, PEND | XCL);
#else
    prev = _InterlockedOr16((ushort *)latch, PEND | XCL);
#endif
    if( !(prev & XCL) ) {
      if( !(prev & ~BOTH) )
        return;
      else
#ifdef unix
        __sync_fetch_and_and ((int *) (ushort *)latch, ~XCL);
#else
      _InterlockedAnd16((ushort *)latch, ~XCL);
#endif
    }
#ifdef  unix
  } while( sched_yield(), 1 );
#else
} while( SwitchToThread(), 1 );
#endif
}

//  try to obtain write lock

//  return 1 if obtained,
//    0 otherwise

int bt_spinwritetry(BtSpinLatch *latch)
{
  ushort prev;

#ifdef  unix
  prev = __sync_fetch_and_or((int *) (ushort *)latch, XCL);
#else
  prev = _InterlockedOr16((ushort *)latch, XCL);
#endif
  //  take write access if all bits are clear

  if( !(prev & XCL) ) {
    if( !(prev & ~BOTH) )
      return 1;
    else
#ifdef unix
      __sync_fetch_and_and ((int *) (ushort *)latch, ~XCL);
#else
    _InterlockedAnd16((ushort *)latch, ~XCL);
#endif
  }

  return 0;
}

//  clear write mode

void bt_spinreleasewrite(BtSpinLatch *latch)
{
#ifdef unix
  __sync_fetch_and_and((int *) (ushort *)latch, ~BOTH);
#else
  _InterlockedAnd16((ushort *)latch, ~BOTH);
#endif
}

//  decrement reader count

void bt_spinreleaseread(BtSpinLatch *latch)
{
#ifdef unix
  __sync_fetch_and_add((int *) (ushort *)latch, -SHARE);
#else
  _InterlockedExchangeAdd16((ushort *)latch, -SHARE);
#endif
}

//  read page from permanent location in Btree file

BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no)
{
#ifdef unix
  if( pread (mgr->idx, page, mgr->page_size, page_no << mgr->page_bits) < mgr->page_size ) {
    fprintf (stderr, "Unable to read page %.8llu errno = %d\n", page_no, errno);
    return BTERR_read;
  }
#else
  off64_t off = page_no << mgr->page_bits;

  OVERLAPPED ovl[1];
  uint amt[1];

  memset (ovl, 0, sizeof(OVERLAPPED));
  ovl->Offset = off;
  ovl->OffsetHigh = off >> 32;

  if( !ReadFile(mgr->idx, page, mgr->page_size, amt, ovl)) {
    fprintf (stderr, "Unable to read page %.8x GetLastError = %d\n", page_no, GetLastError());
    return BTERR_read;
  }
  if( *amt <  mgr->page_size ) {
    fprintf (stderr, "Unable to read page %.8x GetLastError = %d\n", page_no, GetLastError());
    return BTERR_read;
  }
#endif
  return BTERR_ok;
}

//  write page to permanent location in Btree file
//  clear the dirty bit

BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no)
{
  off64_t off = page_no << mgr->page_bits;

#ifdef unix
  if( pwrite(mgr->idx, page, mgr->page_size, off) < mgr->page_size )
    return BTERR_wrt;
#else
  OVERLAPPED ovl[1];
  uint amt[1];

  memset (ovl, 0, sizeof(OVERLAPPED));
  ovl->Offset = off;
  ovl->OffsetHigh = off >> 32;

  if( !WriteFile(mgr->idx, page, mgr->page_size, amt, ovl) )
    return BTERR_wrt;

  if( *amt <  mgr->page_size )
    return BTERR_wrt;
#endif
  return BTERR_ok;
}

//  link latch table entry into head of latch hash table

BTERR bt_latchlink (BtDb *bt, uint hashidx, uint slot, uid page_no, uint loadit)
{
  BtPage page = (BtPage)(((uid)slot << bt->mgr->page_bits) + bt->mgr->pagepool);
  BtLatchSet *latch = bt->mgr->latchsets + slot;

  if( (latch->next = bt->mgr->hashtable[hashidx].slot) )
    bt->mgr->latchsets[latch->next].prev = slot;

  bt->mgr->hashtable[hashidx].slot = slot;
  latch->page_no = page_no;
  latch->entry = slot;
  latch->split = 0;
  latch->prev = 0;
  latch->pin = 1;

  if( loadit ) {
    if( (bt->err = bt_readpage (bt->mgr, page, page_no)) )
      return (BTERR) bt->err;
    else
      bt->reads++;
  }

  bt->err = 0;
  return BTERR_ok;
}

//  set CLOCK bit in latch
//  decrement pin count

void bt_unpinlatch (BtLatchSet *latch)
{
#ifdef unix
  if( ~latch->pin & CLOCK_bit )
    __sync_fetch_and_or((int *) &latch->pin, CLOCK_bit);
  __sync_fetch_and_add((int *) &latch->pin, -1);
#else
  if( ~latch->pin & CLOCK_bit )
    _InterlockedOr16 (&latch->pin, CLOCK_bit);
  _InterlockedDecrement16 (&latch->pin);
#endif
}

//  return the btree cached page address

BtPage bt_mappage (BtDb *bt, BtLatchSet *latch)
{
  BtPage page = (BtPage)(((uid)latch->entry << bt->mgr->page_bits) + bt->mgr->pagepool);

  return page;
}

//  find existing latchset or inspire new one
//  return with latchset pinned

BtLatchSet *bt_pinlatch (BtDb *bt, uid page_no, uint loadit)
{
  uint hashidx = page_no % bt->mgr->latchhash;
  BtLatchSet *latch;
  uint slot, idx;
  BtPage page;

  //  try to find our entry

  bt_spinwritelock(bt->mgr->hashtable[hashidx].latch);

  if( (slot = bt->mgr->hashtable[hashidx].slot ) ) {
    do
    {
      latch = bt->mgr->latchsets + slot;
      if( page_no == latch->page_no )
        break;
    } while( (slot = latch->next) );
  }

  //  found our entry
  //  increment clock

  if( slot ) {
    latch = bt->mgr->latchsets + slot;
#ifdef unix
    __sync_fetch_and_add((int *) &latch->pin, 1);
#else
    _InterlockedIncrement16 (&latch->pin);
#endif
    bt_spinreleasewrite(bt->mgr->hashtable[hashidx].latch);
    return latch;
  }

  //  see if there are any unused pool entries
#ifdef unix
  slot = __sync_fetch_and_add (&bt->mgr->latchdeployed, 1) + 1;
#else
  slot = _InterlockedIncrement (&bt->mgr->latchdeployed);
#endif

  if( slot < bt->mgr->latchtotal ) {
    latch = bt->mgr->latchsets + slot;
    if( bt_latchlink (bt, hashidx, slot, page_no, loadit) )
      return NULL;
    bt_spinreleasewrite (bt->mgr->hashtable[hashidx].latch);
    return latch;
  }

#ifdef unix
  __sync_fetch_and_add (&bt->mgr->latchdeployed, -1);
#else
  _InterlockedDecrement (&bt->mgr->latchdeployed);
#endif
  //  find and reuse previous entry on victim

  while( 1 ) {
#ifdef unix
    slot = __sync_fetch_and_add(&bt->mgr->latchvictim, 1);
#else
    slot = _InterlockedIncrement (&bt->mgr->latchvictim) - 1;
#endif
    // try to get write lock on hash chain
    //  skip entry if not obtained
    //  or has outstanding pins

    slot %= bt->mgr->latchtotal;

    if( !slot )
      continue;

    latch = bt->mgr->latchsets + slot;
    idx = latch->page_no % bt->mgr->latchhash;

    //  see we are on same chain as hashidx

    if( idx == hashidx )
      continue;

    if( !bt_spinwritetry (bt->mgr->hashtable[idx].latch) )
      continue;

    //  skip this slot if it is pinned
    //  or the CLOCK bit is set

    if( latch->pin ) {
      if( latch->pin & CLOCK_bit ) {
#ifdef unix
        __sync_fetch_and_and((int *) &latch->pin, ~CLOCK_bit);
#else
        _InterlockedAnd16 (&latch->pin, ~CLOCK_bit);
#endif
      }
      bt_spinreleasewrite (bt->mgr->hashtable[idx].latch);
      continue;
    }

    //  update permanent page area in btree from buffer pool

    page = (BtPage)(((uid)slot << bt->mgr->page_bits) + bt->mgr->pagepool);

    if( latch->dirty ) {
      if(( bt->err = bt_writepage (bt->mgr, page, latch->page_no)) )
        return NULL;
      else
        latch->dirty = 0, bt->writes++;
    }

    //  unlink our available slot from its hash chain

    if( latch->prev )
      bt->mgr->latchsets[latch->prev].next = latch->next;
    else
      bt->mgr->hashtable[idx].slot = latch->next;

    if( latch->next )
      bt->mgr->latchsets[latch->next].prev = latch->prev;

    bt_spinreleasewrite (bt->mgr->hashtable[idx].latch);

    if( bt_latchlink (bt, hashidx, slot, page_no, loadit) )
      return NULL;

    bt_spinreleasewrite (bt->mgr->hashtable[hashidx].latch);
    return latch;
  }
}

void bt_mgrclose (BtMgr *mgr)
{
  BtLatchSet *latch;
  uint num = 0;
  BtPage page;
  uint slot;

  //  flush dirty pool pages to the btree

  for( slot = 1; slot <= mgr->latchdeployed; slot++ ) {
    page = (BtPage)(((uid)slot << mgr->page_bits) + mgr->pagepool);
    latch = mgr->latchsets + slot;

    if( latch->dirty ) {
      bt_writepage(mgr, page, latch->page_no);
      latch->dirty = 0, num++;
    }
    //    madvise (page, mgr->page_size, MADV_DONTNEED);
  }

  fprintf(stderr, "%d buffer pool pages flushed\n", num);

#ifdef unix
  munmap (mgr->hashtable, (uid)mgr->nlatchpage << mgr->page_bits);
  munmap (mgr->pagezero, mgr->page_size);
#else
  FlushViewOfFile(mgr->pagezero, 0);
  UnmapViewOfFile(mgr->pagezero);
  UnmapViewOfFile(mgr->hashtable);
  CloseHandle(mgr->halloc);
  CloseHandle(mgr->hpool);
#endif
#ifdef unix
  close (mgr->idx);
  free (mgr);
#else
  FlushFileBuffers(mgr->idx);
  CloseHandle(mgr->idx);
  GlobalFree (mgr);
#endif
}

//  close and release memory

void bt_close (BtDb *bt)
{
#ifdef unix
  if( bt->mem )
    free (bt->mem);
#else
  if( bt->mem)
    VirtualFree (bt->mem, 0, MEM_RELEASE);
#endif
  free (bt);
}

//  open/create new btree buffer manager

//  call with file_name, BT_openmode, bits in page size (e.g. 16),
//    size of page pool (e.g. 262144)

BtMgr *bt_mgr (char *name, uint bits, uint nodemax)
{
  uint lvl;
  unsigned char value[BtId];
  int flag, initit = 0;
  BtPageZero *pagezero;
  off64_t size;
  uint amt[1];
  BtMgr* mgr;
  BtKey* key;
  BtVal *val;

  // determine sanity of page size and buffer pool

  if( bits > BT_maxbits )
    bits = BT_maxbits;
  else if( bits < BT_minbits )
    bits = BT_minbits;

  if( nodemax < 16 ) {
    fprintf(stderr, "Buffer pool too small: %d\n", nodemax);
    return (BtMgr *)NULL;
  }

#ifdef unix
  mgr = (BtMgr *) calloc (1, sizeof(BtMgr));

  mgr->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);

  if( mgr->idx == -1 ) {
    fprintf (stderr, "Unable to open btree file\n");
    return free(mgr), (BtMgr *)NULL;
  }
#else
  mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtMgr));
  attr = FILE_ATTRIBUTE_NORMAL;
  mgr->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS, attr, NULL);

  if( mgr->idx == INVALID_HANDLE_VALUE )
    return GlobalFree(mgr), nullptr;
#endif

#ifdef unix
  pagezero = (BtPageZero *) valloc (BT_maxpage);
  *amt = 0;

  // read minimum page size to get root info
  //  to support raw disk partition files
  //  check if bits == 0 on the disk.

  if( (size = lseek (mgr->idx, 0L, 2)) )
    if( pread(mgr->idx, pagezero, BT_minpage, 0) == BT_minpage )
      if( pagezero->alloc->bits )
        bits = pagezero->alloc->bits;
      else
        initit = 1;
    else
      return free(mgr), free(pagezero), (BtMgr *)NULL;
  else
    initit = 1;
#else
  pagezero = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
  size = GetFileSize(mgr->idx, amt);

  if( size || *amt ) {
    if( !ReadFile(mgr->idx, (char *)pagezero, BT_minpage, amt, NULL) )
      return bt_mgrclose (mgr), NULL;
    bits = pagezero->alloc->bits;
  } else
    initit = 1;
#endif

  mgr->page_size = 1 << bits;
  mgr->page_bits = bits;

  //  calculate number of latch hash table entries

  mgr->nlatchpage = (nodemax/16 * sizeof(BtHashEntry) + mgr->page_size - 1) / mgr->page_size;
  mgr->latchhash = ((uid)mgr->nlatchpage << mgr->page_bits) / sizeof(BtHashEntry);

  mgr->nlatchpage += nodemax;   // size of the buffer pool in pages
  mgr->nlatchpage += (sizeof(BtLatchSet) * nodemax + mgr->page_size - 1)/mgr->page_size;
  mgr->latchtotal = nodemax;

  if( !initit )
    goto mgrlatch;

  // initialize an empty b-tree with latch page, root page, page of leaves
  // and page(s) of latches and page pool cache

  memset (pagezero, 0, 1 << bits);
  pagezero->alloc->bits = mgr->page_bits;
  bt_putid(pagezero->alloc->right, MIN_lvl+1);

  //  initialize left-most LEAF page in
  //  alloc->left.

  bt_putid (pagezero->alloc->left, LEAF_page);

  if( bt_writepage (mgr, pagezero->alloc, 0) ) {
    fprintf (stderr, "Unable to create btree page zero\n");
    return bt_mgrclose (mgr), (BtMgr *)NULL;
  }

  memset (pagezero, 0, 1 << bits);
  pagezero->alloc->bits = mgr->page_bits;

  for( lvl=MIN_lvl; lvl--; ) {
    slotptr(pagezero->alloc, 1)->off = mgr->page_size - 3 - (lvl ? BtId + sizeof(BtVal): sizeof(BtVal));
    key = keyptr(pagezero->alloc, 1);
    key->len = 2;   // create stopper key
    key->key[0] = 0xff;
    key->key[1] = 0xff;

    bt_putid(value, MIN_lvl - lvl + 1);
    val = valptr(pagezero->alloc, 1);
    val->len = lvl ? BtId : 0;
    memcpy (val->value, value, val->len);

    pagezero->alloc->min = slotptr(pagezero->alloc, 1)->off;
    pagezero->alloc->lvl = lvl;
    pagezero->alloc->cnt = 1;
    pagezero->alloc->act = 1;

    if( bt_writepage (mgr, pagezero->alloc, MIN_lvl - lvl) ) {
      fprintf (stderr, "Unable to create btree page zero\n");
      return bt_mgrclose (mgr), (BtMgr *)NULL;
    }
  }

  mgrlatch:
#ifdef unix
  free (pagezero);
#else
  VirtualFree (pagezero, 0, MEM_RELEASE);
#endif
#ifdef unix
  // mlock the pagezero page

  flag = PROT_READ | PROT_WRITE;
  mgr->pagezero = (BtPageZero*) mmap (0, mgr->page_size, flag, MAP_SHARED, mgr->idx, ALLOC_page << mgr->page_bits);
  if( mgr->pagezero == MAP_FAILED ) {
    fprintf (stderr, "Unable to mmap btree page zero, error = %s\n", strerror(errno));
    return bt_mgrclose (mgr), (BtMgr *)NULL;
  }
  mlock (mgr->pagezero, mgr->page_size);

  mgr->hashtable = (BtHashEntry *) mmap (0, (uid)mgr->nlatchpage << mgr->page_bits, flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
  if( mgr->hashtable == MAP_FAILED ) {
    fprintf (stderr, "Unable to mmap anonymous buffer pool pages, error = %s\n", strerror(errno));
    return bt_mgrclose (mgr), (BtMgr *)NULL;
  }
#else
  flag = PAGE_READWRITE;
  mgr->halloc = CreateFileMapping(mgr->idx, NULL, flag, 0, mgr->page_size, NULL);
  if( !mgr->halloc ) {
    fprintf (stderr, "Unable to create page zero memory mapping, error = %d\n", GetLastError());
    return bt_mgrclose (mgr), NULL;
  }

  flag = FILE_MAP_WRITE;
  mgr->pagezero = MapViewOfFile(mgr->halloc, flag, 0, 0, mgr->page_size);
  if( !mgr->pagezero ) {
    fprintf (stderr, "Unable to map page zero, error = %d\n", GetLastError());
    return bt_mgrclose (mgr), NULL;
  }

  flag = PAGE_READWRITE;
  size = (uid)mgr->nlatchpage << mgr->page_bits;
  mgr->hpool = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, flag, size >> 32, size, NULL);
  if( !mgr->hpool ) {
    fprintf (stderr, "Unable to create buffer pool memory mapping, error = %d\n", GetLastError());
    return bt_mgrclose (mgr), NULL;
  }

  flag = FILE_MAP_WRITE;
  mgr->hashtable = MapViewOfFile(mgr->pool, flag, 0, 0, size);
  if( !mgr->hashtable ) {
    fprintf (stderr, "Unable to map buffer pool, error = %d\n", GetLastError());
    return bt_mgrclose (mgr), NULL;
  }
#endif

  mgr->pagepool = (unsigned char *)mgr->hashtable + ((uid)(mgr->nlatchpage - mgr->latchtotal) << mgr->page_bits);
  mgr->latchsets = (BtLatchSet *)(mgr->pagepool - (uid)mgr->latchtotal * sizeof(BtLatchSet));

  return mgr;
}

//  open BTree access method
//  based on buffer manager

BtDb *bt_open (BtMgr *mgr)
{
  BtDb *bt = (BtDb *) malloc (sizeof(*bt));

  memset (bt, 0, sizeof(*bt));
  bt->mgr = mgr;
#ifdef unix
  bt->mem = (unsigned char *) valloc (2 *mgr->page_size);
#else
  bt->mem = VirtualAlloc(NULL, 2 * mgr->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
  bt->frame = (BtPage)bt->mem;
  bt->cursor = (BtPage)(bt->mem + 1 * mgr->page_size);
#ifdef unix
  bt->thread_no = __sync_fetch_and_add ((int *) (ushort *) mgr->thread_no, 1) + 1;
#else
  bt->thread_no = _InterlockedIncrement16(mgr->thread_no, 1);
#endif
  return bt;
}

//  compare two keys, return > 0, = 0, or < 0
//  =0: keys are same
//  -1: key2 > key1
//  +1: key2 < key1
//  as the comparison value

int keycmp (BtKey* key1, char *key2, catalog::Schema *key_schema)
{
  storage::Tuple lhs_tuple(key_schema, key1->key);
  storage::Tuple rhs_tuple(key_schema, key2);

  int compare = lhs_tuple.Compare(rhs_tuple);

  //std::cout << "LHS :: " << lhs_tuple;
  //std::cout << "RHS :: " << rhs_tuple;
  //std::cout << "Compare :: " << compare << "\n";

  lhs_tuple.Move(nullptr);
  rhs_tuple.Move(nullptr);

  return compare;
}

// place write, read, or parent lock on requested page_no.

void bt_lockpage(BtDb *bt, BtLock mode, BtLatchSet *latch)
{
  switch( mode ) {
    case BtLockRead:
      ReadLock (latch->readwr);
      break;
    case BtLockWrite:
      WriteLock (latch->readwr);
      break;
    case BtLockAccess:
      ReadLock (latch->access);
      break;
    case BtLockDelete:
      WriteLock (latch->access);
      break;
    case BtLockParent:
      WriteOLock (latch->parent, bt->thread_no);
      break;
    case BtLockAtomic:
      WriteOLock (latch->atomic, bt->thread_no);
      break;
    case BtLockAtomic | BtLockRead:
    WriteOLock (latch->atomic, bt->thread_no);
    ReadLock (latch->readwr);
    break;
  }
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(__attribute__((unused)) BtDb *bt, BtLock mode, BtLatchSet *latch)
{
  switch( mode ) {
    case BtLockRead:
      ReadRelease (latch->readwr);
      break;
    case BtLockWrite:
      WriteRelease (latch->readwr);
      break;
    case BtLockAccess:
      ReadRelease (latch->access);
      break;
    case BtLockDelete:
      WriteRelease (latch->access);
      break;
    case BtLockParent:
      WriteORelease (latch->parent);
      break;
    case BtLockAtomic:
      WriteORelease (latch->atomic);
      break;
    case BtLockAtomic | BtLockRead:
    WriteORelease (latch->atomic);
    ReadRelease (latch->readwr);
    break;
  }
}

//  allocate a new page
//  return with page latched, but unlocked.

int bt_newpage(BtDb *bt, BtPageSet *set, BtPage contents)
{
  uid page_no;

  //  lock allocation page

  bt_spinwritelock(bt->mgr->lock);

  // use empty chain first
  // else allocate empty page

  if( (page_no = bt_getid(bt->mgr->pagezero->chain) ) ) {
    if( (set->latch = bt_pinlatch (bt, page_no, 1) ) )
      set->page = bt_mappage (bt, set->latch);
    else
      return bt->err = BTERR_struct, -1;

    bt_putid(bt->mgr->pagezero->chain, bt_getid(set->page->right));
    bt_spinreleasewrite(bt->mgr->lock);

    memcpy (set->page, contents, bt->mgr->page_size);
    set->latch->dirty = 1;
    return 0;
  }

  page_no = bt_getid(bt->mgr->pagezero->alloc->right);
  bt_putid(bt->mgr->pagezero->alloc->right, page_no+1);

  // unlock allocation latch

  bt_spinreleasewrite(bt->mgr->lock);

  //  don't load cache from btree page

  if( (set->latch = bt_pinlatch (bt, page_no, 0) ) )
    set->page = bt_mappage (bt, set->latch);
  else
    return bt->err = BTERR_struct;

  memcpy (set->page, contents, bt->mgr->page_size);
  set->latch->dirty = 1;
  return 0;
}

//  find slot in page for given key at a given level

int bt_findslot (BtPage page, char *key, catalog::Schema *key_schema)
{
  uint diff, higher = page->cnt, low = 1, slot;
  uint good = 0;

  //    make stopper key an infinite fence value

  if( bt_getid (page->right) )
    higher++;
  else
    good++;

  //  low is the lowest candidate.
  //  loop ends when they meet

  //  higher is already
  //  tested as .ge. the passed key.

  while( (diff = higher - low ) ) {
    slot = low + ( diff >> 1 );
    if( keycmp (keyptr(page, slot), key, key_schema) < 0 )
      low = slot + 1;
    else
      higher = slot, good++;
  }

  //  return zero if key is on right link page

  return good ? higher : 0;
}

//  find and load page at given level for given key
//  leave page rd or wr locked as requested

int bt_loadpage (BtDb *bt, BtPageSet *set, char *key, uint lvl, BtLock lock)
{
  uid page_no = ROOT_page, prevpage = 0;
  uint drill = 0xff, slot;
  BtLatchSet *prevlatch;
  uint mode, prevmode;

  //  start at root of btree and drill down

  do {
    // determine lock mode of drill level
    mode = (drill == lvl) ? lock : BtLockRead;

    if( !(set->latch = bt_pinlatch (bt, page_no, 1)) )
      return 0;

    // obtain access lock using lock chaining with Access mode

    if( page_no > ROOT_page )
      bt_lockpage(bt, BtLockAccess, set->latch);

    set->page = bt_mappage (bt, set->latch);

    //  release & unpin parent or left sibling page

    if( prevpage ) {
      bt_unlockpage(bt, (BtLock) prevmode, prevlatch);
      bt_unpinlatch (prevlatch);
      prevpage = 0;
    }

    // obtain mode lock using lock chaining through AccessLock

    bt_lockpage(bt, (BtLock) mode, set->latch);

    if( set->page->free )
      return bt->err = BTERR_struct, 0;

    if( page_no > ROOT_page )
      bt_unlockpage(bt, BtLockAccess, set->latch);

    // re-read and re-lock root after determining actual level of root

    if( set->page->lvl != drill) {
      if( set->latch->page_no != ROOT_page )
        return bt->err = BTERR_struct, 0;

      drill = set->page->lvl;

      if( lock != BtLockRead && drill == lvl ) {
        bt_unlockpage(bt, (BtLock) mode, set->latch);
        bt_unpinlatch (set->latch);
        continue;
      }
    }

    prevpage = set->latch->page_no;
    prevlatch = set->latch;
    prevmode = mode;

    //  find key on page at this level
    //  and descend to requested level

    if( !set->page->kill )
      if( (slot = bt_findslot (set->page, key, bt->key_schema)) ) {
        if( drill == lvl )
          return slot;

        // find next non-dead slot -- the fence key if nothing else

        while( slotptr(set->page, slot)->dead )
          if( slot++ < set->page->cnt )
            continue;
          else
            return bt->err = BTERR_struct, 0;

        page_no = bt_getid(valptr(set->page, slot)->value);
        drill--;
        continue;
      }

    //  or slide right into next page

    page_no = bt_getid(set->page->right);
  } while( page_no );

  // return error on end of right chain

  bt->err = BTERR_struct;
  return 0; // return error
}

//  return page to free list
//  page must be delete & write locked

void bt_freepage (BtDb *bt, BtPageSet *set)
{
  //  lock allocation page

  bt_spinwritelock (bt->mgr->lock);

  //  store chain

  memcpy(set->page->right, bt->mgr->pagezero->chain, BtId);
  bt_putid(bt->mgr->pagezero->chain, set->latch->page_no);
  set->latch->dirty = 1;
  set->page->free = 1;

  // unlock released page

  bt_unlockpage (bt, BtLockDelete, set->latch);
  bt_unlockpage (bt, BtLockWrite, set->latch);
  bt_unpinlatch (set->latch);

  // unlock allocation page

  bt_spinreleasewrite (bt->mgr->lock);
}

//  a fence key was deleted from a page
//  push new fence value upwards

BTERR bt_fixfence (BtDb *bt, BtPageSet *set, uint lvl, uint unique)
{
  char leftkey[BT_keyarray], rightkey[BT_keyarray];
  unsigned char value[BtId];
  BtKey* ptr;

  //  remove the old fence value

  ptr = keyptr(set->page, set->page->cnt);
  memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));
  memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
  set->latch->dirty = 1;

  //  cache new fence value

  ptr = keyptr(set->page, set->page->cnt);
  memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

  bt_lockpage (bt, BtLockParent, set->latch);
  bt_unlockpage (bt, BtLockWrite, set->latch);

  //  insert new (now smaller) fence key

  bt_putid (value, set->latch->page_no);
  ptr = (BtKey*)leftkey;

  if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
    return (BTERR) bt->err;

  //  now delete old fence key

  ptr = (BtKey*)rightkey;

  if( bt_deletekey (bt, ptr->key, lvl+1, unique) )
    return (BTERR) bt->err;

  bt_unlockpage (bt, BtLockParent, set->latch);
  bt_unpinlatch(set->latch);
  return BTERR_ok;
}

//  root has a single child
//  collapse a level from the tree

BTERR bt_collapseroot (BtDb *bt, BtPageSet *root)
{
  BtPageSet child[1];
  uid page_no;
  uint idx;

  // find the child entry and promote as new root contents

  do {
    for( idx = 0; idx++ < root->page->cnt; )
      if( !slotptr(root->page, idx)->dead )
        break;

    page_no = bt_getid (valptr(root->page, idx)->value);

    if( (child->latch = bt_pinlatch (bt, page_no, 1)) )
      child->page = bt_mappage (bt, child->latch);
    else
      return (BTERR)bt->err;

    bt_lockpage (bt, BtLockDelete, child->latch);
    bt_lockpage (bt, BtLockWrite, child->latch);

    memcpy (root->page, child->page, bt->mgr->page_size);
    root->latch->dirty = 1;

    bt_freepage (bt, child);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (bt, BtLockWrite, root->latch);
  bt_unpinlatch (root->latch);
  return BTERR_ok;
}

//  delete a page and manage keys
//  call with page writelocked
//  returns with page unpinned

BTERR bt_deletepage (BtDb *bt, BtPageSet *set, uint unique)
{
  char lowerfence[BT_keyarray], higherfence[BT_keyarray];
  unsigned char value[BtId];
  uint lvl = set->page->lvl;
  BtPageSet right[1];
  uid page_no;
  BtKey *ptr;

  //  cache copy of fence key
  //  to post in parent

  ptr = keyptr(set->page, set->page->cnt);
  memcpy (lowerfence, ptr, ptr->len + sizeof(BtKey));

  //  obtain lock on right page

  page_no = bt_getid(set->page->right);

  if( (right->latch = bt_pinlatch (bt, page_no, 1) ) )
    right->page = bt_mappage (bt, right->latch);
  else
    return BTERR_ok;

  bt_lockpage (bt, BtLockWrite, right->latch);

  // cache copy of key to update

  ptr = keyptr(right->page, right->page->cnt);
  memcpy (higherfence, ptr, ptr->len + sizeof(BtKey));

  if( right->page->kill )
    return (BTERR)(bt->err = BTERR_struct);

  // pull contents of right peer into our empty page

  memcpy (set->page, right->page, bt->mgr->page_size);
  set->latch->dirty = 1;

  // mark right page deleted and point it to left page
  //  until we can post parent updates that remove access
  //  to the deleted page.

  bt_putid (right->page->right, set->latch->page_no);
  right->latch->dirty = 1;
  right->page->kill = 1;

  bt_lockpage (bt, BtLockParent, right->latch);
  bt_unlockpage (bt, BtLockWrite, right->latch);

  bt_lockpage (bt, BtLockParent, set->latch);
  bt_unlockpage (bt, BtLockWrite, set->latch);

  // redirect higher key directly to our new node contents

  bt_putid (value, set->latch->page_no);
  ptr = (BtKey*)higherfence;

  if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
    return (BTERR)bt->err;

  //  delete old lower key to our node

  ptr = (BtKey*)lowerfence;

  if( bt_deletekey (bt, ptr->key, lvl+1, unique) )
    return (BTERR)bt->err;

  //  obtain delete and write locks to right node

  bt_unlockpage (bt, BtLockParent, right->latch);
  bt_lockpage (bt, BtLockDelete, right->latch);
  bt_lockpage (bt, BtLockWrite, right->latch);
  bt_freepage (bt, right);

  bt_unlockpage (bt, BtLockParent, set->latch);
  bt_unpinlatch (set->latch);
  return BTERR_ok;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtDb *bt, char *key, uint lvl, uint unique)
{
  uint slot, idx, found, fence;
  BtPageSet set[1];
  BtKey *ptr;
  BtVal *val;

  if(( slot = bt_loadpage (bt, set, key, lvl, BtLockWrite)) )
    ptr = keyptr(set->page, slot);
  else
    return (BTERR)bt->err;

  // if librarian slot, advance to real slot

  if( slotptr(set->page, slot)->type == Librarian )
    ptr = keyptr(set->page, ++slot);

  fence = slot == set->page->cnt;

  // if key is found delete it, otherwise ignore request

  while( (found = !keycmp (ptr, key, bt->key_schema) ) ) {
    //printf("slot %d slot status %d  \n", slot, slotptr(set->page, slot)->dead);

    if( (found = slotptr(set->page, slot)->dead == 0 ) ) {
      //printf("key found.. deleting  \n");

      val = valptr(set->page,slot);
      slotptr(set->page, slot)->dead = 1;
      set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
      set->page->act--;

      // collapse empty slots beneath the fence

      while( (idx = set->page->cnt - 1 ) ) {
        if( slotptr(set->page, idx)->dead ) {
          *slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
          memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
        }
        else {
          break;
        }
      }
    }

    // FIXME: deleting all matching keys in page, not across pages
    if(unique || slot++ == set->page->cnt)
      break;

    ptr = keyptr(set->page, slot);
  }

  //  did we delete a fence key in an upper level?

  if( found && lvl && set->page->act && fence ) {
    if( bt_fixfence (bt, set, lvl, unique) )
      return (BTERR)bt->err;
    else
      return BTERR_ok;
  }

  //  do we need to collapse root?

  if( lvl > 1 && set->latch->page_no == ROOT_page && set->page->act == 1 ) {
    if( bt_collapseroot (bt, set) )
      return (BTERR)bt->err;
    else
      return BTERR_ok;
  }

  //  delete empty page

  if( !set->page->act )
    return bt_deletepage (bt, set, unique);

  set->latch->dirty = 1;
  bt_unlockpage(bt, BtLockWrite, set->latch);
  bt_unpinlatch (set->latch);

  return BTERR_ok;
}

BtKey *bt_foundkey (BtDb *bt)
{
  return (BtKey*)(bt->key);
}

//  advance to next slot

uint bt_findnext (BtDb *bt, BtPageSet *set, uint slot)
{
  BtLatchSet *prevlatch;
  uid page_no;

  if( slot < set->page->cnt )
    return slot + 1;

  prevlatch = set->latch;

  if( (page_no = bt_getid(set->page->right) ) )
    if( (set->latch = bt_pinlatch (bt, page_no, 1) ) )
      set->page = bt_mappage (bt, set->latch);
    else
      return 0;
  else
    return bt->err = BTERR_struct, 0;

  // obtain access lock using lock chaining with Access mode

  bt_lockpage(bt, BtLockAccess, set->latch);

  bt_unlockpage(bt, BtLockRead, prevlatch);
  bt_unpinlatch (prevlatch);

  bt_lockpage(bt, BtLockRead, set->latch);
  bt_unlockpage(bt, BtLockAccess, set->latch);
  return 1;
}

//  find unique key or first duplicate key in
//  leaf level and return number of value bytes
//  or (-1) if not found.  Setup key for bt_foundkey

int bt_findkey (BtDb *bt, char *key, uint keylen, unsigned char *value, uint valmax)
{
  BtPageSet set[1];
  uint len, slot;
  int ret = -1;
  BtKey *ptr;
  BtVal *val;

  if( (slot = bt_loadpage (bt, set, key, 0, BtLockRead)) )
    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if( slotptr(set->page, slot)->type == Librarian )
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy (bt->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if( slotptr(set->page, slot)->type == Duplicate )
        len -= BtId;

      //  not there if we reach the stopper key

      if( slot == set->page->cnt )
        if( !bt_getid (set->page->right) )
          break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if( slotptr(set->page, slot)->dead )
        continue;

      if( keylen == len ) {
        int compare = memcmp (ptr->key, key, len);

        if( !compare ) {
          val = valptr (set->page,slot);
          if( valmax > val->len )
            valmax = val->len;
          memcpy (value, val->value, valmax);
          ret = valmax;
        }
      }

      break;

    } while( (slot = bt_findnext (bt, set, slot) ));

  bt_unlockpage (bt, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);
  return ret;
}

//  check page for space available,
//  clean if necessary and return
//  0 - page needs splitting
//  >0  new slot value

uint bt_cleanpage(BtDb *bt, BtPageSet *set, uint keylen, uint slot, uint vallen)
{
  uint nxt = bt->mgr->page_size;
  BtPage page = set->page;
  uint cnt = 0, idx = 0;
  uint max = page->cnt;
  uint newslot = max;
  BtKey *key;
  BtVal *val;

  if( page->min >= (max+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal))
    return slot;

  //  skip cleanup and proceed to split
  //  if there's not enough garbage
  //  to bother with.

  if( page->garbage < nxt / 5 )
    return 0;

  memcpy (bt->frame, page, bt->mgr->page_size);

  // skip page info and set rest of page to zero

  memset (page+1, 0, bt->mgr->page_size - sizeof(*page));
  set->latch->dirty = 1;
  page->garbage = 0;
  page->act = 0;

  // clean up page first by
  // removing deleted keys

  while( cnt++ < max ) {
    if( cnt == slot )
      newslot = idx + 2;

    if( cnt < max || bt->frame->lvl )
      if( slotptr(bt->frame,cnt)->dead )
        continue;

    // copy the value across

    val = valptr(bt->frame, cnt);
    nxt -= val->len + sizeof(BtVal);
    memcpy ((unsigned char *)page + nxt, val, val->len + sizeof(BtVal));

    // copy the key across

    key = keyptr(bt->frame, cnt);
    nxt -= key->len + sizeof(BtKey);
    memcpy ((char *)page + nxt, key, key->len + sizeof(BtKey));

    // make a librarian slot

    slotptr(page, ++idx)->off = nxt;
    slotptr(page, idx)->type = Librarian;
    slotptr(page, idx)->dead = 1;

    // set up the slot

    slotptr(page, ++idx)->off = nxt;
    slotptr(page, idx)->type = slotptr(bt->frame, cnt)->type;

    if( !(slotptr(page, idx)->dead = slotptr(bt->frame, cnt)->dead) )
      page->act++;
  }

  page->min = nxt;
  page->cnt = idx;

  //  see if page has enough space now, or does it need splitting?

  if( page->min >= (idx+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal) )
    return newslot;

  return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtDb *bt, BtPageSet *root, BtLatchSet *right)
{
  char leftkey[BT_keyarray];
  uint nxt = bt->mgr->page_size;
  unsigned char value[BtId];
  BtPageSet left[1];
  uid left_page_no;
  BtKey *ptr;
  BtVal *val;

  //  save left page fence key for new root

  ptr = keyptr(root->page, root->page->cnt);
  memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

  //  Obtain an empty page to use, and copy the current
  //  root contents into it, e.g. lower keys

  if( bt_newpage(bt, left, root->page) )
    return (BTERR) bt->err;

  left_page_no = left->latch->page_no;
  bt_unpinlatch (left->latch);

  // preserve the page info at the bottom
  // of higher keys and set rest to zero

  memset(root->page+1, 0, bt->mgr->page_size - sizeof(*root->page));

  // insert stopper key at top of newroot page
  // and increase the root height

  nxt -= BtId + sizeof(BtVal);
  bt_putid (value, right->page_no);
  val = (BtVal *)((unsigned char *)root->page + nxt);
  memcpy (val->value, value, BtId);
  val->len = BtId;

  nxt -= 2 + sizeof(BtKey);
  slotptr(root->page, 2)->off = nxt;
  ptr = (BtKey *)((char *)root->page + nxt);
  ptr->len = 2;
  ptr->key[0] = 0xff;
  ptr->key[1] = 0xff;

  // insert lower keys page fence key on newroot page as first key

  nxt -= BtId + sizeof(BtVal);
  bt_putid (value, left_page_no);
  val = (BtVal *)((unsigned char *)root->page + nxt);
  memcpy (val->value, value, BtId);
  val->len = BtId;

  ptr = (BtKey *)leftkey;
  nxt -= ptr->len + sizeof(BtKey);
  slotptr(root->page, 1)->off = nxt;
  memcpy ((char *)root->page + nxt, leftkey, ptr->len + sizeof(BtKey));

  bt_putid(root->page->right, 0);
  root->page->min = nxt;    // reset lowest used offset and key count
  root->page->cnt = 2;
  root->page->act = 2;
  root->page->lvl++;

  // release and unpin root pages

  bt_unlockpage(bt, BtLockWrite, root->latch);
  bt_unpinlatch (root->latch);

  bt_unpinlatch (right);
  return BTERR_ok;
}

//  split already locked full node
//  leave it locked.
//  return pool entry for new right
//  page, unlocked

uint bt_splitpage (BtDb *bt, BtPageSet *set)
{
  uint cnt = 0, idx = 0, max, nxt = bt->mgr->page_size;
  uint lvl = set->page->lvl;
  BtPageSet right[1];
  BtKey *key, *ptr;
  BtVal *val, *src;

  //  split higher half of keys to bt->frame

  memset (bt->frame, 0, bt->mgr->page_size);
  max = set->page->cnt;
  cnt = max / 2;
  idx = 0;

  while( cnt++ < max ) {
    if( cnt < max || set->page->lvl )
      if( slotptr(set->page, cnt)->dead )
        continue;

    src = valptr(set->page, cnt);
    nxt -= src->len + sizeof(BtVal);
    memcpy ((unsigned char *)bt->frame + nxt, src, src->len + sizeof(BtVal));

    key = keyptr(set->page, cnt);
    nxt -= key->len + sizeof(BtKey);
    ptr = (BtKey*)((char *)bt->frame + nxt);
    memcpy (ptr, key, key->len + sizeof(BtKey));

    //  add librarian slot

    slotptr(bt->frame, ++idx)->off = nxt;
    slotptr(bt->frame, idx)->type = Librarian;
    slotptr(bt->frame, idx)->dead = 1;

    //  add actual slot

    slotptr(bt->frame, ++idx)->off = nxt;
    slotptr(bt->frame, idx)->type = slotptr(set->page, cnt)->type;

    if( !(slotptr(bt->frame, idx)->dead = slotptr(set->page, cnt)->dead) )
      bt->frame->act++;
  }

  bt->frame->bits = bt->mgr->page_bits;
  bt->frame->min = nxt;
  bt->frame->cnt = idx;
  bt->frame->lvl = lvl;

  // link right node

  if( set->latch->page_no > ROOT_page )
    bt_putid (bt->frame->right, bt_getid (set->page->right));

  //  get new free page and write higher keys to it.

  if( bt_newpage(bt, right, bt->frame) )
    return 0;

  memcpy (bt->frame, set->page, bt->mgr->page_size);
  memset (set->page+1, 0, bt->mgr->page_size - sizeof(*set->page));
  set->latch->dirty = 1;

  nxt = bt->mgr->page_size;
  set->page->garbage = 0;
  set->page->act = 0;
  max /= 2;
  cnt = 0;
  idx = 0;

  if( slotptr(bt->frame, max)->type == Librarian )
    max--;

  //  assemble page of smaller keys

  while( cnt++ < max ) {
    if( slotptr(bt->frame, cnt)->dead )
      continue;
    val = valptr(bt->frame, cnt);
    nxt -= val->len + sizeof(BtVal);
    memcpy ((unsigned char *)set->page + nxt, val, val->len + sizeof(BtVal));

    key = keyptr(bt->frame, cnt);
    nxt -= key->len + sizeof(BtKey);
    memcpy ((char *)set->page + nxt, key, key->len + sizeof(BtKey));

    //  add librarian slot

    slotptr(set->page, ++idx)->off = nxt;
    slotptr(set->page, idx)->type = Librarian;
    slotptr(set->page, idx)->dead = 1;

    //  add actual slot

    slotptr(set->page, ++idx)->off = nxt;
    slotptr(set->page, idx)->type = slotptr(bt->frame, cnt)->type;
    set->page->act++;
  }

  bt_putid(set->page->right, right->latch->page_no);
  set->page->min = nxt;
  set->page->cnt = idx;

  return right->latch->entry;
}

//  fix keys for newly split page
//  call with page locked, return
//  unlocked

BTERR bt_splitkeys (BtDb *bt, BtPageSet *set, BtLatchSet *right)
{
  char leftkey[BT_keyarray], rightkey[BT_keyarray];
  unsigned char value[BtId];
  uint lvl = set->page->lvl;
  BtPage page;
  BtKey *ptr;

  // if current page is the root page, split it

  if( set->latch->page_no == ROOT_page )
    return bt_splitroot (bt, set, right);

  ptr = keyptr(set->page, set->page->cnt);
  memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

  page = bt_mappage (bt, right);

  ptr = keyptr(page, page->cnt);
  memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));

  // insert new fences in their parent pages

  bt_lockpage (bt, BtLockParent, right);

  bt_lockpage (bt, BtLockParent, set->latch);
  bt_unlockpage (bt, BtLockWrite, set->latch);

  // insert new fence for reformulated left block of smaller keys

  bt_putid (value, set->latch->page_no);
  ptr = (BtKey *)leftkey;

  if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
    return (BTERR) bt->err;

  // switch fence for right block of larger keys to new right page

  bt_putid (value, right->page_no);
  ptr = (BtKey *)rightkey;

  if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
    return (BTERR)bt->err;

  bt_unlockpage (bt, BtLockParent, set->latch);
  bt_unpinlatch (set->latch);

  bt_unlockpage (bt, BtLockParent, right);
  bt_unpinlatch (right);
  return BTERR_ok;
}

//  install new key and value onto page
//  page must already be checked for
//  adequate space

BTERR bt_insertslot (BtDb *bt, BtPageSet *set, uint slot, char *key,uint keylen, unsigned char *value, uint vallen, uint type, uint release)
{
  uint idx, librarian;
  BtSlot *node;
  BtKey *ptr;
  BtVal *val;

  //  if found slot > desired slot and previous slot
  //  is a librarian slot, use it

  if( slot > 1 )
    if( slotptr(set->page, slot-1)->type == Librarian )
      slot--;

  // copy value onto page

  set->page->min -= vallen + sizeof(BtVal);
  val = (BtVal*)((unsigned char *)set->page + set->page->min);
  memcpy (val->value, value, vallen);
  val->len = vallen;

  // copy key onto page

  set->page->min -= keylen + sizeof(BtKey);
  ptr = (BtKey*)((char *)set->page + set->page->min);
  memcpy (ptr->key, key, keylen);
  ptr->len = keylen;

  //  find first empty slot

  for( idx = slot; idx < set->page->cnt; idx++ )
    if( slotptr(set->page, idx)->dead )
      break;

  // now insert key into array before slot

  if( idx == set->page->cnt )
    idx += 2, set->page->cnt += 2, librarian = 2;
  else
    librarian = 1;

  set->latch->dirty = 1;
  set->page->act++;

  while( idx > slot + librarian - 1 )
    *slotptr(set->page, idx) = *slotptr(set->page, idx - librarian), idx--;

  //  add librarian slot

  if( librarian > 1 ) {
    node = slotptr(set->page, slot++);
    node->off = set->page->min;
    node->type = Librarian;
    node->dead = 1;
  }

  //  fill in new slot

  node = slotptr(set->page, slot);
  node->off = set->page->min;
  node->type = type;
  node->dead = 0;

  if( release ) {
    bt_unlockpage (bt, BtLockWrite, set->latch);
    bt_unpinlatch (set->latch);
  }

  return BTERR_ok;
}

//  Insert new key into the btree at given level.
//  either add a new key or update/add an existing one

BTERR bt_insertkey (BtDb *bt, char *key, uint keylen, uint lvl, void *value, uint vallen, uint unique)
{
  char newkey[BT_keyarray];
  uint slot, len, entry;
  BtPageSet set[1];
  BtKey *ptr, *ins;
  uid sequence;
  BtVal *val;
  uint type;

  // set up the key we're working on

  ins = (BtKey*)newkey;
  memcpy (ins->key, key, keylen);
  ins->len = keylen;

  ptr = nullptr;

  // is this a non-unique index value?

  if( unique )
    type = Unique;
  else {
    type = Duplicate;
    sequence = bt_newdup (bt);
    bt_putid ((unsigned char *) ins->key + ins->len + sizeof(BtKey), sequence);
    ins->len += BtId;
  }

  while( 1 ) { // find the page and slot for the current key
    if( (slot = bt_loadpage (bt, set, ins->key, lvl, BtLockWrite) ) )
      ptr = keyptr(set->page, slot);
    else {
      if( !bt->err )
        bt->err = BTERR_ovflw;
      return (BTERR)bt->err;
    }

    // if librarian slot == found slot, advance to real slot

    if( slotptr(set->page, slot)->type == Librarian )
      if( !keycmp (ptr, key, bt->key_schema) )
        ptr = keyptr(set->page, ++slot);

    len = ptr->len;
    if( slotptr(set->page, slot)->type == Duplicate )
      len -= BtId;

    //  if inserting a duplicate key or unique key
    //  check for adequate space on the page
    //  and insert the new key before slot.

    int compare = memcmp (ptr->key, ins->key, ins->len);
    if( (unique && (len != ins->len || compare)) || (!unique) ) {
      if( !(slot = bt_cleanpage (bt, set, ins->len, slot, vallen)) ) {
        if( !(entry = bt_splitpage (bt, set)) )
          return (BTERR)bt->err;
        else if( bt_splitkeys (bt, set, bt->mgr->latchsets + entry) )
          return (BTERR)bt->err;
        else
          continue;
      }

      //std::cout << "new key : unique : " << unique << "\n";

      return bt_insertslot (bt, set, slot, ins->key, ins->len, (unsigned char *) value, vallen, type, 1);
    }

    //std::cout << "key exists : unique : " << unique << "\n";

    // if key already exists, update value and return

    val = valptr(set->page, slot);

    if( val->len >= vallen ) {
      if( slotptr(set->page, slot)->dead )
        set->page->act++;
      set->page->garbage += val->len - vallen;
      set->latch->dirty = 1;
      slotptr(set->page, slot)->dead = 0;
      val->len = vallen;
      memcpy (val->value, value, vallen);
      bt_unlockpage(bt, BtLockWrite, set->latch);
      bt_unpinlatch (set->latch);
      return BTERR_ok;
    }

    //  new update value doesn't fit in existing value area

    if( !slotptr(set->page, slot)->dead )
      set->page->garbage += val->len + ptr->len + sizeof(BtKey) + sizeof(BtVal);
    else {
      slotptr(set->page, slot)->dead = 0;
      set->page->act++;
    }

    if( !(slot = bt_cleanpage (bt, set, keylen, slot, vallen)) ) {
      if( !(entry = bt_splitpage (bt, set)) )
        return (BTERR)bt->err;
      else if( bt_splitkeys (bt, set, bt->mgr->latchsets + entry) )
        return (BTERR)bt->err;
      else
        continue;
    }

    set->page->min -= vallen + sizeof(BtVal);
    val = (BtVal*)((unsigned char *)set->page + set->page->min);
    memcpy (val->value, value, vallen);
    val->len = vallen;

    set->latch->dirty = 1;
    set->page->min -= keylen + sizeof(BtKey);
    ptr = (BtKey*)((char *)set->page + set->page->min);
    memcpy (ptr->key, key, keylen);
    ptr->len = keylen;

    slotptr(set->page, slot)->off = set->page->min;
    bt_unlockpage(bt, BtLockWrite, set->latch);
    bt_unpinlatch (set->latch);
    return BTERR_ok;
  }

  return BTERR_ok;
}

//  determine actual page where key is located
//  return slot number

uint bt_atomicpage (BtDb *bt, BtPage source, AtomicTxn *locks, uint src, BtPageSet *set)
{
  BtKey *key = keyptr(source,src);
  uint slot = locks[src].slot;
  uint entry;

  if( src > 1 && locks[src].reuse )
    entry = locks[src-1].entry, slot = 0;
  else
    entry = locks[src].entry;

  if( slot ) {
    set->latch = bt->mgr->latchsets + entry;
    set->page = bt_mappage (bt, set->latch);
    return slot;
  }

  //  is locks->reuse set? or was slot zeroed?
  //  if so, find where our key is located
  //  on current page or pages split on
  //  same page txn operations.

  do {
    set->latch = bt->mgr->latchsets + entry;
    set->page = bt_mappage (bt, set->latch);

    if( ( slot = bt_findslot(set->page, key->key, bt->key_schema))) {
      if( slotptr(set->page, slot)->type == Librarian )
        slot++;
      if( locks[src].reuse )
        locks[src].entry = entry;
      return slot;
    }
  } while( (entry = set->latch->split ) );

  bt->err = BTERR_atomic;
  return 0;
}

BTERR bt_atomicinsert (BtDb *bt, BtPage source, AtomicTxn *locks, uint src)
{
  BtKey *key = keyptr(source, src);
  BtVal *val = valptr(source, src);
  BtLatchSet *latch;
  BtPageSet set[1];
  uint entry, slot;

  while( (slot = bt_atomicpage (bt, source, locks, src, set) )) {
    if( ( slot = bt_cleanpage(bt, set, key->len, slot, val->len) ) )
      return bt_insertslot (bt, set, slot, key->key, key->len, val->value, val->len, slotptr(source,src)->type, 0);

    if( (entry = bt_splitpage (bt, set) ) )
      latch = bt->mgr->latchsets + entry;
    else
      return (BTERR)bt->err;

    //  splice right page into split chain
    //  and WriteLock it.

    bt_lockpage(bt, BtLockWrite, latch);
    latch->split = set->latch->split;
    set->latch->split = entry;
    locks[src].slot = 0;
  }

  return (BTERR)(bt->err = BTERR_atomic);
}

BTERR bt_atomicdelete (BtDb *bt, BtPage source, AtomicTxn *locks, uint src)
{
  BtKey *key = keyptr(source, src);
  uint slot;
  BtPageSet set[1];
  BtKey *ptr;
  BtVal *val;

  if( (slot = bt_atomicpage (bt, source, locks, src, set)) )
    ptr = keyptr(set->page, slot);
  else
    return (BTERR)(bt->err = BTERR_struct);

  if( !keycmp (ptr, key->key, bt->key_schema) )
    if( !slotptr(set->page, slot)->dead )
      slotptr(set->page, slot)->dead = 1;
    else
      return BTERR_ok;
  else
    return BTERR_ok;

  val = valptr(set->page, slot);
  set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
  set->latch->dirty = 1;
  set->page->act--;
  bt->found++;
  return BTERR_ok;
}

//  delete an empty master page for a transaction

//  note that the far right page never empties because
//  it always contains (at least) the infinite stopper key
//  and that all pages that don't contain any keys are
//  deleted, or are being held under Atomic lock.

BTERR bt_atomicfree (BtDb *bt, BtPageSet *prev)
{
  BtPageSet right[1], temp[1];
  unsigned char value[BtId];
  uid right_page_no;
  BtKey *ptr;

  bt_lockpage(bt, BtLockWrite, prev->latch);

  //  grab the right sibling

  if(  (right->latch = bt_pinlatch(bt, bt_getid (prev->page->right), 1)) )
    right->page = bt_mappage (bt, right->latch);
  else
    return (BTERR) bt->err;

  bt_lockpage(bt, BtLockAtomic, right->latch);
  bt_lockpage(bt, BtLockWrite, right->latch);

  //  and pull contents over empty page
  //  while preserving master's left link

  memcpy (right->page->left, prev->page->left, BtId);
  memcpy (prev->page, right->page, bt->mgr->page_size);

  //  forward seekers to old right sibling
  //  to new page location in set

  bt_putid (right->page->right, prev->latch->page_no);
  right->latch->dirty = 1;
  right->page->kill = 1;

  //  remove pointer to right page for searchers by
  //  changing right fence key to point to the master page

  ptr = keyptr(right->page,right->page->cnt);
  bt_putid (value, prev->latch->page_no);

  if( bt_insertkey (bt, ptr->key, ptr->len, 1, value, BtId, 1) )
    return (BTERR) bt->err;

  //  now that master page is in good shape we can
  //  remove its locks.

  bt_unlockpage (bt, BtLockAtomic, prev->latch);
  bt_unlockpage (bt, BtLockWrite, prev->latch);

  //  fix master's right sibling's left pointer
  //  to remove scanner's poiner to the right page

  temp->page = nullptr;
  if( (right_page_no = bt_getid (prev->page->right)) ) {
    if( (temp->latch = bt_pinlatch (bt, right_page_no, 1)) )
      temp->page = bt_mappage (bt, temp->latch);

    bt_lockpage (bt, BtLockWrite, temp->latch);
    bt_putid (temp->page->left, prev->latch->page_no);
    temp->latch->dirty = 1;

    bt_unlockpage (bt, BtLockWrite, temp->latch);
    bt_unpinlatch (temp->latch);
  } else {  // master is now the far right page
    bt_spinwritelock (bt->mgr->lock);
    bt_putid (bt->mgr->pagezero->alloc->left, prev->latch->page_no);
    bt_spinreleasewrite(bt->mgr->lock);
  }

  //  now that there are no pointers to the right page
  //  we can delete it after the last read access occurs

  bt_unlockpage (bt, BtLockWrite, right->latch);
  bt_unlockpage (bt, BtLockAtomic, right->latch);
  bt_lockpage (bt, BtLockDelete, right->latch);
  bt_lockpage (bt, BtLockWrite, right->latch);
  bt_freepage (bt, right);
  return BTERR_ok;
}

//  atomic modification of a batch of keys.

//  return -1 if BTERR is set
//  otherwise return slot number
//  causing the key constraint violation
//  or zero on successful completion.

int bt_atomictxn (BtDb *bt, BtPage source, uint unique)
{
  uint src, idx, slot, samepage, entry;
  AtomicKey *head, *tail, *leaf;
  BtPageSet set[1], prev[1];
  unsigned char value[BtId];
  BtKey *key, *ptr, *key2;
  BtLatchSet *latch;
  AtomicTxn *locks;
  int result = 0;
  BtSlot temp[1];
  uid right;

  locks = (AtomicTxn *) calloc (source->cnt + 1, sizeof(AtomicTxn));
  head = NULL;
  tail = NULL;

  // stable sort the list of keys into order to
  //  prevent deadlocks between threads.

  for( src = 1; src++ < source->cnt; ) {
    *temp = *slotptr(source,src);
    key = keyptr (source,src);

    for( idx = src; --idx; ) {
      key2 = keyptr (source,idx);
      if( keycmp (key, key2->key, bt->key_schema) < 0 ) {
        *slotptr(source,idx+1) = *slotptr(source,idx);
        *slotptr(source,idx) = *temp;
      } else
        break;
    }
  }

  // Load the leaf page for each key
  // group same page references with reuse bit
  // and determine any constraint violations

  for( src = 0; src++ < source->cnt; ) {
    key = keyptr(source, src);
    slot = 0;

    // first determine if this modification falls
    // on the same page as the previous modification
    //  note that the far right leaf page is a special case

    if( (samepage = src > 1) ) {
      if( (samepage = !bt_getid(set->page->right) || keycmp (keyptr(set->page, set->page->cnt), key->key, bt->key_schema) >= 0 ) )
        slot = bt_findslot(set->page, key->key, bt->key_schema);
      else
        bt_unlockpage(bt, BtLockRead, set->latch);
    }

    if( !slot ) {
      if(( slot = bt_loadpage(bt, set, key->key, 0, (BtLock)( BtLockRead | BtLockAtomic))) )
        set->latch->split = 0;
      else
        return -1;
    }

    if( slotptr(set->page, slot)->type == Librarian )
      ptr = keyptr(set->page, ++slot);
    else
      ptr = keyptr(set->page, slot);

    if( !samepage ) {
      locks[src].entry = set->latch->entry;
      locks[src].slot = slot;
      locks[src].reuse = 0;
    } else {
      locks[src].entry = 0;
      locks[src].slot = 0;
      locks[src].reuse = 1;
    }

    switch( slotptr(source, src)->type ) {
      case Duplicate:
      case Unique:
        if( !slotptr(set->page, slot)->dead )
          if( slot < set->page->cnt || bt_getid (set->page->right) )
            if( !keycmp (ptr, key->key, bt->key_schema) ) {

              // return constraint violation if key already exists

              bt_unlockpage(bt, BtLockRead, set->latch);
              result = src;

              while( src ) {
                if( locks[src].entry ) {
                  set->latch = bt->mgr->latchsets + locks[src].entry;
                  bt_unlockpage(bt, BtLockAtomic, set->latch);
                  bt_unpinlatch (set->latch);
                }
                src--;
              }
              free (locks);
              return result;
            }
        break;
    }
  }

  //  unlock last loadpage lock

  if( source->cnt )
    bt_unlockpage(bt, BtLockRead, set->latch);

  //  obtain write lock for each master page

  for( src = 0; src++ < source->cnt; )
    if( locks[src].reuse )
      continue;
    else
      bt_lockpage(bt, BtLockWrite, bt->mgr->latchsets + locks[src].entry);

  // insert or delete each key
  // process any splits or merges
  // release Write & Atomic latches
  // set ParentModifications and build
  // queue of keys to insert for split pages
  // or delete for deleted pages.

  // run through txn list backwards

  samepage = source->cnt + 1;

  for( src = source->cnt; src; src-- ) {
    if( locks[src].reuse )
      continue;

    //  perform the txn operations
    //  from smaller to larger on
    //  the same page

    for( idx = src; idx < samepage; idx++ )
      switch( slotptr(source,idx)->type ) {
        case Delete:
          if( bt_atomicdelete (bt, source, locks, idx) )
            return -1;
          break;

        case Duplicate:
        case Unique:
          if( bt_atomicinsert (bt, source, locks, idx) )
            return -1;
          break;
      }

    //  after the same page operations have finished,
    //  process master page for splits or deletion.

    latch = prev->latch = bt->mgr->latchsets + locks[src].entry;
    prev->page = bt_mappage (bt, prev->latch);
    samepage = src;

    //  pick-up all splits from master page
    //  each one is already WriteLocked.

    entry = prev->latch->split;

    while( entry ) {
      set->latch = bt->mgr->latchsets + entry;
      set->page = bt_mappage (bt, set->latch);
      entry = set->latch->split;

      // delete empty master page by undoing its split
      //  (this is potentially another empty page)
      //  note that there are no new left pointers yet

      if( !prev->page->act ) {
        memcpy (set->page->left, prev->page->left, BtId);
        memcpy (prev->page, set->page, bt->mgr->page_size);
        bt_lockpage (bt, BtLockDelete, set->latch);
        bt_freepage (bt, set);

        prev->latch->dirty = 1;
        continue;
      }

      // remove empty page from the split chain

      if( !set->page->act ) {
        memcpy (prev->page->right, set->page->right, BtId);
        prev->latch->split = set->latch->split;
        bt_lockpage (bt, BtLockDelete, set->latch);
        bt_freepage (bt, set);
        continue;
      }

      //  schedule prev fence key update

      ptr = keyptr(prev->page,prev->page->cnt);
      leaf = (AtomicKey *) calloc (sizeof(AtomicKey), 1);

      memcpy (leaf->leafkey, ptr, ptr->len + sizeof(BtKey));
      leaf->page_no = prev->latch->page_no;
      leaf->entry = prev->latch->entry;
      leaf->type = 0;

      if( tail )
        tail->next = leaf;
      else
        head = leaf;

      tail = leaf;

      // splice in the left link into the split page

      bt_putid (set->page->left, prev->latch->page_no);
      bt_lockpage(bt, BtLockParent, prev->latch);
      bt_unlockpage(bt, BtLockWrite, prev->latch);
      *prev = *set;
    }

    //  update left pointer in next right page from last split page
    //  (if all splits were reversed, latch->split == 0)

    if( latch->split ) {
      //  fix left pointer in master's original (now split)
      //  far right sibling or set rightmost page in page zero

      if( (right = bt_getid (prev->page->right) ) ) {
        if( (set->latch = bt_pinlatch (bt, right, 1) ) )
          set->page = bt_mappage (bt, set->latch);
        else
          return -1;

        bt_lockpage (bt, BtLockWrite, set->latch);
        bt_putid (set->page->left, prev->latch->page_no);
        set->latch->dirty = 1;
        bt_unlockpage (bt, BtLockWrite, set->latch);
        bt_unpinlatch (set->latch);
      } else {  // prev is rightmost page
        bt_spinwritelock (bt->mgr->lock);
        bt_putid (bt->mgr->pagezero->alloc->left, prev->latch->page_no);
        bt_spinreleasewrite(bt->mgr->lock);
      }

      //  Process last page split in chain

      ptr = keyptr(prev->page,prev->page->cnt);
      leaf = (AtomicKey *) calloc (sizeof(AtomicKey), 1);

      memcpy (leaf->leafkey, ptr, ptr->len + sizeof(BtKey));
      leaf->page_no = prev->latch->page_no;
      leaf->entry = prev->latch->entry;
      leaf->type = 0;

      if( tail )
        tail->next = leaf;
      else
        head = leaf;

      tail = leaf;

      bt_lockpage(bt, BtLockParent, prev->latch);
      bt_unlockpage(bt, BtLockWrite, prev->latch);

      //  remove atomic lock on master page

      bt_unlockpage(bt, BtLockAtomic, latch);
      continue;
    }

    //  finished if prev page occupied (either master or final split)

    if( prev->page->act ) {
      bt_unlockpage(bt, BtLockWrite, latch);
      bt_unlockpage(bt, BtLockAtomic, latch);
      bt_unpinlatch(latch);
      continue;
    }

    // any and all splits were reversed, and the
    // master page located in prev is empty, delete it
    // by pulling over master's right sibling.

    // Remove empty master's fence key

    ptr = keyptr(prev->page,prev->page->cnt);

    if( bt_deletekey (bt, ptr->key, 1, unique) )
      return -1;

    //  perform the remainder of the delete
    //  from the FIFO queue

    leaf = (AtomicKey *) calloc (sizeof(AtomicKey), 1);

    memcpy (leaf->leafkey, ptr, ptr->len + sizeof(BtKey));
    leaf->page_no = prev->latch->page_no;
    leaf->entry = prev->latch->entry;
    leaf->nounlock = 1;
    leaf->type = 2;

    if( tail )
      tail->next = leaf;
    else
      head = leaf;

    tail = leaf;

    //  leave atomic lock in place until
    //  deletion completes in next phase.

    bt_unlockpage(bt, BtLockWrite, prev->latch);
  }

  //  add & delete keys for any pages split or merged during transaction

  if( (leaf = head) )
    do {
      set->latch = bt->mgr->latchsets + leaf->entry;
      set->page = bt_mappage (bt, set->latch);

      bt_putid (value, leaf->page_no);
      ptr = (BtKey *)leaf->leafkey;

      switch( leaf->type ) {
        case 0: // insert key
          if( bt_insertkey (bt, ptr->key, ptr->len, 1, value, BtId, 1) )
            return -1;

          break;

        case 1: // delete key
          if( bt_deletekey (bt, ptr->key, 1, unique) )
            return -1;

          break;

        case 2: // free page
          if( bt_atomicfree (bt, set) )
            return -1;

          break;
      }

      if( !leaf->nounlock )
        bt_unlockpage (bt, BtLockParent, set->latch);

      bt_unpinlatch (set->latch);
      tail = (AtomicKey *) leaf->next;
      free (leaf);
    } while( (leaf = tail) );

  // return success

  free (locks);
  return 0;
}

//  set cursor to highest slot on highest page

uint bt_lastkey (BtDb *bt)
{
  uid page_no = bt_getid (bt->mgr->pagezero->alloc->left);
  BtPageSet set[1];

  if( (set->latch = bt_pinlatch (bt, page_no, 1) ) )
    set->page = bt_mappage (bt, set->latch);
  else
    return 0;

  bt_lockpage(bt, BtLockRead, set->latch);
  memcpy (bt->cursor, set->page, bt->mgr->page_size);
  bt_unlockpage(bt, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);

  bt->cursor_page = page_no;
  return bt->cursor->cnt;
}

//  return previous slot on cursor page

uint bt_prevkey (BtDb *bt, uint slot)
{
  uid ourright, next, us = bt->cursor_page;
  BtPageSet set[1];

  if( --slot )
    return slot;

  ourright = bt_getid(bt->cursor->right);

  goleft:
  if( !(next = bt_getid(bt->cursor->left)) )
    return 0;

  findourself:
  bt->cursor_page = next;

  if( (set->latch = bt_pinlatch (bt, next, 1) ))
    set->page = bt_mappage (bt, set->latch);
  else
    return 0;

  bt_lockpage(bt, BtLockRead, set->latch);
  memcpy (bt->cursor, set->page, bt->mgr->page_size);
  bt_unlockpage(bt, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);

  next = bt_getid (bt->cursor->right);

  if( bt->cursor->kill )
    goto findourself;

  if( next != us ) {
    if( next == ourright )
      goto goleft;
    else
      goto findourself;
  }

  return bt->cursor->cnt;
}

//  return next slot on cursor page
//  or slide cursor right into next page

uint bt_nextkey (BtDb *bt, uint slot)
{
  BtPageSet set[1];
  uid right;

  do {
    right = bt_getid(bt->cursor->right);

    while( slot++ < bt->cursor->cnt )
      if( slotptr(bt->cursor,slot)->dead )
        continue;
      else if( right || (slot < bt->cursor->cnt) ) // skip infinite stopper
        return slot;
      else
        break;

    if( !right )
      break;

    bt->cursor_page = right;

    if( (set->latch = bt_pinlatch (bt, right, 1)) )
      set->page = bt_mappage (bt, set->latch);
    else
      return 0;

    bt_lockpage(bt, BtLockRead, set->latch);

    memcpy (bt->cursor, set->page, bt->mgr->page_size);

    bt_unlockpage(bt, BtLockRead, set->latch);
    bt_unpinlatch (set->latch);
    slot = 0;

  } while( 1 );

  return bt->err = 0;
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, char *key)
{
  BtPageSet set[1];
  uint slot;

  // cache page for retrieval

  if( ( slot = bt_loadpage (bt, set, key, 0, BtLockRead) ) )
    memcpy (bt->cursor, set->page, bt->mgr->page_size);
  else
    return 0;

  bt->cursor_page = set->latch->page_no;

  bt_unlockpage(bt, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);
  return slot;
}

BtKey *bt_key(BtDb *bt, uint slot)
{
  return keyptr(bt->cursor, slot);
}

BtVal *bt_val(BtDb *bt, uint slot)
{
  return valptr(bt->cursor,slot);
}

/*
 * USAGE :
 *
 * gcc -D STANDALONE -O3 threadskv8.c -lpthread
 * ./a.out btree_file w buffer_pool_size 1024 3 src_file1
 * ./a.out btree_file s buffer_pool_size 1024 3 src_file1
 *
 */

#ifndef unix
double getCpuTime(int type)
{
  FILETIME crtime[1];
  FILETIME xittime[1];
  FILETIME systime[1];
  FILETIME usrtime[1];
  SYSTEMTIME timeconv[1];
  double ans = 0;

  memset (timeconv, 0, sizeof(SYSTEMTIME));

  switch( type ) {
    case 0:
      GetSystemTimeAsFileTime (xittime);
      FileTimeToSystemTime (xittime, timeconv);
      ans = (double)timeconv->wDayOfWeek * 3600 * 24;
      break;
    case 1:
      GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
      FileTimeToSystemTime (usrtime, timeconv);
      break;
    case 2:
      GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
      FileTimeToSystemTime (systime, timeconv);
      break;
  }

  ans += (double)timeconv->wHour * 3600;
  ans += (double)timeconv->wMinute * 60;
  ans += (double)timeconv->wSecond;
  ans += (double)timeconv->wMilliseconds / 1000;
  return ans;
}
#else
#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
  struct rusage used[1];
  struct timeval tv[1];

  switch( type ) {
    case 0:
      gettimeofday(tv, NULL);
      return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

    case 1:
      getrusage(RUSAGE_SELF, used);
      return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

    case 2:
      getrusage(RUSAGE_SELF, used);
      return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
  }

  return 0;
}
#endif

void bt_poolaudit (BtMgr *mgr)
{
  BtLatchSet *latch;
  uint slot = 0;

  while( slot++ < mgr->latchdeployed ) {
    latch = mgr->latchsets + slot;

    if( *latch->readwr->rin & MASK )
      fprintf(stderr, "latchset %d rwlocked for page %.8llu\n", slot, latch->page_no);
    memset ((ushort *)latch->readwr, 0, sizeof(RWLock));

    if( *latch->access->rin & MASK )
      fprintf(stderr, "latchset %d accesslocked for page %.8llu\n", slot, latch->page_no);
    memset ((ushort *)latch->access, 0, sizeof(RWLock));

    if( *latch->parent->tid )
      fprintf(stderr, "latchset %d parentlocked for page %.8llu\n", slot, latch->page_no);
    memset ((ushort *)latch->parent, 0, sizeof(RWLock));

    if( latch->pin & ~CLOCK_bit ) {
      fprintf(stderr, "latchset %d pinned for page %.8llu\n", slot, latch->page_no);
      latch->pin = 0;
    }
  }
}

uint bt_latchaudit (BtDb *bt)
{
  ushort idx, hashidx;
  uid page_no;
  BtLatchSet *latch;
  uint cnt = 0;

  if( *(ushort *)(bt->mgr->lock) )
    fprintf(stderr, "Alloc page locked\n");
  *(ushort *)(bt->mgr->lock) = 0;

  for( idx = 1; idx <= bt->mgr->latchdeployed; idx++ ) {
    latch = bt->mgr->latchsets + idx;
    if( *latch->readwr->rin & MASK )
      fprintf(stderr, "latchset %d rwlocked for page %.8llu\n", idx, latch->page_no);
    memset ((ushort *)latch->readwr, 0, sizeof(RWLock));

    if( *latch->access->rin & MASK )
      fprintf(stderr, "latchset %d accesslocked for page %.8llu\n", idx, latch->page_no);
    memset ((ushort *)latch->access, 0, sizeof(RWLock));

    if( *latch->parent->tid )
      fprintf(stderr, "latchset %d parentlocked for page %.8llu\n", idx, latch->page_no);
    memset ((ushort *)latch->parent, 0, sizeof(WOLock));

    if( latch->pin ) {
      fprintf(stderr, "latchset %d pinned for page %.8llu\n", idx, latch->page_no);
      latch->pin = 0;
    }
  }

  for( hashidx = 0; hashidx < bt->mgr->latchhash; hashidx++ ) {
    if( *(ushort *)(bt->mgr->hashtable[hashidx].latch) )
      fprintf(stderr, "hash entry %d locked\n", hashidx);

    *(ushort *)(bt->mgr->hashtable[hashidx].latch) = 0;

    if( (idx = bt->mgr->hashtable[hashidx].slot ) ) do {
      latch = bt->mgr->latchsets + idx;
      if( latch->pin )
        fprintf(stderr, "latchset %d pinned for page %.8llu\n", idx, latch->page_no);
    } while( ( idx = latch->next ) );
  }

  page_no = LEAF_page;

  while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
    uid off = page_no << bt->mgr->page_bits;
#ifdef unix
    ssize_t ret = pread (bt->mgr->idx, bt->frame, bt->mgr->page_size, off);
    if(ret == -1) {
     fprintf(stderr, "pread error \n");
     return 0;
    }
#else
    DWORD amt[1];

    SetFilePointer (bt->mgr->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

    if( !ReadFile(bt->mgr->idx, bt->frame, bt->mgr->page_size, amt, NULL))
      return bt->err = BTERR_map;

    if( *amt <  bt->mgr->page_size )
      return bt->err = BTERR_map;
#endif
    if( !bt->frame->free && !bt->frame->lvl )
      cnt += bt->frame->act;
    page_no++;
  }

  cnt--;  // remove stopper key
  fprintf(stderr, " Total keys read %d\n", cnt);

  bt_close (bt);
  return 0;
}

typedef struct {
  char idx;
  char *type;
  char *infile;
  BtMgr *mgr;
  int num;
} ThreadArg;

//  standalone program to index file of keys
//  then list them onto std-out

#ifdef unix
void *index_file (void *arg)
#else
uint __stdcall index_file (void *arg)
#endif
{
  int line = 0, found = 0, cnt = 0;
  uid next, page_no = LEAF_page;  // start on first page of leaves
  int ch, len = 0, type = 0;
  unsigned int slot;
  char key[BT_maxkey];
  unsigned char txn[65536];
  ThreadArg *args = (ThreadArg*) arg;
  BtPageSet set[1];
  uint nxt = 65536;
  BtPage page;
  BtKey *ptr;
  BtVal *val;
  BtDb *bt;
  FILE *in;

  uint unique = 0;
  bt = bt_open (args->mgr);
  page = (BtPage)txn;

  if( (size_t ) args->idx < strlen (args->type) )
    ch = args->type[(size_t) args->idx];
  else
    ch = args->type[strlen(args->type) - 1];

  switch(ch | 0x20)
  {
    case 'a':
      fprintf(stderr, "started latch mgr audit\n");
      cnt = bt_latchaudit (bt);
      fprintf(stderr, "finished latch mgr audit, found %d keys\n", cnt);
      break;

    case 'd':
      type = Delete;

    case 'p':
      if( !type )
        type = Unique;

      if( args->num )
        if( type == Delete )
          fprintf(stderr, "started TXN pennysort delete for %s\n", args->infile);
        else
          fprintf(stderr, "started TXN pennysort insert for %s\n", args->infile);
      else
        if( type == Delete )
          fprintf(stderr, "started pennysort delete for %s\n", args->infile);
        else
          fprintf(stderr, "started pennysort insert for %s\n", args->infile);

      if( (in = fopen (args->infile, "rb")) )
        while( ch = getc(in), ch != EOF )
          if( ch == '\n' )
          {
            line++;

            if( !args->num ) {
              if( bt_insertkey (bt, key, 10, 0, key + 10, len - 10, 1) )
                fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
              len = 0;
              continue;
            }

            nxt -= len - 10;
            memcpy (txn + nxt, key + 10, len - 10);
            nxt -= 1;
            txn[nxt] = len - 10;
            nxt -= 10;
            memcpy (txn + nxt, key, 10);
            nxt -= 1;
            txn[nxt] = 10;
            slotptr(page,++cnt)->off  = nxt;
            slotptr(page,cnt)->type = type;
            len = 0;

            if( cnt < args->num )
              continue;

            page->cnt = cnt;
            page->act = cnt;
            page->min = nxt;

            if( bt_atomictxn (bt, page, unique) )
              fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
            nxt = sizeof(txn);
            cnt = 0;

          }
          else if( len < BT_maxkey )
            key[len++] = ch;
      fprintf(stderr, "finished %s for %d keys: %d reads %d writes %d found\n", args->infile, line, bt->reads, bt->writes, bt->found);
      break;

    case 'w':
      fprintf(stderr, "started indexing for %s\n", args->infile);
      if( (in = fopen (args->infile, "r") ) )
        while( ch = getc(in), ch != EOF )
          if( ch == '\n' )
          {
            line++;

            if( bt_insertkey (bt, key, len, 0, NULL, 0, 1) )
              fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
            len = 0;
          }
          else if( len < BT_maxkey )
            key[len++] = ch;
      fprintf(stderr, "finished %s for %d keys: %d reads %d writes\n", args->infile, line, bt->reads, bt->writes);
      break;

    case 'f':
      fprintf(stderr, "started finding keys for %s\n", args->infile);
      if( (in = fopen (args->infile, "rb")) )
        while( ch = getc(in), ch != EOF )
          if( ch == '\n' )
          {
            line++;
            if( bt_findkey (bt, key, len, NULL, 0) == 0 )
              found++;
            else if( bt->err )
              fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line), exit(0);
            len = 0;
          }
          else if( len < BT_maxkey )
            key[len++] = ch;
      fprintf(stderr, "finished %s for %d keys, found %d: %d reads %d writes\n", args->infile, line, found, bt->reads, bt->writes);
      break;

    case 's':
      fprintf(stderr, "started scanning\n");
      do {
        if( (set->latch = bt_pinlatch (bt, page_no, 1) ) )
          set->page = bt_mappage (bt, set->latch);
        else
          fprintf(stderr, "unable to obtain latch"), exit(1);
        bt_lockpage (bt, BtLockRead, set->latch);
        next = bt_getid (set->page->right);

        for( slot = 0; slot++ < set->page->cnt; )
          if( next || slot < set->page->cnt )
            if( !slotptr(set->page, slot)->dead ) {
              ptr = keyptr(set->page, slot);
              len = ptr->len;

              if( slotptr(set->page, slot)->type == Duplicate )
                len -= BtId;

              fwrite (ptr->key, len, 1, stdout);
              val = valptr(set->page, slot);
              fwrite (val->value, val->len, 1, stdout);
              fputc ('\n', stdout);
              cnt++;
            }

        bt_unlockpage (bt, BtLockRead, set->latch);
        bt_unpinlatch (set->latch);
      } while( (page_no = next ) );

      fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
      break;

    case 'r':
      fprintf(stderr, "started reverse scan\n");
      if( (slot = bt_lastkey (bt)) )
        while( (slot = bt_prevkey (bt, slot) ) ) {
          if( slotptr(bt->cursor, slot)->dead )
            continue;

          ptr = keyptr(bt->cursor, slot);
          len = ptr->len;

          if( slotptr(bt->cursor, slot)->type == Duplicate )
            len -= BtId;

          fwrite (ptr->key, len, 1, stdout);
          val = valptr(bt->cursor, slot);
          fwrite (val->value, val->len, 1, stdout);
          fputc ('\n', stdout);
          cnt++;
        }

      fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
      break;

    case 'c':
#ifdef unix
      posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
      fprintf(stderr, "started counting\n");
      page_no = LEAF_page;

      while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
        if( bt_readpage (bt->mgr, bt->frame, page_no) )
          break;

        if( !bt->frame->free && !bt->frame->lvl )
          cnt += bt->frame->act;

        bt->reads++;
        page_no++;
      }

      cnt--;  // remove stopper key
      fprintf(stderr, " Total keys counted %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
      break;
  }

  bt_close (bt);
#ifdef unix
  return NULL;
#else
  return 0;
#endif
}

typedef struct timeval timer;

int main_test (int argc, char **argv)
{
  int idx, cnt, err;
  int bits = 16;
  double start;
#ifdef unix
  pthread_t *threads;
#else
  HANDLE *threads;
#endif
  ThreadArg *args;
  uint poolsize = 0;
  float elapsed;
  int num = 0;
  BtMgr *mgr;

  if( argc < 3 ) {
    fprintf (stderr, "Usage: %s idx_file cmds [page_bits buffer_pool_size txn_size src_file1 src_file2 ... ]\n", argv[0]);
    fprintf (stderr, "  where idx_file is the name of the btree file\n");
    fprintf (stderr, "  cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort, with one character command for each input src_file. Commands with no input file need a placeholder.\n");
    fprintf (stderr, "  page_bits is the page size in bits\n");
    fprintf (stderr, "  buffer_pool_size is the number of pages in buffer pool\n");
    fprintf (stderr, "  txn_size = n to block transactions into n units, or zero for no transactions\n");
    fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
    exit(0);
  }

  start = getCpuTime(0);

  if( argc > 3 )
    bits = atoi(argv[3]);

  if( argc > 4 )
    poolsize = atoi(argv[4]);

  if( !poolsize )
    fprintf (stderr, "Warning: no mapped_pool\n");

  if( argc > 5 )
    num = atoi(argv[5]);

  cnt = argc - 6;
#ifdef unix
  threads = (pthread_t *) malloc (cnt * sizeof(pthread_t));
#else
  threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
  args = (ThreadArg *) malloc (cnt * sizeof(ThreadArg));

  mgr = bt_mgr ((argv[1]), bits, poolsize);

  if( !mgr ) {
    fprintf(stderr, "Index Open Error %s\n", argv[1]);
    exit (1);
  }

  //  fire off threads

  for( idx = 0; idx < cnt; idx++ ) {
    args[idx].infile = argv[idx + 6];
    args[idx].type = argv[2];
    args[idx].mgr = mgr;
    args[idx].num = num;
    args[idx].idx = idx;
#ifdef unix
    if( ( err = pthread_create (threads + idx, NULL, index_file, args + idx) ) )
      fprintf(stderr, "Error creating thread %d\n", err);
#else
    threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, index_file, args + idx, 0, NULL);
#endif
  }

  //  wait for termination

#ifdef unix
  for( idx = 0; idx < cnt; idx++ )
    pthread_join (threads[idx], NULL);
#else
  WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

  for( idx = 0; idx < cnt; idx++ )
    CloseHandle(threads[idx]);

#endif
  bt_poolaudit(mgr);
  bt_mgrclose (mgr);

  elapsed = getCpuTime(0) - start;
  fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
  elapsed = getCpuTime(1);
  fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
  elapsed = getCpuTime(2);
  fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

  return 0;
}

} // End index namespace
} // End nstore namespace


