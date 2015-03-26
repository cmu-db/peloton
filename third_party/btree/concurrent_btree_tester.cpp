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

#include "concurrent_btree.h"

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
  uid next, page_no;
  BtLatchSet *latch;
  uint cnt = 0;
  BtKey *ptr;

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

    if( idx = bt->mgr->hashtable[hashidx].slot ) do {
      latch = bt->mgr->latchsets + idx;
      if( latch->pin )
        fprintf(stderr, "latchset %d pinned for page %.8llu\n", idx, latch->page_no);
    } while( idx = latch->next );
  }

  page_no = LEAF_page;

  while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
    uid off = page_no << bt->mgr->page_bits;
#ifdef unix
    pread (bt->mgr->idx, bt->frame, bt->mgr->page_size, off);
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
  int line = 0, found = 0, cnt = 0, idx;
  uid next, page_no = LEAF_page;  // start on first page of leaves
  int ch, len = 0, slot, type = 0;
  unsigned char key[BT_maxkey];
  unsigned char txn[65536];
  ThreadArg *args = (ThreadArg*) arg;
  BtPageSet set[1];
  uint nxt = 65536;
  BtPage page;
  BtKey *ptr;
  BtVal *val;
  BtDb *bt;
  FILE *in;

  bt = bt_open (args->mgr);
  page = (BtPage)txn;

  if( args->idx < strlen (args->type) )
    ch = args->type[args->idx];
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

      if( in = fopen (args->infile, "rb") )
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

            if( bt_atomictxn (bt, page) )
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
      if( in = fopen (args->infile, "r") )
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
      if( in = fopen (args->infile, "rb") )
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
        if( set->latch = bt_pinlatch (bt, page_no, 1) )
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
      } while( page_no = next );

      fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
      break;

    case 'r':
      fprintf(stderr, "started reverse scan\n");
      if( slot = bt_lastkey (bt) )
        while( slot = bt_prevkey (bt, slot) ) {
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

int main (int argc, char **argv)
{
  int idx, cnt, len, slot, err;
  int segsize, bits = 16;
  double start, stop;
#ifdef unix
  pthread_t *threads;
#else
  HANDLE *threads;
#endif
  ThreadArg *args;
  uint poolsize = 0;
  float elapsed;
  int num = 0;
  char key[1];
  BtMgr *mgr;
  BtKey *ptr;
  BtDb *bt;

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
    if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
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
}


