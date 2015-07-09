/*-------------------------------------------------------------------------
 *
 * shmset.c
 *
 * Shared memory context for postgres based on aset.c and a shared
 * memory management library.
 *
 * Portions Copyright (c) 2003, Regents of the University of California
 * Portions Copyright (c) 2015, CMU
 *
 * IDENTIFICATION
 *    src/backend/utils/mmgr/shmset.c
 *
 * by: owenc@eecs.berkeley.edu
 *
 * For all services such as shared memory allocation, and locking this allocator
 * relies on the open source mm shared memory management library.
 * This library abstracts the details of shared memory allocation and locking,
 * and may choose to implement shared memory using a number of methods such as IPC,
 * mmap,or files. In this way the library provides a  set of services that is
 * highly portable across a number of platforms.
 *
 * For more information on the mm library which was developed for use by
 * Apache, please see http://www.ossp.org/pkg/lib/mm/
 */

#include "postgres.h"
#include "utils/memutils.h"

#include <mm.h>

/* Define this to detail debug alloc information */
/* #define HAVE_SHMALLOCINFO */

/*--------------------
 * Chunk freelist k holds chunks of size 1 << (k + SHMALLOC_MINBITS),
 * for k = 0 .. ALLOCSET_NUM_FREELISTS-1.
 *
 * Note that all chunks in the freelists have power-of-2 sizes.  This
 * improves recyclability: we may waste some space, but the wasted space
 * should stay pretty constant as requests are made and released.
 *
 * A request too large for the last freelist is handled by allocating a
 * dedicated block from malloc().  The block still has a block header and
 * chunk header, but when the chunk is freed we'll return the whole block
 * to malloc(), not put it on our freelists.
 *
 * CAUTION: SHMALLOC_MINBITS must be large enough so that
 * 1<<SHMALLOC_MINBITS is at least MAXALIGN,
 * or we may fail to align the smallest chunks adequately.
 * 16-byte alignment is enough on all currently known machines.
 *
 * With the current parameters, request sizes up to 8K are treated as chunks,
 * larger requests go into dedicated blocks.  Change ALLOCSET_NUM_FREELISTS
 * to adjust the boundary point.
 *--------------------
 */

#define SHMALLOC_MINBITS		4		        /* smallest chunk size is 16 bytes */
#define SHMALLOCSET_NUM_FREELISTS	10
#define SHMALLOC_CHUNK_LIMIT	(1 << (SHMALLOCSET_NUM_FREELISTS-1+SHMALLOC_MINBITS))
/* Size of largest chunk that we use a fixed size for */

/*--------------------
 * The first block allocated for an allocset has size initBlockSize.
 * Each time we have to allocate another block, we double the block size
 * (if possible, and without exceeding maxBlockSize), so as to reduce
 * the bookkeeping load on malloc().
 *
 * Blocks allocated to hold oversize chunks do not follow this rule, however;
 * they are just however big they need to be to hold that single chunk.
 *--------------------
 */

#define SHMALLOC_BLOCKHDRSZ MAXALIGN(sizeof(SHMAllocBlockData))
#define SHMALLOC_CHUNKHDRSZ MAXALIGN(sizeof(SHMAllocChunkData))

typedef struct SHMAllocBlockData *SHMAllocBlock;		/* forward reference */
typedef struct SHMAllocChunkData *SHMAllocChunk;

/*
 * AllocPointer
 *		Aligned pointer which may be a member of an allocation set.
 */
typedef void *SHMAllocPointer;

/*
 * AllocSetContext is our standard implementation of MemoryContext.
 */
typedef struct SHMAllocSetContext
{
  MemoryContextData header;	/* Standard memory-context fields */
  /* Info about storage allocated in this context: */
  SHMAllocBlock blocks;		/* head of list of blocks in this set */
  SHMAllocChunk freelist[SHMALLOCSET_NUM_FREELISTS];	/* free chunk lists */
  /* Allocation parameters for this context: */
  Size		initBlockSize;	/* initial block size */
  Size		maxBlockSize;	/* maximum block size */
  SHMAllocBlock keeper;		/* if not NULL, keep this block over
   * resets */

}	SHMAllocSetContext;

typedef SHMAllocSetContext *SHMAllocSet;

/*
 * SHMAllocBlock
 *		An AllocBlock is the unit of memory that is obtained by aset.c
 *		from malloc().	It contains one or more SHMAllocChunks, which are
 *		the units requested by palloc() and freed by pfree().  AllocChunks
 *		cannot be returned to malloc() individually, instead they are put
 *		on freelists by pfree() and re-used by the next palloc() that has
 *		a matching request size.
 *
 *		AllocBlockData is the header data for a block --- the usable space
 *		within the block begins at the next alignment boundary.
 */
typedef struct SHMAllocBlockData
{
  SHMAllocSet aset;			/* aset that owns this block */
  SHMAllocBlock next;			/* next block in aset's blocks list */
  char	   *freeptr;		/* start of free space in this block */
  char	   *endptr;			/* end of space in this block */
}	SHMAllocBlockData;

/*
 * AllocChunk
 *		The prefix of each piece of memory in an AllocBlock
 *
 * NB: this MUST match StandardChunkHeader as defined by utils/memutils.h.
 */
typedef struct SHMAllocChunkData
{
  /* aset is the owning aset if allocated, or the freelist link if free */
  void	   *aset;
  /* size is always the size of the usable space in the chunk */
  Size		size;
#ifdef MEMORY_CONTEXT_CHECKING
  /* when debugging memory usage, also store actual requested size */
  /* this is zero in a free chunk */
  Size		requested_size;
#endif
}	SHMAllocChunkData;

/*
 * AllocPointerIsValid
 *		True iff pointer is valid allocation pointer.
 */
#define SHMAllocPointerIsValid(pointer) SHMPointerIsValid(pointer)

/*
 * AllocSetIsValid
 *		True iff set is valid allocation set.
 */
#define SHMAllocSetIsValid(set) SHMPointerIsValid(set)

#define SHMAllocPointerGetChunk(ptr)	\
    ((SHMAllocChunk)(((char *)(ptr)) - SHMALLOC_CHUNKHDRSZ))
#define SHMAllocChunkGetPointer(chk)	\
    ((SHMAllocPointer)(((char *)(chk)) + SHMALLOC_CHUNKHDRSZ))

/*
 * These functions implement the MemoryContext API for AllocSet contexts.
 */
static void *SHMAllocSetAlloc(MemoryContext context, Size size);
static void SHMAllocSetFree(MemoryContext context, void *pointer);
static void *SHMAllocSetRealloc(MemoryContext context, void *pointer, Size size);
static void SHMAllocSetInit(MemoryContext context);
static void SHMAllocSetReset(MemoryContext context);
static void SHMAllocSetDelete(MemoryContext context);
static Size SHMAllocSetGetChunkSpace(MemoryContext context, void *pointer);
static bool SHMAllocSetIsEmpty(MemoryContext context);
static void SHMAllocSetStats(MemoryContext context, int level);

#ifdef MEMORY_CONTEXT_CHECKING
static void SHMAllocSetCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for SHMAllocSet contexts.
 */
static MemoryContextMethods SHMAllocSetMethods = {
    SHMAllocSetAlloc,
    SHMAllocSetFree,
    SHMAllocSetRealloc,
    SHMAllocSetInit,
    SHMAllocSetReset,
    SHMAllocSetDelete,
    SHMAllocSetGetChunkSpace,
    SHMAllocSetIsEmpty,
    SHMAllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
  ,SHMAllocSetCheck
#endif
};


/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_SHMALLOCINFO
#define SHMAllocFreeInfo(_cxt, _chunk) \
    fprintf(stderr, "SHMAllocFree: %s: %p, %d\n", \
            (_cxt)->header.name, (_chunk), (_chunk)->size)
#define SHMAllocAllocInfo(_cxt, _chunk) \
    fprintf(stderr, "SHMAllocAlloc: %s: %p, %d\n", \
            (_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define SHMAllocFreeInfo(_cxt, _chunk)
#define SHMAllocAllocInfo(_cxt, _chunk)
#endif

/* ----------
 * SHMAllocSetFreeIndex -
 *
 *		Depending on the size of an allocation compute which freechunk
 *		list of the alloc set it belongs to.  Caller must have verified
 *		that size <= SHMALLOC_CHUNK_LIMIT.
 * ----------
 */
static inline int
SHMAllocSetFreeIndex(Size size)
{
  int			idx = 0;

  if (size > 0)
  {
    size = (size - 1) >> SHMALLOC_MINBITS;
    while (size != 0)
    {
      idx++;
      size >>= 1;
    }
    Assert(idx < SHMALLOCSET_NUM_FREELISTS);
  }

  return idx;
}


/*
 * Public routines
 */

/*
 * SHMAllocSetContextCreate
 *		Create a new SHMAllocSet context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 * minContextSize: minimum context size
 * initBlockSize: initial allocation block size
 * maxBlockSize: maximum allocation block size
 */
MemoryContext
SHMAllocSetContextCreate(MemoryContext parent,
                         const char *name,
                         Size minContextSize,
                         Size initBlockSize,
                         Size maxBlockSize,
                         MM * shmcxt)
{
  SHMAllocSet context;


  /* Do the type-independent part of context creation */
  context = (SHMAllocSet) SHMContextCreate(T_SHMAllocSetContext,
                                           sizeof(SHMAllocSetContext),
                                           &SHMAllocSetMethods,
                                           parent,
                                           name,
                                           shmcxt);

  /*
   * Make sure alloc parameters are reasonable, and save them.
   *
   * We somewhat arbitrarily enforce a minimum 1K block size.
   */
  initBlockSize = MAXALIGN(initBlockSize);
  if (initBlockSize < 1024)
    initBlockSize = 1024;
  maxBlockSize = MAXALIGN(maxBlockSize);
  if (maxBlockSize < initBlockSize)
    maxBlockSize = initBlockSize;
  context->initBlockSize = initBlockSize;
  context->maxBlockSize = maxBlockSize;

  /*
   * Grab always-allocated space, if requested
   */
  if (minContextSize > SHMALLOC_BLOCKHDRSZ + SHMALLOC_CHUNKHDRSZ)
  {
    Size		blksize = MAXALIGN(minContextSize);
    SHMAllocBlock block;

    block = (SHMAllocBlock) mm_malloc(shmcxt, blksize);
    if (block == NULL)
    {
      SHMContextStats(TopSharedMemoryContext);
      elog(ERROR, "Memory exhausted in SHMAllocSetContextCreate(%lu)",
           (unsigned long) minContextSize);
    }
    block->aset = context;
    block->freeptr = ((char *) block) + SHMALLOC_BLOCKHDRSZ;
    block->endptr = ((char *) block) + blksize;
    block->next = context->blocks;
    context->blocks = block;
    /* Mark block as not to be released at reset time */
    context->keeper = block;
  }


  return (MemoryContext) context;
}

/*
 * SHMAllocSetInit
 *		Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (SHMAllocSetContextCreate can allocate memory when it gets control
 * back, however.)
 */
static void
SHMAllocSetInit(MemoryContext context)
{
  /*
   * Since MemoryContextCreate already zeroed the context node, we don't
   * have to do anything here: it's already OK.
   */
}

/*
 * SHMAllocSetReset
 *		Frees all memory which is allocated in the given set.
 *
 * Actually, this routine has some discretion about what to do.
 * It should mark all allocated chunks freed, but it need not
 * necessarily give back all the resources the set owns.  Our
 * actual implementation is that we hang on to any "keeper"
 * block specified for the set.
 */
static void
SHMAllocSetReset(MemoryContext context)
{
  MM		   *mmcxt = SHMFindMMContext(context);
  SHMAllocSet set = NULL;
  SHMAllocBlock block = NULL;

  mm_lock(mmcxt, MM_LOCK_RW);
  set = (SHMAllocSet) context;
  block = set->blocks;
  AssertArg(SHMAllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
  /* Check for corruption and leaks before freeing */
  SHMAllocSetCheck(context);
#endif

  /* Clear chunk freelists */
  MemSet(set->freelist, 0, sizeof(set->freelist));
  /* New blocks list is either empty or just the keeper block */
  set->blocks = set->keeper;

  while (block != NULL)
  {
    SHMAllocBlock next = block->next;

    if (block == set->keeper)
    {
      /* Reset the block, but don't return it to malloc */
      char	   *datastart = ((char *) block) + SHMALLOC_BLOCKHDRSZ;

#ifdef CLOBBER_FREED_MEMORY
      /* Wipe freed memory for debugging purposes */
      memset(datastart, 0x7F, block->freeptr - datastart);
#endif
      block->freeptr = datastart;
      block->next = NULL;
    }
    else
    {
      /* Normal case, release the block */
#ifdef CLOBBER_FREED_MEMORY
      /* Wipe freed memory for debugging purposes */
      memset(block, 0x7F, block->freeptr - ((char *) block));
#endif
      mm_free(mmcxt, block);
    }
    block = next;
  }
  mm_unlock(mmcxt);
}

/*
 * SHMAllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 *
 * Unlike SHMAllocSetReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
static void
SHMAllocSetDelete(MemoryContext context)
{
  MM		   *mmcxt = SHMFindMMContext(context);

  SHMAllocSet set = NULL;
  SHMAllocBlock block = NULL;

  mm_lock(mmcxt, MM_LOCK_RW);
  set = (SHMAllocSet) context;
  block = set->blocks;
  AssertArg(SHMAllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
  /* Check for corruption and leaks before freeing */
  SHMAllocSetCheck(context);
#endif

  /* Make it look empty, just in case... */
  MemSet(set->freelist, 0, sizeof(set->freelist));
  set->blocks = NULL;
  set->keeper = NULL;

  while (block != NULL)
  {
    SHMAllocBlock next = block->next;

#ifdef CLOBBER_FREED_MEMORY
    /* Wipe freed memory for debugging purposes */
    memset(block, 0x7F, block->freeptr - ((char *) block));
#endif
    mm_free(mmcxt, block);
    block = next;
  }
  mm_unlock(mmcxt);
}

/*
 * SHMAllocSetAlloc
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 */
static void *
SHMAllocSetAlloc(MemoryContext context, Size size)
{
  MM		   *mmcxt = SHMFindMMContext(context);
  void	   *ret = NULL;
  SHMAllocSet set = (SHMAllocSet) context;
  SHMAllocBlock block;
  SHMAllocChunk chunk;
  SHMAllocChunk priorfree;
  int			fidx;
  Size		chunk_size;
  Size		blksize;

  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(SHMAllocSetIsValid(set));

  /*
   * If requested size exceeds maximum for chunks, allocate an entire
   * block for this request.
   */
  if (size > SHMALLOC_CHUNK_LIMIT)
  {
    chunk_size = MAXALIGN(size);
    blksize = chunk_size + SHMALLOC_BLOCKHDRSZ + SHMALLOC_CHUNKHDRSZ;
    block = (SHMAllocBlock) mm_malloc(mmcxt, blksize);
    if (block == NULL)
    {
      SHMContextStats(TopSharedMemoryContext);
      elog(ERROR, "Memory exhausted in SHMAllocSetAlloc(%lu)",
           (unsigned long) size);
    }
    block->aset = set;
    block->freeptr = block->endptr = ((char *) block) + blksize;

    chunk = (SHMAllocChunk) (((char *) block) + SHMALLOC_BLOCKHDRSZ);
    chunk->aset = set;
    chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    /* set mark to catch clobber of "unused" space */
    if (size < chunk_size)
      ((char *) SHMAllocChunkGetPointer(chunk))[size] = 0x7E;
#endif

    /*
     * Stick the new block underneath the active allocation block, so
     * that we don't lose the use of the space remaining therein.
     */
    if (set->blocks != NULL)
    {
      block->next = set->blocks->next;
      set->blocks->next = block;
    }
    else
    {
      block->next = NULL;
      set->blocks = block;
    }

    SHMAllocAllocInfo(set, chunk);
    ret = SHMAllocChunkGetPointer(chunk);
    mm_unlock(mmcxt);
    return ret;
  }

  /*
   * Request is small enough to be treated as a chunk.  Look in the
   * corresponding free list to see if there is a free chunk we could
   * reuse.
   */
  fidx = SHMAllocSetFreeIndex(size);
  priorfree = NULL;
  for (chunk = set->freelist[fidx]; chunk; chunk = (SHMAllocChunk) chunk->aset)
  {
    if (chunk->size >= size)
      break;
    priorfree = chunk;
  }

  /*
   * If one is found, remove it from the free list, make it again a
   * member of the alloc set and return its data address.
   */
  if (chunk != NULL)
  {
    if (priorfree == NULL)
      set->freelist[fidx] = (SHMAllocChunk) chunk->aset;
    else
      priorfree->aset = chunk->aset;

    chunk->aset = (void *) set;

#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    /* set mark to catch clobber of "unused" space */
    if (size < chunk->size)
      ((char *) SHMAllocChunkGetPointer(chunk))[size] = 0x7E;
#endif

    SHMAllocAllocInfo(set, chunk);
    ret = SHMAllocChunkGetPointer(chunk);
    mm_unlock(mmcxt);
    return ret;
  }

  /*
   * Choose the actual chunk size to allocate.
   */
  chunk_size = 1 << (fidx + SHMALLOC_MINBITS);
  Assert(chunk_size >= size);

  /*
   * If there is enough room in the active allocation block, we will put
   * the chunk into that block.  Else must start a new one.
   */
  if ((block = set->blocks) != NULL)
  {
    Size		availspace = block->endptr - block->freeptr;

    if (availspace < (chunk_size + SHMALLOC_CHUNKHDRSZ))
    {
      /*
       * The existing active (top) block does not have enough room
       * for the requested allocation, but it might still have a
       * useful amount of space in it.  Once we push it down in the
       * block list, we'll never try to allocate more space from it.
       * So, before we do that, carve up its free space into chunks
       * that we can put on the set's freelists.
       *
       * Because we can only get here when there's less than
       * SHMALLOC_CHUNK_LIMIT left in the block, this loop cannot
       * iterate more than ALLOCSET_NUM_FREELISTS-1 times.
       */
      while (availspace >= ((1 << SHMALLOC_MINBITS) + SHMALLOC_CHUNKHDRSZ))
      {
        Size		availchunk = availspace - SHMALLOC_CHUNKHDRSZ;
        int			a_fidx = SHMAllocSetFreeIndex(availchunk);

        /*
         * In most cases, we'll get back the index of the next
         * larger freelist than the one we need to put this chunk
         * on.	The exception is when availchunk is exactly a
         * power of 2.
         */
        if (availchunk != (1 << (a_fidx + SHMALLOC_MINBITS)))
        {
          a_fidx--;
          Assert(a_fidx >= 0);
          availchunk = (1 << (a_fidx + SHMALLOC_MINBITS));
        }

        chunk = (SHMAllocChunk) (block->freeptr);

        block->freeptr += (availchunk + SHMALLOC_CHUNKHDRSZ);
        availspace -= (availchunk + SHMALLOC_CHUNKHDRSZ);

        chunk->size = availchunk;
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = 0;		/* mark it free */
#endif
        chunk->aset = (void *) set->freelist[a_fidx];
        set->freelist[a_fidx] = chunk;
      }

      /* Mark that we need to create a new block */
      block = NULL;
    }
  }

  /*
   * Time to create a new regular (multi-chunk) block?
   */
  if (block == NULL)
  {
    Size		required_size;

    if (set->blocks == NULL)
    {
      /* First block of the alloc set, use initBlockSize */
      blksize = set->initBlockSize;
    }
    else
    {
      /* Get size of prior block */
      blksize = set->blocks->endptr - ((char *) set->blocks);

      /*
       * Special case: if very first allocation was for a large
       * chunk (or we have a small "keeper" block), could have an
       * undersized top block.  Do something reasonable.
       */
      if (blksize < set->initBlockSize)
        blksize = set->initBlockSize;
      else
      {
        /* Crank it up, but not past max */
        blksize <<= 1;
        if (blksize > set->maxBlockSize)
          blksize = set->maxBlockSize;
      }
    }

    /*
     * If initBlockSize is less than SHMALLOC_CHUNK_LIMIT, we could
     * need more space...
     */
    required_size = chunk_size + SHMALLOC_BLOCKHDRSZ + SHMALLOC_CHUNKHDRSZ;
    if (blksize < required_size)
      blksize = required_size;

    /* Try to allocate it */
    block = (SHMAllocBlock) mm_malloc(mmcxt, blksize);

    /*
     * We could be asking for pretty big blocks here, so cope if
     * malloc fails.  But give up if there's less than a meg or so
     * available...
     */
    while (block == NULL && blksize > 1024 * 1024)
    {
      blksize >>= 1;
      if (blksize < required_size)
        break;
      block = (SHMAllocBlock) mm_malloc(mmcxt, blksize);
    }

    if (block == NULL)
    {
      SHMContextStats(TopSharedMemoryContext);
      elog(ERROR, "Memory exhausted in SHMAllocSetAlloc(%lu)",
           (unsigned long) size);
    }

    block->aset = set;
    block->freeptr = ((char *) block) + SHMALLOC_BLOCKHDRSZ;
    block->endptr = ((char *) block) + blksize;

    block->next = set->blocks;
    set->blocks = block;
  }

  /*
   * OK, do the allocation
   */
  chunk = (SHMAllocChunk) (block->freeptr);

  block->freeptr += (chunk_size + SHMALLOC_CHUNKHDRSZ);
  Assert(block->freeptr <= block->endptr);

  chunk->aset = (void *) set;
  chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
  chunk->requested_size = size;
  /* set mark to catch clobber of "unused" space */
  if (size < chunk->size)
    ((char *) SHMAllocChunkGetPointer(chunk))[size] = 0x7E;
#endif

  SHMAllocAllocInfo(set, chunk);
  ret = SHMAllocChunkGetPointer(chunk);
  mm_unlock(mmcxt);
  return ret;
}

/*
 * SHMAllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
static void
SHMAllocSetFree(MemoryContext context, void *pointer)
{
  MM		   *mmcxt = SHMFindMMContext(context);

  SHMAllocSet set = NULL;
  SHMAllocChunk chunk = NULL;

  mm_lock(mmcxt, MM_LOCK_RW);
  set = (SHMAllocSet) context;
  chunk = SHMAllocPointerGetChunk(pointer);
  SHMAllocFreeInfo(set, chunk);

#ifdef MEMORY_CONTEXT_CHECKING
  /* Test for someone scribbling on unused space in chunk */
  if (chunk->requested_size < chunk->size)
    if (((char *) pointer)[chunk->requested_size] != 0x7E)
      elog(NOTICE, "SHMAllocSetFree: detected write past chunk end in %s %p",
           set->header.name, chunk);
#endif

  if (chunk->size > SHMALLOC_CHUNK_LIMIT)
  {
    /*
     * Big chunks are certain to have been allocated as single-chunk
     * blocks.	Find the containing block and return it to malloc().
     */
    SHMAllocBlock block = set->blocks;
    SHMAllocBlock prevblock = NULL;

    while (block != NULL)
    {
      if (chunk == (SHMAllocChunk) (((char *) block) + SHMALLOC_BLOCKHDRSZ))
        break;
      prevblock = block;
      block = block->next;
    }
    if (block == NULL)
      elog(ERROR, "SHMAllocSetFree: cannot find block containing chunk %p",
           chunk);
    /* let's just make sure chunk is the only one in the block */
    Assert(block->freeptr == ((char *) block) +
           (chunk->size + SHMALLOC_BLOCKHDRSZ + SHMALLOC_CHUNKHDRSZ));

    /* OK, remove block from aset's list and free it */
    if (prevblock == NULL)
      set->blocks = block->next;
    else
      prevblock->next = block->next;
#ifdef CLOBBER_FREED_MEMORY
    /* Wipe freed memory for debugging purposes */
    memset(block, 0x7F, block->freeptr - ((char *) block));
#endif
    mm_free(mmcxt, block);
  }
  else
  {
    /* Normal case, put the chunk into appropriate freelist */
    int			fidx = SHMAllocSetFreeIndex(chunk->size);

    chunk->aset = (void *) set->freelist[fidx];

#ifdef CLOBBER_FREED_MEMORY
    /* Wipe freed memory for debugging purposes */
    memset(pointer, 0x7F, chunk->size);
#endif

#ifdef MEMORY_CONTEXT_CHECKING
    /* Reset requested_size to 0 in chunks that are on freelist */
    chunk->requested_size = 0;
#endif
    set->freelist[fidx] = chunk;
  }
  mm_unlock(mmcxt);
}

/*
 * SHMAllocSetRealloc
 *		Returns new pointer to allocated memory of given size; this memory
 *		is added to the set.  Memory associated with given pointer is copied
 *		into the new memory, and the old memory is freed.
 */
static void *
SHMAllocSetRealloc(MemoryContext context, void *pointer, Size size)
{
  MM		   *mmcxt = SHMFindMMContext(context);
  void	   *ret = NULL;
  SHMAllocSet set = NULL;
  SHMAllocChunk chunk = NULL;
  Size		oldsize = (Size) NULL;

  mm_lock(mmcxt, MM_LOCK_RW);
  set = (SHMAllocSet) context;
  chunk = SHMAllocPointerGetChunk(pointer);
  oldsize = chunk->size;



#ifdef MEMORY_CONTEXT_CHECKING
  /* Test for someone scribbling on unused space in chunk */
  if (chunk->requested_size < oldsize)
    if (((char *) pointer)[chunk->requested_size] != 0x7E)
      elog(NOTICE, "SHMAllocSetRealloc: detected write past chunk end in %s %p",
           set->header.name, chunk);
#endif

  /*
   * Chunk sizes are aligned to power of 2 in SHMAllocSetAlloc(). Maybe
   * the allocated area already is >= the new size.  (In particular, we
   * always fall out here if the requested size is a decrease.)
   */
  if (oldsize >= size)
  {
#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    /* set mark to catch clobber of "unused" space */
    if (size < oldsize)
      ((char *) pointer)[size] = 0x7E;
#endif
    mm_unlock(mmcxt);
    return pointer;
  }

  if (oldsize > SHMALLOC_CHUNK_LIMIT)
  {
    /*
     * The chunk must been allocated as a single-chunk block.  Find
     * the containing block and use realloc() to make it bigger with
     * minimum space wastage.
     */
    SHMAllocBlock block = set->blocks;
    SHMAllocBlock prevblock = NULL;
    Size		chksize;
    Size		blksize;

    while (block != NULL)
    {
      if (chunk == (SHMAllocChunk) (((char *) block) + SHMALLOC_BLOCKHDRSZ))
        break;
      prevblock = block;
      block = block->next;
    }
    if (block == NULL)
      elog(ERROR, "SHMAllocSetRealloc: cannot find block containing chunk %p",
           chunk);
    /* let's just make sure chunk is the only one in the block */
    Assert(block->freeptr == ((char *) block) +
           (chunk->size + SHMALLOC_BLOCKHDRSZ + SHMALLOC_CHUNKHDRSZ));

    /* Do the realloc */
    chksize = MAXALIGN(size);
    blksize = chksize + SHMALLOC_BLOCKHDRSZ + SHMALLOC_CHUNKHDRSZ;
    block = (SHMAllocBlock) mm_realloc(mmcxt, block, blksize);
    if (block == NULL)
    {
      SHMContextStats(TopSharedMemoryContext);
      elog(ERROR, "Memory exhausted in SHMAllocSetReAlloc(%lu)",
           (unsigned long) size);
    }
    block->freeptr = block->endptr = ((char *) block) + blksize;

    /* Update pointers since block has likely been moved */
    chunk = (SHMAllocChunk) (((char *) block) + SHMALLOC_BLOCKHDRSZ);
    if (prevblock == NULL)
      set->blocks = block;
    else
      prevblock->next = block;
    chunk->size = chksize;

#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    /* set mark to catch clobber of "unused" space */
    if (size < chunk->size)
      ((char *) SHMAllocChunkGetPointer(chunk))[size] = 0x7E;
#endif


    ret = SHMAllocChunkGetPointer(chunk);
    mm_unlock(mmcxt);
    return ret;
  }
  else
  {
    /*
     * Small-chunk case.  If the chunk is the last one in its block,
     * there might be enough free space after it that we can just
     * enlarge the chunk in-place.	It's relatively painful to find
     * the containing block in the general case, but we can detect
     * last-ness quite cheaply for the typical case where the chunk is
     * in the active (topmost) allocation block.  (At least with the
     * regression tests and code as of 1/2001, realloc'ing the last
     * chunk of a non-topmost block hardly ever happens, so it's not
     * worth scanning the block list to catch that case.)
     *
     * NOTE: must be careful not to create a chunk of a size that
     * SHMAllocSetAlloc would not create, else we'll get confused
     * later.
     */
    SHMAllocPointer newPointer;

    if (size <= SHMALLOC_CHUNK_LIMIT)
    {
      SHMAllocBlock block = set->blocks;
      char	   *chunk_end;

      chunk_end = (char *) chunk + (oldsize + SHMALLOC_CHUNKHDRSZ);
      if (chunk_end == block->freeptr)
      {
        /* OK, it's last in block ... is there room? */
        Size		freespace = block->endptr - block->freeptr;
        int			fidx;
        Size		newsize;
        Size		delta;

        fidx = SHMAllocSetFreeIndex(size);
        newsize = 1 << (fidx + SHMALLOC_MINBITS);
        Assert(newsize >= oldsize);
        delta = newsize - oldsize;
        if (freespace >= delta)
        {
          /* Yes, so just enlarge the chunk. */
          block->freeptr += delta;
          chunk->size += delta;
#ifdef MEMORY_CONTEXT_CHECKING
          chunk->requested_size = size;
          /* set mark to catch clobber of "unused" space */
          if (size < chunk->size)
            ((char *) pointer)[size] = 0x7E;
#endif
          mm_unlock(mmcxt);
          return pointer;
        }
      }
    }

    /* Normal small-chunk case: just do it by brute force. */

    /* allocate new chunk */
    newPointer = SHMAllocSetAlloc((MemoryContext) set, size);

    /* transfer existing data (certain to fit) */
    memcpy(newPointer, pointer, oldsize);

    /* free old chunk */
    SHMAllocSetFree((MemoryContext) set, pointer);

    mm_unlock(mmcxt);
    return newPointer;
  }

}

/*
 * SHMAllocSetGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
SHMAllocSetGetChunkSpace(MemoryContext context, void *pointer)
{
  SHMAllocChunk chunk;
  MM		   *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);
  chunk = SHMAllocPointerGetChunk(pointer);
  mm_unlock(mmcxt);

  return chunk->size + SHMALLOC_CHUNKHDRSZ;
}

/*
 * SHMAllocSetIsEmpty
 *    Is an SHMallocset empty of any allocated space?
 */
static bool
SHMAllocSetIsEmpty(MemoryContext context)
{
  /*
   * For now, we say "empty" only if the context is new or just reset. We
   * could examine the freelists to determine if all space has been freed,
   * but it's not really worth the trouble for present uses of this
   * functionality.
   */
  if (context->isReset)
    return true;
  return false;
}

/*
 * SHMAllocSetStats
 *		Displays stats about memory consumption of an allocset.
 */
static void
SHMAllocSetStats(MemoryContext context, int level)
{
  MM		   *mmcxt = SHMFindMMContext(context);

  SHMAllocSet set = (SHMAllocSet) context;
  long		nblocks = 0;
  long		nchunks = 0;
  long		totalspace = 0;
  long		freespace = 0;
  SHMAllocBlock block;
  SHMAllocChunk chunk;
  int			fidx;
  int     i;

  mm_lock(mmcxt, MM_LOCK_RW);

  for (block = set->blocks; block != NULL; block = block->next)
  {
    nblocks++;
    totalspace += block->endptr - ((char *) block);
    freespace += block->endptr - block->freeptr;
  }
  for (fidx = 0; fidx < SHMALLOCSET_NUM_FREELISTS; fidx++)
  {
    for (chunk = set->freelist[fidx]; chunk != NULL;
        chunk = (SHMAllocChunk) chunk->aset)
    {
      nchunks++;
      freespace += chunk->size + SHMALLOC_CHUNKHDRSZ;
    }
  }

  for (i = 0; i < level; i++)
    fprintf(stderr, "  ");

  fprintf(stderr,
          "%s: %ld total in %ld blocks; %ld free (%ld chunks); %ld used\n",
          set->header.name, totalspace, nblocks, freespace, nchunks,
          totalspace - freespace);

  mm_unlock(mmcxt);
}

#ifdef MEMORY_CONTEXT_CHECKING

/*
 * SHMAllocSetCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as NOTICE, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
SHMAllocSetCheck(MemoryContext context)
{
  MM		   *mmcxt = SHMFindMMContext(context);
  SHMAllocSet set = (SHMAllocSet) context;
  char	   *name = set->header.name;
  SHMAllocBlock block;

  mm_lock(mmcxt, MM_LOCK_RW);

  for (block = set->blocks; block != NULL; block = block->next)
  {
    char	   *bpoz = ((char *) block) + SHMALLOC_BLOCKHDRSZ;
    long		blk_used = block->freeptr - bpoz;
    long		blk_data = 0;
    long		nchunks = 0;

    /*
     * Empty block - empty can be keeper-block only
     */
    if (!blk_used)
    {
      if (set->keeper != block)
        elog(NOTICE, "SHMAllocSetCheck: %s: empty block %p",
             name, block);
    }

    /*
     * Chunk walker
     */
    while (bpoz < block->freeptr)
    {
      SHMAllocChunk chunk = (SHMAllocChunk) bpoz;
      Size		chsize,
      dsize;
      char	   *chdata_end;

      chsize = chunk->size;		/* aligned chunk size */
      dsize = chunk->requested_size;		/* real data */
      chdata_end = ((char *) chunk) + (SHMALLOC_CHUNKHDRSZ + dsize);

      /*
       * Check chunk size
       */
      if (dsize > chsize)
        elog(NOTICE, "SHMAllocSetCheck: %s: req size > alloc size for chunk %p in block %p",
             name, chunk, block);
      if (chsize < (1 << SHMALLOC_MINBITS))
        elog(NOTICE, "SHMAllocSetCheck: %s: bad size %lu for chunk %p in block %p",
             name, (unsigned long) chsize, chunk, block);

      /* single-chunk block? */
      if (chsize > SHMALLOC_CHUNK_LIMIT &&
          chsize + SHMALLOC_CHUNKHDRSZ != blk_used)
        elog(NOTICE, "SHMAllocSetCheck: %s: bad single-chunk %p in block %p",
             name, chunk, block);

      /*
       * If chunk is allocated, check for correct aset pointer. (If
       * it's free, the aset is the freelist pointer, which we can't
       * check as easily...)
       */
      if (dsize > 0 && chunk->aset != (void *) set)
        elog(NOTICE, "SHMAllocSetCheck: %s: bogus aset link in block %p, chunk %p",
             name, block, chunk);

      /*
       * Check for overwrite of "unallocated" space in chunk
       */
      if (dsize > 0 && dsize < chsize && *chdata_end != 0x7E)
        elog(NOTICE, "SHMAllocSetCheck: %s: detected write past chunk end in block %p, chunk %p",
             name, block, chunk);

      blk_data += chsize;
      nchunks++;

      bpoz += SHMALLOC_CHUNKHDRSZ + chsize;
    }

    if ((blk_data + (nchunks * SHMALLOC_CHUNKHDRSZ)) != blk_used)
      elog(NOTICE, "SHMAllocSetCheck: %s: found inconsistent memory block %p",
           name, block);
  }
  mm_unlock(mmcxt);
}

#endif   /* MEMORY_CONTEXT_CHECKING */
