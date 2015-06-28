/*-------------------------------------------------------------------------
 *
 * shmcxt.c
 *
 *  POSTGRES memory context management code.
 *
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 1996-2001, PostgreSQL Global Development Group
 * Portions Copyright (c) 2003, Regents of the University of California
 * Portions Copyright (c) 2015, CMU
 *
 * IDENTIFICATION
 *    src/backend/utils/mmgr/shmctx.c
 *
 *
 * This module handles context management operations that are independent
 * of the particular kind of context being operated on.  It calls
 * context-type-specific operations via the function pointers in a
 * context's MemoryContextMethods struct.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/memnodes.h"
#include "utils/memutils.h"
#include "storage/spin.h"

#include <mm.h>

MM  *mm_query_segment = NULL;

/*
 * These functions implement the MemoryContext API for Shared Memory contexts.
 */
MemoryContext SHMContextCreate(NodeTag tag, Size size,
                 MemoryContextMethods *methods,
                 MemoryContext parent,
                 const char *name, MM * shmctx);
MM * SHMFindMMContext(MemoryContext ac);
void SHMContextShutdown(void);
void SHMContextInit(void);
void SHMContextReset(MemoryContext context);
void SHMContextResetChildren(MemoryContext context);
void SHMContextDelete(MemoryContext context);
void SHMContextDeleteChildren(MemoryContext context);
void SHMContextResetAndDeleteChildren(MemoryContext context);
bool SHMContextContains(MemoryContext context, void *pointer);
void SHMContextStats(MemoryContext context);

static void SHMContextStatsInternal(MemoryContext context, int level);

#ifdef MEMORY_CONTEXT_CHECKING
void SHMContextCheck(MemoryContext context);
#endif

/*--------------------
 * MemoryContextCreate
 *    Context-type-independent part of context creation.
 *
 * This is only intended to be called by context-type-specific
 * context creation routines, not by the unwashed masses.
 *
 * The context creation procedure is a little bit tricky because
 * we want to be sure that we don't leave the context tree invalid
 * in case of failure (such as insufficient memory to allocate the
 * context node itself).  The procedure goes like this:
 *  1.  Context-type-specific routine first calls MemoryContextCreate(),
 *    passing the appropriate tag/size/methods values (the methods
 *    pointer will ordinarily point to statically allocated data).
 *    The parent and name parameters usually come from the caller.
 *  2.  MemoryContextCreate() attempts to allocate the context node,
 *    plus space for the name.  If this fails we can elog() with no
 *    damage done.
 *  3.  We fill in all of the type-independent MemoryContext fields.
 *  4.  We call the type-specific init routine (using the methods pointer).
 *    The init routine is required to make the node minimally valid
 *    with zero chance of failure --- it can't allocate more memory,
 *    for example.
 *  5.  Now we have a minimally valid node that can behave correctly
 *    when told to reset or delete itself.  We link the node to its
 *    parent (if any), making the node part of the context tree.
 *  6.  We return to the context-type-specific routine, which finishes
 *    up type-specific initialization.  This routine can now do things
 *    that might fail (like allocate more memory), so long as it's
 *    sure the node is left in a state that delete will handle.
 *
 * This protocol doesn't prevent us from leaking memory if step 6 fails
 * during creation of a top-level context, since there's no parent link
 * in that case.  However, if you run out of memory while you're building
 * a top-level context, you might as well go home anyway...
 *
 * Normally, the context node and the name are allocated from
 * TopMemoryContext (NOT from the parent context, since the node must
 * survive resets of its parent context!).  However, this routine is itself
 * used to create TopMemoryContext!  If we see that TopMemoryContext is NULL,
 * we assume we are creating TopMemoryContext and use malloc() to allocate
 * the node.
 *
 * Note that the name field of a MemoryContext does not point to
 * separately-allocated storage, so it should not be freed at context
 * deletion.
 *--------------------
 */
MemoryContext
SHMContextCreate(NodeTag tag, Size size,
                 MemoryContextMethods *methods,
                 MemoryContext parent,
                 const char *name, MM * shmctx)
{
  MemoryContext node = NULL;
  Size    needed = size + strlen(name) + 1;

  /* Get space for node and name */
  if (tag == T_SHMAllocSetContext)
  {
    /* OC: if this is a shared memory context, then malloc its */
    /* memory context in shared memory */
    if (!shmctx)
    {
      elog(ERROR, "MemoryContextCreate: mm shared memory pool not yet created");
      return NULL;
    }
    if (TopSharedMemoryContext)
    {
      MemoryContext old = MemoryContextSwitchTo(TopSharedMemoryContext);

      node = (MemoryContext) palloc(needed);
      MemoryContextSwitchTo(old);
    }
    else
    {
      /* warning -- danger -- this context will CANNOT be freed */
      /* MemoryContextDelete exprects a block alloced with palloc */
      node = (MemoryContext) mm_malloc(shmctx, needed);

    }
  }
  else
    Assert(false);

  /* Initialize the node as best we can */
  MemSet(node, 0, size);
  node->type = tag;
  node->methods = methods;
  node->parent = NULL;    /* for the moment */
  node->firstchild = NULL;
  node->nextchild = NULL;
  node->name = ((char *) node) + size;
  strcpy(node->name, name);

  /* Type-specific routine finishes any other essential initialization */
  (*node->methods->init) (node);

  /* OK to link node to parent (if any) */
  if (parent)
  {
    node->parent = parent;
    node->nextchild = parent->firstchild;
    parent->firstchild = node;
  }

  /* Return to type-specific creation routine to finish up */
  return node;
}

MM *
SHMFindMMContext(MemoryContext ac)
{
  return mm_query_segment;
}

/*
 * SHMMemoryContextInit
 *	OC: this routine initailizes the shared memory contexts used
 *	by the C telegraph code.  Please see shmset.c for more information
 *	on the strategy.
 *
 *	This routine:
 *	allocates an MM shared memory segment to use for bookkeeping info.	data
 *	should not be placed in this segment.
 *
 *	allocates a query shared memory segment to be used to hold query plans
 *
 */
void
SHMContextShutdown(void)
{
  if (TopSharedMemoryContext == NULL ||
      mm_query_segment == NULL)
    return;

  SHMContextReset(TopSharedMemoryContext);
  TopSharedMemoryContext = NULL;
  mm_destroy(mm_query_segment);
  mm_query_segment = NULL;
}

void
SHMContextInit(void)
{
  char		mmquerysegname[100];
  //int			pid = 0;

  if (mm_query_segment)
    return;

  //pid = getpid();
  //sprintf(mmquerysegname, "/tmp/mm_query_segment_%d", pid);
  sprintf(mmquerysegname, "/tmp/shm.peloton");

  mm_query_segment = mm_create(20 * 1024 * 1024, mmquerysegname);
  if (!mm_query_segment)
  {
    elog(ERROR, "couldn't allocate mm_query_segment");
  }

  TopSharedMemoryContext = SHMAllocSetContextCreate((MemoryContext) NULL,
                                                    "TopSharedMemoryContext",
                                                    8 * 1024,
                                                    8 * 1024,
                                                    8 * 1024,
                                                    mm_query_segment);
}

/*
 * SHMContextReset
 *		Release all space allocated within a context and its descendants,
 *		but don't delete the contexts themselves.
 *
 * The type-specific reset routine handles the context itself, but we
 * have to do the recursion for the children.
 */
void
SHMContextReset(MemoryContext context)
{
  MM		   *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(MemoryContextIsValid(context));

  SHMContextResetChildren(context);
  (*context->methods->reset) (context);
  mm_unlock(mmcxt);
}

/*
 * MemoryContextResetChildren
 *		Release all space allocated within a context's descendants,
 *		but don't delete the contexts themselves.  The named context
 *		itself is not touched.
 */
void
SHMContextResetChildren(MemoryContext context)
{
  MM		   *mmcxt = SHMFindMMContext(context);

  MemoryContext child;

  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(MemoryContextIsValid(context));

  for (child = context->firstchild; child != NULL; child = child->nextchild)
    SHMContextReset(child);
  mm_unlock(mmcxt);
}

/*
 * MemoryContextDelete
 *		Delete a context and its descendants, and release all space
 *		allocated therein.
 *
 * The type-specific delete routine removes all subsidiary storage
 * for the context, but we have to delete the context node itself,
 * as well as recurse to get the children.	We must also delink the
 * node from its parent, if it has one.
 */
void
SHMContextDelete(MemoryContext context)
{
  MM		   *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(MemoryContextIsValid(context));
  /* We had better not be deleting TopMemoryContext ... */
  Assert(context != TopMemoryContext);
  /* And not CurrentMemoryContext, either */
  Assert(context != CurrentMemoryContext);

  SHMContextDeleteChildren(context);

  /*
   * We delink the context from its parent before deleting it, so that
   * if there's an error we won't have deleted/busted contexts still
   * attached to the context tree.  Better a leak than a crash.
   */
  if (context->parent)
  {
    MemoryContext parent = context->parent;

    if (context == parent->firstchild)
      parent->firstchild = context->nextchild;
    else
    {
      MemoryContext child;

      for (child = parent->firstchild; child; child = child->nextchild)
      {
        if (context == child->nextchild)
        {
          child->nextchild = context->nextchild;
          break;
        }
      }
    }
  }
  (*context->methods->delete_context) (context);
  pfree(context);

  mm_unlock(mmcxt);
}

/*
 * MemoryContextDeleteChildren
 *		Delete all the descendants of the named context and release all
 *		space allocated therein.  The named context itself is not touched.
 */
void
SHMContextDeleteChildren(MemoryContext context)
{
  MM		   *mmcxt = NULL;

  mmcxt = SHMFindMMContext(context);
  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(MemoryContextIsValid(context));

  /*
   * MemoryContextDelete will delink the child from me, so just iterate
   * as long as there is a child.
   */
  while (context->firstchild != NULL)
    SHMContextDelete(context->firstchild);
  mm_unlock(mmcxt);
}

/*
 * MemoryContextResetAndDeleteChildren
 *		Release all space allocated within a context and delete all
 *		its descendants.
 *
 * This is a common combination case where we want to preserve the
 * specific context but get rid of absolutely everything under it.
 */
void
SHMContextResetAndDeleteChildren(MemoryContext context)
{
  /* AssertArg(MemoryContextIsValid(context)); */
  MM		   *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);
  SHMContextDeleteChildren(context);
  (*context->methods->reset) (context);
  mm_unlock(mmcxt);
}

/*
 * MemoryContextStats
 *		Print statistics about the named context and all its descendants.
 *
 * This is just a debugging utility, so it's not fancy.  The statistics
 * are merely sent to stderr.
 */
void
SHMContextStats(MemoryContext context)
{
  SHMContextStatsInternal(context, 0);
}

static void
SHMContextStatsInternal(MemoryContext context, int level)
{
  MemoryContext child;
  MM       *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(MemoryContextIsValid(context));

  (*context->methods->stats) (context, level);
  for (child = context->firstchild; child != NULL; child = child->nextchild)
    SHMContextStatsInternal(child, level + 1);
  mm_unlock(mmcxt);
}

/*
 * MemoryContextContains
 *		Detect whether an allocated chunk of memory belongs to a given
 *		context or not.
 *
 * Caution: this test is reliable as long as 'pointer' does point to
 * a chunk of memory allocated from *some* context.  If 'pointer' points
 * at memory obtained in some other way, there is a small chance of a
 * false-positive result, since the bits right before it might look like
 * a valid chunk header by chance.
 */
bool
SHMContextContains(MemoryContext context, void *pointer)
{
  StandardChunkHeader *header;
  MM		   *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);


  /*
   * Try to detect bogus pointers handed to us, poorly though we can.
   * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
   * allocated chunk.
   */
  if (pointer == NULL || pointer != (void *) MAXALIGN(pointer))
  {
    mm_unlock(mmcxt);
    return false;
  }

  /*
   * OK, it's probably safe to look at the chunk header.
   */
  header = (StandardChunkHeader *)
		            ((char *) pointer - STANDARDCHUNKHEADERSIZE);

  /*
   * If the context link doesn't match then we certainly have a
   * non-member chunk.  Also check for a reasonable-looking size as
   * extra guard against being fooled by bogus pointers.
   */
  if (header->context == context && AllocSizeIsValid(header->size))
  {
    mm_unlock(mmcxt);
    return true;
  }
  mm_unlock(mmcxt);
  return false;
}


/*
 * MemoryContextCheck
 *    Check all chunks in the named context.
 *
 * This is just a debugging utility, so it's not fancy.
 */
#ifdef MEMORY_CONTEXT_CHECKING
void
SHMContextCheck(MemoryContext context)
{
  MemoryContext child;
  MM       *mmcxt = SHMFindMMContext(context);

  mm_lock(mmcxt, MM_LOCK_RW);
  AssertArg(MemoryContextIsValid(context));

  (*context->methods->check) (context);
  for (child = context->firstchild; child != NULL; child = child->nextchild)
    SHMContextCheck(child);
  mm_unlock(mmcxt);
}
#endif

