/*-------------------------------------------------------------------------
 *
 * relblock_io.h
 *	  POSTGRES relation block io utilities definitions.
 *
 *
 * src/include/access/relblock_io.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELBLOCK_H
#define RELBLOCK_H

#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf.h"
#include "access/heapam.h"

#define BLOCK_FIXED_LENGTH_SIZE 100             /* In terms of number of tuples */
#define BLOCK_VARIABLE_LENGTH_SIZE 1024*32      /* Raw size in bytes (Must be < 2^16)*/

#define BLOCK_POINTER_SIZE  8                   /* 8 bytes */
#define NUM_REL_BLOCK_ENTRIES 1000              /* Entries in shared rel block table */

#define RELBLOCK_CACHELINE_SIZE   16            /* 64 bytes */

// RelationBlock storage information
typedef enum RelationBlockBackend{
	STORAGE_BACKEND_FS,
	STORAGE_BACKEND_VM,
	STORAGE_BACKEND_NVM
} RelationBlockBackend;

#define STORAGE_BACKEND_DEFAULT STORAGE_BACKEND_FS

/* Possible block types */
typedef enum RelationBlockType
{
	/* Used to store fixed-length tuples */
	RELATION_FIXED_BLOCK_TYPE,
	/* Used to store variable-length attributes */
	RELATION_VARIABLE_BLOCK_TYPE
} RelationBlockType;

/* RelationBlock structure */
typedef struct RelationBlockData
{
	RelationBlockType rb_type;
	RelationBlockBackend rb_backend;
	Size rb_size;

	/* For fixed-length blocks */
	List *rb_cg_locations;
	// Keep these in sync
	bool *rb_slotmap;
	int rb_free_slots;
	HeapTupleHeaderData *rb_tuple_headers;

	/* For variable-length blocks */
	void *rb_location;
	// Keep these in sync
	void *rb_start_scan;
	Size rb_free_space;
} RelationBlockData;
typedef RelationBlockData* RelationBlock;

/* RelationColumnGroup structure */
typedef struct RelationColumnGroupData
{
	/* Id */
	int cg_id;
	/* Size of slot in CG */
	Size cg_size;
	/* Starting attr in CG */
	int cg_start_attr_id;
} RelationColumnGroupData;
typedef RelationColumnGroupData* RelationColumnGroup;

typedef struct RelationBlockInfoData
{
	Oid relid;
	Size reltuplen;

	/* relation blocks on VM */
	List *rel_fixed_blocks_on_VM;
	List *rel_variable_blocks_on_VM;

	/* relation blocks on NVM */
	List *rel_fixed_blocks_on_NVM;
	List *rel_variable_blocks_on_NVM;

	/* column groups */
	int  *rel_attr_group;
	List *rel_column_groups;
} RelationBlockInfoData;
typedef RelationBlockInfoData* RelationBlockInfo;

/* RelBlock HTAB */

/* Key for RelBlock Lookup Table */
typedef struct RelBlockTag{
	Oid       relid;
} RelBlockTag;

/* Entry for RelBlock Lookup Table */
typedef struct RelBlockLookupEnt{
	/*
	  XXX Payload required to handle a weird hash
	  function issue in dynahash.c;
	  otherwise the keys don't collide
	*/
	int               payload;
	int               pid;
	RelationBlockInfo relblockinfo;
} RelBlockLookupEnt;

extern HTAB *SharedRelBlockHash;

/* Variable-length block header */
typedef struct RelBlockVarlenHeaderData{
	/* occupied status for the slot */
	bool vb_slot_status;
	/* length of the slot */
	uint16 vb_slot_length;
	/* length of the previous slot */
	uint16 vb_prev_slot_length;
} RelBlockVarlenHeaderData;
typedef RelBlockVarlenHeaderData* RelBlockVarlenHeader;

#define RELBLOCK_VARLEN_HEADER_SIZE     8   /* 8 bytes */

typedef struct RelBlockLocation
{
	/* location of block */
	RelationBlock rb_location;
	/* offset of slot within block (starts from 1) */
	OffsetNumber rb_offset;
} RelBlockLocation;

/* relblock.c */
extern void RelationInitBlockTableEntry(Relation relation);
extern List** GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
								   RelationBlockType relblocktype);

extern Oid  RelationBlockInsertTuple(Relation relation, HeapTuple tup, CommandId cid,
									 int options, BulkInsertState bistate);

/* relblock_table.c */
extern Size RelBlockTableShmemSize(int size);
extern void InitRelBlockTable(int size);
extern uint32 RelBlockTableHashCode(RelBlockTag *tagPtr);
extern RelBlockLookupEnt *RelBlockTableLookup(RelBlockTag *tagPtr, uint32 hashcode);
extern int	RelBlockTableInsert(RelBlockTag *tagPtr, uint32 hashcode,
								RelationBlockInfo relblockinfo);
extern void RelBlockTableDelete(RelBlockTag *tagPtr, uint32 hashcode);
extern void RelBlockTablePrint();

/* relblock_fixed.c */
extern RelBlockLocation GetFixedLengthSlot(Relation relation, RelationBlockBackend relblockbackend);

/* relblock_varlen.c */
extern void *GetVariableLengthSlot(Relation relation, RelationBlockBackend relblockbackend,
								   Size allocation_size);
extern void  ReleaseVariableLengthSlot(Relation relation, RelationBlockBackend relblockbackend,
									   void *location);

#endif   /* RELBLOCK_H */
