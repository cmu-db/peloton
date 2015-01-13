/*-------------------------------------------------------------------------
 *
 * rel_block.h
 *	  N-Store relation block definitions.
 *
 *
 * src/include/access/rel_block.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REL_BLOCK_H
#define REL_BLOCK_H

#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf.h"
#include "access/heapam.h"

#define BLOCK_FIXED_LENGTH_SIZE 100             /* In terms of number of tuples */
#define BLOCK_VARIABLE_LENGTH_SIZE 1024*32      /* Raw size in bytes */

#define BLOCK_POINTER_SIZE  8                   /* 8 bytes */
#define NUM_REL_BLOCK_ENTRIES 1000              /* Entries in shared rel block table */

#define RELBLOCK_CACHELINE_SIZE   16            /* 64 bytes */

// Relation storage backend type
typedef enum RelBackend{
	STORAGE_BACKEND_FS,
	STORAGE_BACKEND_MM,
} RelBackend;

#define STORAGE_BACKEND_DEFAULT STORAGE_BACKEND_FS

///////////////////////////////////////////////////////////////////////////////
// RELATION BLOCK INFORMATION
///////////////////////////////////////////////////////////////////////////////

/* Possible block types */
typedef enum RelBlockType
{
	/* Used to store fixed-length tuples */
	RELATION_FIXED_BLOCK_TYPE,
	/* Used to store variable-length attributes */
	RELATION_VARIABLE_BLOCK_TYPE
} RelBlockType;

/* Relation Block Information */
typedef struct RelBlockData
{
	RelBlockType rb_type; 
	Size rb_size;

	/* For fixed-length blocks */
	List *rb_tile_locations;

	// Keep these in sync
	bool *rb_slot_bitmap;
	int rb_free_slots;
	HeapTupleHeader *rb_tuple_headers;

	/* For variable-length blocks */
	void *rb_location;
	// Keep these in sync
	Size rb_free_space;

} RelBlockData;
typedef RelBlockData* RelBlock;

///////////////////////////////////////////////////////////////////////////////
// RELATION INFORMATION
///////////////////////////////////////////////////////////////////////////////

/* Tile information */
typedef struct RelTileData
{
	/* Id */
	int tile_id;
	/* Size of slot in tile */
	Size tile_size;
	/* Starting attr in tile */
	int tile_start_attr_id;

} RelTileData;
typedef RelTileData* RelTile;

typedef struct RelInfoData
{
	Oid rel_id;
	Size rel_tuple_len;

	/* relation blocks -fixed and variable length */
	List *rel_fl_blocks;
	List *rel_vl_blocks;

	/* relation tile information */
	int  *rel_attr_to_tile_map;
	List *rel_tile_to_attrs_map;
} RelInfoData;
typedef RelInfoData* RelInfo;

///////////////////////////////////////////////////////////////////////////////
// Relation Info Hash Table ( Relation -> Relation Info )
///////////////////////////////////////////////////////////////////////////////

/* Key for RelBlock Lookup Table */
typedef struct RelInfoTag{
	Oid       rel_id;
} RelInfoTag;

/* Entry for RelBlock Lookup Table */
typedef struct RelBlockLookupEnt{
	// XXX Payload required to handle a hash function issue in dynahash.c
	int               payload;
	int               pid;
	RelInfo           rel_info;
} RelInfoLookupEnt;

extern HTAB *Shared_Rel_Info_Hash_Table;

///////////////////////////////////////////////////////////////////////////////
// Variable Length Block
///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////
// Location
///////////////////////////////////////////////////////////////////////////////

typedef struct TupleLocation
{
	/* location of block */
	RelBlock rb_location;
	/* offset of slot within block (starts from 1) */
	OffsetNumber rb_offset;
} TupleLocation;

/* relblock.c */
extern void RelInitBlockTableEntry(Relation relation);
extern List** GetRelBlockList(Relation relation, RelBlockType relblocktype);

extern Oid  RelBlockInsertTuple(Relation relation, HeapTuple tup, CommandId cid,
								int options, BulkInsertState bistate);

/* relblock_table.c */
extern Size RelBlockTableShmemSize(Size size);
extern void InitRelBlockTable(Size size);

extern uint32 RelBlockTableHashCode(RelInfoTag *tagPtr);
extern RelInfoLookupEnt *RelBlockTableLookup(RelInfoTag *tagPtr, uint32 hashcode);
extern int	RelBlockTableInsert(RelInfoTag *tagPtr, uint32 hashcode,
								RelInfo relblockinfo);
extern void RelBlockTableDelete(RelInfoTag *tagPtr, uint32 hashcode);
extern void RelBlockTablePrint();

/* relblock_fixed.c */
extern TupleLocation GetFixedLengthSlot(Relation relation);

/* relblock_varlen.c */
extern void *GetVariableLengthSlot(Relation relation, Size allocation_size);
extern void  ReleaseVariableLengthSlot(Relation relation, void *location);

#endif   /* REL_BLOCK_H */
