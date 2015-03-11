/*-------------------------------------------------------------------------
 *
 * tile_header.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_header.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/backend.h"

#include <atomic>
#include <mutex>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

/**
 *
 * This contains information related to MVCC.
 * It is shared by all tiles in a tile group.
 *
 * Layout :
 *
 * 	---------------------------------------------------------------------------
 *  | Txn ID (8 bytes)  | Begin TimeStamp (8 bytes) | End TimeStamp (8 bytes) |
 * 	---------------------------------------------------------------------------
 *
 */

class TileGroupHeader {
	TileGroupHeader() = delete;
	TileGroupHeader(const TileGroupHeader& other) = delete;

public:

	TileGroupHeader(Backend* _backend, int tuple_count) :
		backend(_backend),
		data(NULL),
		num_tuple_slots(tuple_count),
		next_tuple_slot(0),
		tile_header_lock(tile_header_mutex) {

		header_size = num_tuple_slots * header_entry_size;

		/// allocate storage space for header
		data = (char *) backend->Allocate(header_size);
		assert(data != NULL);
	}

	~TileGroupHeader() {
		/// reclaim the space
		backend->Free(data);
		data = NULL;
	}

	id_t GetNextEmptyTupleSlot() {
		id_t tuple_slot_id = INVALID_ID;

		tile_header_lock.lock();

		if(next_tuple_slot < num_tuple_slots - 1) {
			tuple_slot_id = next_tuple_slot;
			next_tuple_slot++;
		}

		tile_header_lock.unlock();

		return tuple_slot_id;
	}

	id_t GetActiveTupleCount() const {
		return next_tuple_slot;
	}

	//===--------------------------------------------------------------------===//
	// MVCC utilities
	//===--------------------------------------------------------------------===//

	inline id_t GetTransactionId(const id_t tuple_slot_id) const {
		return *((id_t*)( data + (tuple_slot_id * header_entry_size)));
	}

	inline id_t GetBeginTimeStamp(const id_t tuple_slot_id) const {
		return *((id_t*)( data + (tuple_slot_id * header_entry_size) + sizeof(id_t) ));
	}

	inline id_t GetEndTimeStamp(const id_t tuple_slot_id) const {
		return *((id_t*)( data + (tuple_slot_id * header_entry_size) + 2 * sizeof(id_t) ));
	}

	inline void SetTransactionId(const id_t tuple_slot_id, id_t transaction_id) {
		*((id_t*)( data + (tuple_slot_id * header_entry_size))) = transaction_id;
	}

	inline void SetBeginTimeStamp(const id_t tuple_slot_id, id_t begin_timestamp) {
		*((id_t*)( data + (tuple_slot_id * header_entry_size) + sizeof(id_t))) = begin_timestamp;
	}

	inline void SetEndTimeStamp(const id_t tuple_slot_id, id_t end_timestamp) const {
		*((id_t*)( data + (tuple_slot_id * header_entry_size) + 2 * sizeof(id_t))) = end_timestamp;
	}

	bool IsVisible(const id_t tuple_slot_id, id_t transaction_id, id_t at_timestamp) {

		bool own = (transaction_id == GetTransactionId(tuple_slot_id));
		bool activated = (at_timestamp >= GetBeginTimeStamp(tuple_slot_id));
		bool invalidated = (at_timestamp <= GetEndTimeStamp(tuple_slot_id));

		std::cout << "Own :: " << own << " " <<
				"Activated :: " << activated <<
				"Invalidated :: " << invalidated << "\n";

		// Visible iff Past Insert || Own Insert
		if((!own && activated && !invalidated) || (own && !activated && !invalidated))
			return true;

		return false;
	}

private:

	// header entry size is the size of the layout described above
	static const size_t header_entry_size = 3 * sizeof(id_t);

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	// storage backend
	Backend *backend;

	// set of fixed-length tuple slots
	char *data;

	// number of tuple slots allocated
	id_t num_tuple_slots;

	size_t header_size;

	// next free tuple slot
	id_t next_tuple_slot;

	// synch helpers
	std::mutex tile_header_mutex;

	std::unique_lock<std::mutex> tile_header_lock;
};


} // End storage namespace
} // End nstore namespace



