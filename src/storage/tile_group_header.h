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
		data(nullptr),
		num_tuple_slots(tuple_count),
		next_tuple_slot(0) {

		header_size = num_tuple_slots * header_entry_size;

		// allocate storage space for header
		data = (char *) backend->Allocate(header_size);
		assert(data != nullptr);
	}

	~TileGroupHeader() {
		// reclaim the space
		backend->Free(data);
		data = nullptr;
	}

	id_t GetNextEmptyTupleSlot() {
		id_t tuple_slot_id = INVALID_ID;

		{
			std::lock_guard<std::mutex> tile_header_lock(tile_header_mutex);

			if(next_tuple_slot < num_tuple_slots) {
				tuple_slot_id = next_tuple_slot;
				next_tuple_slot++;
			}
		}

		return tuple_slot_id;
	}

	id_t GetActiveTupleCount() const {
		return next_tuple_slot;
	}

	//===--------------------------------------------------------------------===//
	// MVCC utilities
	//===--------------------------------------------------------------------===//

	// Getters

	inline txn_id_t GetTransactionId(const id_t tuple_slot_id) const {
		return *((txn_id_t*)( data + (tuple_slot_id * header_entry_size)));
	}

	inline cid_t GetBeginCommitId(const id_t tuple_slot_id) const {
		return *((cid_t*)( data + (tuple_slot_id * header_entry_size) + sizeof(id_t) ));
	}

	inline cid_t GetEndCommitId(const id_t tuple_slot_id) const {
		return *((cid_t*)( data + (tuple_slot_id * header_entry_size) + 2 * sizeof(id_t) ));
	}

	// Getters for addresses

	inline txn_id_t *GetTransactionIdLocation(const id_t tuple_slot_id) const {
		return ((txn_id_t*)( data + (tuple_slot_id * header_entry_size)));
	}

	inline cid_t *GetBeginCommitIdLocation(const id_t tuple_slot_id) const {
		return ((cid_t*)( data + (tuple_slot_id * header_entry_size) + sizeof(id_t) ));
	}

	inline cid_t *GetEndCommitIdLocation(const id_t tuple_slot_id) const {
		return ((cid_t*)( data + (tuple_slot_id * header_entry_size) + 2 * sizeof(id_t) ));
	}

	// Setters

	inline void SetTransactionId(const id_t tuple_slot_id, txn_id_t transaction_id) {
		*((txn_id_t*)( data + (tuple_slot_id * header_entry_size))) = transaction_id;
	}

	inline void SetBeginCommitId(const id_t tuple_slot_id, cid_t begin_cid) {
		*((cid_t*)( data + (tuple_slot_id * header_entry_size) + sizeof(id_t))) = begin_cid;
	}

	inline void SetEndCommitId(const id_t tuple_slot_id, cid_t end_cid) const {
		*((cid_t*)( data + (tuple_slot_id * header_entry_size) + 2 * sizeof(cid_t))) = end_cid;
	}

	// Visibility check

	bool IsVisible(const id_t tuple_slot_id, txn_id_t txn_id, cid_t at_cid) {

		bool own = (txn_id == GetTransactionId(tuple_slot_id));
		bool activated = (at_cid >= GetBeginCommitId(tuple_slot_id));
		bool invalidated = (at_cid >= GetEndCommitId(tuple_slot_id));

		std::cout << "-----------------------------------------------------------\n";
		std::cout << "Slot :: " << tuple_slot_id
				<< " Txn     :: txn_id : " << txn_id << " cid : " << at_cid << "\n";

		std::cout << "Slot    :: txn_id : " << GetTransactionId(tuple_slot_id)
						<< " begin cid : " << GetBeginCommitId(tuple_slot_id) << " "
						<< " end cid : " << GetEndCommitId(tuple_slot_id) << "\n";

		std::cout << "MVCC    :: own : " << own << " activated : " << activated
				<< " invalidated : " << invalidated << "\n";

		std::cout << "-----------------------------------------------------------\n";

		// Visible iff past Insert || Own Insert
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
};


} // End storage namespace
} // End nstore namespace



