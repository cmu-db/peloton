//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.cpp
//
// Identification: src/storage/tile_group_header.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "storage/tile_group_header.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include "common/container_tuple.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/platform.h"
#include "common/printable.h"
#include "concurrency/transaction_manager_factory.h"
#include "gc/gc_manager.h"
#include "logging/log_manager.h"
#include "storage/backend_manager.h"
#include "type/value.h"
#include "storage/tuple.h"

namespace peloton {
namespace storage {

TileGroupHeader::TileGroupHeader(const BackendType &backend_type,
                                 const int &tuple_count)
    : backend_type(backend_type),
      tile_group(nullptr),
      data(nullptr),
      num_tuple_slots(tuple_count),
      next_tuple_slot(0),
      tile_header_lock() {
  header_size = num_tuple_slots * header_entry_size;

  // allocate storage space for header
  // auto &storage_manager = storage::StorageManager::GetInstance();
  // data = reinterpret_cast<char *>(
  // storage_manager.Allocate(backend_type, header_size));
  data = new char[header_size];
  PL_ASSERT(data != nullptr);

  // zero out the data
  PL_MEMSET(data, 0, header_size);

  // Set MVCC Initial Value
  for (oid_t tuple_slot_id = START_OID; tuple_slot_id < num_tuple_slots;
       tuple_slot_id++) {
    SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
    SetBeginCommitId(tuple_slot_id, MAX_CID);
    SetEndCommitId(tuple_slot_id, MAX_CID);
    SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);
    SetPrevItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);
  }

  // Initially immutabile flag to false initially.
  immutable = false;
}

TileGroupHeader::~TileGroupHeader() {
  // reclaim the space
  // auto &storage_manager = storage::StorageManager::GetInstance();
  // storage_manager.Release(backend_type, data);
  delete[] data;
  data = nullptr;
}

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

const std::string TileGroupHeader::GetInfo() const {
  std::ostringstream os;

  os << "TILE GROUP HEADER (";
  os << "Address:" << this << ", ";
  os << "NumActiveTuples:";
  os << GetActiveTupleCount() << ", ";
  os << "Immutable: " << GetImmutability();
  os << ")";
  os << std::endl;

  std::string spacer = StringUtil::Repeat(" ", TUPLE_ID_WIDTH + 2);

  oid_t active_tuple_slots = GetCurrentNextTupleSlot();
  for (oid_t header_itr = 0; header_itr < active_tuple_slots; header_itr++) {
    txn_id_t txn_id = GetTransactionId(header_itr);
    cid_t beg_commit_id = GetBeginCommitId(header_itr);
    cid_t end_commit_id = GetEndCommitId(header_itr);

    if (header_itr > 0) os << std::endl;
    os << std::right << std::setfill('0') << std::setw(TUPLE_ID_WIDTH)
       << header_itr << ": ";

    // TRANSACTION ID
    os << "TxnId:";
    if (txn_id == MAX_TXN_ID)
      os << std::left << std::setfill(' ') << std::setw(TXN_ID_WIDTH)
         << "MAX_TXN_ID";
    else
      os << std::right << std::setfill('0') << std::setw(TXN_ID_WIDTH)
         << txn_id;
    os << " ";

    // BEGIN COMMIT ID
    os << "BeginCommitId:";
    if (beg_commit_id == MAX_CID)
      os << std::left << std::setfill(' ') << std::setw(TXN_ID_WIDTH)
         << "MAX_CID";
    else
      os << std::right << std::setfill('0') << std::setw(TXN_ID_WIDTH)
         << beg_commit_id;
    os << " ";

    // END COMMIT ID
    os << "EndCId:";
    if (end_commit_id == MAX_CID)
      os << std::left << std::setfill(' ') << std::setw(TXN_ID_WIDTH)
         << "MAX_CID";
    else
      os << std::right << std::setfill('0') << std::setw(TXN_ID_WIDTH)
         << end_commit_id;
    os << std::endl;
    os << spacer;

    // NEXT RANGE
    peloton::ItemPointer nextPointer = GetNextItemPointer(header_itr);
    os << "Next:[";
    if (nextPointer.block == INVALID_OID) {
      os << "INVALID_OID";
    } else {
      os << nextPointer.block;
    }
    os << ", ";
    if (nextPointer.offset == INVALID_OID) {
      os << "INVALID_OID";
    } else {
      os << nextPointer.offset;
    }
    os << "] ";

    // PREVIOUS RANGE
    peloton::ItemPointer prevPointer = GetPrevItemPointer(header_itr);
    os << "Prev:[";
    if (prevPointer.block == INVALID_OID) {
      os << "INVALID_OID";
    } else {
      os << prevPointer.block;
    }
    os << ", ";
    if (prevPointer.offset == INVALID_OID) {
      os << "INVALID_OID";
    } else {
      os << prevPointer.offset;
    }
    os << "]";
  }

  return os.str();
}

void TileGroupHeader::Sync() {
  // Sync the tile group data
  // auto &storage_manager = storage::StorageManager::GetInstance();
  // storage_manager.Sync(backend_type, data, header_size);
}

void TileGroupHeader::PrintVisibility(txn_id_t txn_id, cid_t at_cid) {
  oid_t active_tuple_slots = GetCurrentNextTupleSlot();
  std::stringstream os;

  os << peloton::GETINFO_SINGLE_LINE << "\n";

  for (oid_t header_itr = 0; header_itr < active_tuple_slots; header_itr++) {
    bool own = (txn_id == GetTransactionId(header_itr));
    bool activated = (at_cid >= GetBeginCommitId(header_itr));
    bool invalidated = (at_cid >= GetEndCommitId(header_itr));

    txn_id_t txn_id = GetTransactionId(header_itr);
    cid_t beg_commit_id = GetBeginCommitId(header_itr);
    cid_t end_commit_id = GetEndCommitId(header_itr);

    int width = 10;

    os << " slot :: " << std::setw(width) << header_itr;

    os << " txn id : ";
    if (txn_id == MAX_TXN_ID)
      os << std::setw(width) << "MAX_TXN_ID";
    else
      os << std::setw(width) << txn_id;

    os << " beg cid : ";
    if (beg_commit_id == MAX_CID)
      os << std::setw(width) << "MAX_CID";
    else
      os << std::setw(width) << beg_commit_id;

    os << " end cid : ";
    if (end_commit_id == MAX_CID)
      os << std::setw(width) << "MAX_CID";
    else
      os << std::setw(width) << end_commit_id;

    peloton::ItemPointer location = GetNextItemPointer(header_itr);
    os << " prev : "
       << "[ " << location.block << " , " << location.offset << " ]";  //<<

    os << " own : " << own;
    os << " activated : " << activated;
    os << " invalidated : " << invalidated << " ";

    // Visible iff past Insert || Own Insert
    if ((!own && activated && !invalidated) ||
        (own && !activated && !invalidated))
      os << "  [ true  ]\n";
    else
      os << "  [ false ]\n";
  }

  os << peloton::GETINFO_SINGLE_LINE << "\n";

  LOG_TRACE("%s", os.str().c_str());
}

// this function is called only when building tile groups for aggregation
// operations.
oid_t TileGroupHeader::GetActiveTupleCount() const {
  oid_t active_tuple_slots = 0;

  for (oid_t tuple_slot_id = START_OID; tuple_slot_id < num_tuple_slots;
       tuple_slot_id++) {
    txn_id_t tuple_txn_id = GetTransactionId(tuple_slot_id);
    if (tuple_txn_id != INVALID_TXN_ID) {
      PL_ASSERT(tuple_txn_id == INITIAL_TXN_ID);
      active_tuple_slots++;
    }
  }

  return active_tuple_slots;
}

}  // namespace storage
}  // namespace peloton
