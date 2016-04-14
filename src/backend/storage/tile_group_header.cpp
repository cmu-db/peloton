//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_header.cpp
//
// Identification: src/backend/storage/tile_group_header.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <iomanip>
#include <sstream>

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/storage/storage_manager.h"
#include "backend/storage/tile_group_header.h"

namespace peloton {
namespace storage {

TileGroupHeader::TileGroupHeader(const BackendType &backend_type,
                                 const int &tuple_count)
    : backend_type(backend_type),
      data(nullptr),
      num_tuple_slots(tuple_count),
      next_tuple_slot(0) {
  header_size = num_tuple_slots * header_entry_size;

  // allocate storage space for header
  auto &storage_manager = storage::StorageManager::GetInstance();
  data = reinterpret_cast<char *>(
      storage_manager.Allocate(backend_type, header_size));
  assert(data != nullptr);

  // zero out the data
  std::memset(data, 0, header_size);

  // Set MVCC Initial Value
  for (oid_t tuple_slot_id = START_OID; tuple_slot_id < num_tuple_slots;
       tuple_slot_id++) {
    SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
    SetBeginCommitId(tuple_slot_id, MAX_CID);
    SetEndCommitId(tuple_slot_id, MAX_CID);
    SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);
    SetPrevItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);

    SetInsertCommit(tuple_slot_id, false);  // unused
    SetDeleteCommit(tuple_slot_id, false);  // unused
  }
}

TileGroupHeader::~TileGroupHeader() {
  // reclaim the space
  auto &storage_manager = storage::StorageManager::GetInstance();
  storage_manager.Release(backend_type, data);

  data = nullptr;
}

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

const std::string TileGroupHeader::GetInfo() const {
  std::ostringstream os;

  os << "\t-----------------------------------------------------------\n";
  os << "\tTILE GROUP HEADER \n";

  oid_t active_tuple_slots = GetNextTupleSlot();
  peloton::ItemPointer item;

  for (oid_t header_itr = 0; header_itr < active_tuple_slots; header_itr++) {
    txn_id_t txn_id = GetTransactionId(header_itr);
    cid_t beg_commit_id = GetBeginCommitId(header_itr);
    cid_t end_commit_id = GetEndCommitId(header_itr);
    bool insert_commit = GetInsertCommit(header_itr);
    bool delete_commit = GetDeleteCommit(header_itr);

    int width = 10;
    os << "\t txn id : ";
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

    os << " insert commit : ";
    if (insert_commit == true)
      os << "O";
    else
      os << "X";

    os << " delete commit : ";
    if (delete_commit == true)
      os << "O";
    else
      os << "X";

    peloton::ItemPointer location = GetNextItemPointer(header_itr);
    peloton::ItemPointer location2 = GetPrevItemPointer(header_itr);
    os << " next : "
       << "[ " << location.block << " , " << location.offset << " ]\n"
       << " prev : "
       << "[ " << location2.block << " , " << location2.offset << " ]\n";
  }

  os << "\t-----------------------------------------------------------\n";

  return os.str();
}

void TileGroupHeader::Sync() {
  // Sync the tile group data
  auto &storage_manager = storage::StorageManager::GetInstance();
  storage_manager.Sync(backend_type, data, header_size);
}

void TileGroupHeader::PrintVisibility(txn_id_t txn_id, cid_t at_cid) {
  oid_t active_tuple_slots = GetNextTupleSlot();
  std::stringstream os;

  os << "\t-----------------------------------------------------------\n";

  for (oid_t header_itr = 0; header_itr < active_tuple_slots; header_itr++) {
    bool own = (txn_id == GetTransactionId(header_itr));
    bool activated = (at_cid >= GetBeginCommitId(header_itr));
    bool invalidated = (at_cid >= GetEndCommitId(header_itr));

    txn_id_t txn_id = GetTransactionId(header_itr);
    cid_t beg_commit_id = GetBeginCommitId(header_itr);
    cid_t end_commit_id = GetEndCommitId(header_itr);
    bool insert_commit = GetInsertCommit(header_itr);
    bool delete_commit = GetDeleteCommit(header_itr);

    int width = 10;

    os << "\tslot :: " << std::setw(width) << header_itr;

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

    os << " insert commit : ";
    if (insert_commit == true)
      os << "O";
    else
      os << "X";

    os << " delete commit : ";
    if (delete_commit == true)
      os << "O";
    else
      os << "X";

    peloton::ItemPointer location = GetNextItemPointer(header_itr);
    os << " prev : "
       << "[ " << location.block << " , " << location.offset << " ]";  //<<

    os << " own : " << own;
    os << " activated : " << activated;
    os << " invalidated : " << invalidated << " ";

    // Visible iff past Insert || Own Insert
    if ((!own && activated && !invalidated) ||
        (own && !activated && !invalidated))
      os << "\t\t[ true  ]\n";
    else
      os << "\t\t[ false ]\n";
  }

  os << "\t-----------------------------------------------------------\n";

  LOG_TRACE("%s", os.str().c_str());
}

// this function is called only when building tile groups for aggregation operations.
oid_t TileGroupHeader::GetActiveTupleCount() {
  oid_t active_tuple_slots = 0;

  for (oid_t tuple_slot_id = START_OID; tuple_slot_id < num_tuple_slots;
       tuple_slot_id++) {
    txn_id_t tuple_txn_id = GetTransactionId(tuple_slot_id);
    if (tuple_txn_id != INVALID_TXN_ID) {
      assert(tuple_txn_id == INITIAL_TXN_ID);
      active_tuple_slots++;
    }
  }

  return active_tuple_slots;
}

}  // End storage namespace
}  // End peloton namespace
