//===----------------------------------------------------------------------===//
//
//							PelotonDB
//
// tile_group_header.cpp
//
// Identification: src/backend/storage/tile_group_header.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/storage/tile_group_header.h"

#include <iostream>
#include <iomanip>
#include <sstream>

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

std::ostream &operator<<(std::ostream &os,
                         const TileGroupHeader &tile_group_header) {
  os << "\t-----------------------------------------------------------\n";
  os << "\tTILE GROUP HEADER \n";

  oid_t active_tuple_slots = tile_group_header.GetNextTupleSlot();
  peloton::ItemPointer item;

  for (oid_t header_itr = 0; header_itr < active_tuple_slots; header_itr++) {
    txn_id_t txn_id = tile_group_header.GetTransactionId(header_itr);
    cid_t beg_commit_id = tile_group_header.GetBeginCommitId(header_itr);
    cid_t end_commit_id = tile_group_header.GetEndCommitId(header_itr);

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

    peloton::ItemPointer location =
        tile_group_header.GetPrevItemPointer(header_itr);
    os << " prev : "
       << "[ " << location.block << " , " << location.offset << " ]\n";
  }

  os << "\t-----------------------------------------------------------\n";

  return os;
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

    peloton::ItemPointer location = GetPrevItemPointer(header_itr);
    os << " prev : "
       << "[ " << location.block << " , " << location.offset << " ]"; //<<

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

  LOG_INFO("%s", os.str().c_str());
}

} // End storage namespace
} // End peloton namespace
