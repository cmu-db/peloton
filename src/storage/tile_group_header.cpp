/*-------------------------------------------------------------------------
 *
 * tile_group_header.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile_group_header.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/tile_group_header.h"

#include <iostream>
#include <iomanip>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile Group Header
//===--------------------------------------------------------------------===//

std::ostream& operator<<(std::ostream& os, const TileGroupHeader& tile_group_header) {

	os << "\t-----------------------------------------------------------\n";
	os << "\tTILE GROUP HEADER \n";
	os << "\t Txn Id      Begin Commit Id      End Commit Id      Prev ItemPointer \n";

	id_t active_tuple_slots = tile_group_header.GetActiveTupleCount();
  ItemPointer item;

	for(id_t header_itr = 0 ; header_itr < active_tuple_slots ; header_itr++){

		os <<  std::setw(10) << tile_group_header.GetTransactionId(header_itr) << " "
		    << std::setw(25) << tile_group_header.GetBeginCommitId(header_itr) << " "
		    << std::setw(25) <<	tile_group_header.GetEndCommitId(header_itr);

		item = tile_group_header.GetPrevItemPointer(header_itr);
		os << " " << std::setw(10) << item.block << " : " << item.offset << " \n";

	}

	os << "\t-----------------------------------------------------------\n";

	return os;
}

void TileGroupHeader::PrintVisibility(txn_id_t txn_id, cid_t at_cid) {

	id_t active_tuple_slots = GetActiveTupleCount();

	std::cout << "\t-----------------------------------------------------------\n";

	std::cout << "\tTxn :: " << txn_id  << " cid : " << at_cid << "\n";

	for(id_t header_itr = 0 ; header_itr < active_tuple_slots ; header_itr++){

		bool own = (txn_id == GetTransactionId(header_itr));
		bool activated = (at_cid >= GetBeginCommitId(header_itr));
		bool invalidated = (at_cid >= GetEndCommitId(header_itr));

		/*
		std::cout << "\tSlot :: " << header_itr
				<< " txn_id : " << GetTransactionId(header_itr)
				<< " begin cid : " << GetBeginCommitId(header_itr)
				<< " end cid : " << GetEndCommitId(header_itr) << "\n";
		*/

		std::cout << "\tMVCC :: own : " << own << " activated : " << activated
				<< " invalidated : " << invalidated << " ";

		// Visible iff past Insert || Own Insert
		if((!own && activated && !invalidated) || (own && !activated && !invalidated))
			std::cout << "\t\t[ true  ]\n";
		else
			std::cout << "\t\t[ false ]\n";

	}

	std::cout << "\t-----------------------------------------------------------\n";
}

} // End storage namespace
} // End nstore namespace
