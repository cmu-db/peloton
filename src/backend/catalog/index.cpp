/*-------------------------------------------------------------------------
 *
 * index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/index.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/index.h"
#include "backend/index/index.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Index& index) {

    os << "\tINDEX :: " << index.GetName();

    // Get underlying physical index
    auto physical_index = index.GetPhysicalIndex();
    if(physical_index != nullptr) {
      os << " Type : " << physical_index->GetTypeName();
      os << " Unique : " << physical_index->HasUniqueKeys() << "\n";
    }

    return os;
}


} // End catalog namespace
} // End peloton namespace


