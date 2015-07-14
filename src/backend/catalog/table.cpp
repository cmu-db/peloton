/*-------------------------------------------------------------------------
 *
 * table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/table.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Table& table) {

    os << "\tTABLE : " << table.GetName() << "\n";

    return os;
}


} // End catalog namespace
} // End peloton namespace


