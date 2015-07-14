/*-------------------------------------------------------------------------
 *
 * database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/database.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Database& database) {

    os << "\tDATABASE " << database.GetName() << "\n\n";

    return os;
}


} // End catalog namespace
} // End peloton namespace

