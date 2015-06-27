/*-------------------------------------------------------------------------
 *
 * column.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/column.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/column.h"
#include "backend/common/types.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Column& column) {

    os << "\tCOLUMN ";

    os << column.offset << " : " << column.GetName() << " "
       << GetTypeName(column.type) << " " << column.size << " " << column.not_null;

    os << "\n";

    return os;
}

} // End catalog namespace
} // End peloton namespace


