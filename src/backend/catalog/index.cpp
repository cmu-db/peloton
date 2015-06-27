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
#include "backend/catalog/column.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Index& index) {

    os << "\tINDEX :: " << index.GetName();
    os << " Type : " << IndexTypeToString(index.type);
    os << " Unique : " << index.unique << "\n";
    os << " Columns : ";
    for(auto col : index.columns) {
        os << col->GetName() << " ";
    }
    os << "\n";

    return os;
}


} // End catalog namespace
} // End peloton namespace


