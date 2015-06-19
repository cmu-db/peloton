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

namespace nstore {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Index& index) {

  os << "\tINDEX :: ";

  os << index.name << " Type : " << index.type << " Unique : " << index.unique << "\n";

  os << "\t\tColumns : ";
  for(auto col : index.columns) {
    os << col->GetName() << " ";
  }

  os << "\n";

  return os;
}


} // End catalog namespace
} // End nstore namespace


