/*-------------------------------------------------------------------------
 *
 * catalog.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/catalog.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/common/types.h"
#include "backend/catalog/catalog.h"

namespace peloton {
namespace catalog {

Catalog& Catalog::GetInstance() {
  static Catalog catalog;
  return catalog;
}

std::ostream& operator<<(std::ostream& os, const Catalog& catalog) {

  os << "\tCATALOG : \n";

  return os;
}


} // End catalog namespace
} // End peloton namespace
