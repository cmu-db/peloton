/*-------------------------------------------------------------------------
 *
 * index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/index/index.h"
#include "backend/common/exception.h"
#include "backend/catalog/manager.h"

#include <iostream>

namespace peloton {
namespace index {

Index::Index(IndexMetadata *metadata)
: metadata(metadata) {

  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;

}

std::ostream& operator<<(std::ostream& os, const Index& index) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tINDEX\n";

  os << index.GetTypeName() << "\t(" << index.GetName() << ")";
  os << (index.HasUniqueKeys() ? " UNIQUE " : " NON-UNIQUE") << "\n";

  os << "\tValue schema : " << *(index.GetKeySchema() );

  os << "\t-----------------------------------------------------------\n";

  return os;
}

void Index::GetInfo() const {

  std::cout << this->GetName() << ",";
  std::cout << GetTypeName() << ",";
  std::cout << lookup_counter << ",";
  std::cout << insert_counter << ",";
  std::cout << delete_counter << ",";
  std::cout << update_counter << std::endl;

}

} // End index namespace
} // End peloton namespace



