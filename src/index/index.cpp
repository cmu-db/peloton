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

#include "index/index.h"

#include <iostream>

namespace nstore {
namespace index {

Index::Index(const IndexMetadata &metadata) :
                metadata(metadata),
                identifier(metadata.identifier),
                catalog(metadata.catalog),
                key_schema(metadata.key_schema),
                tuple_schema(metadata.tuple_schema),
                is_unique_index(metadata.unique) {

  // # of columns in key
  column_count = metadata.key_schema->GetColumnCount();

  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;

  // initialize memory size to zero
  memory_size_estimate = 0;
}

std::ostream& operator<<(std::ostream& os, const Index& index) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tINDEX\n";

  os << index.GetTypeName() << "\t(" << index.GetName() << ")";
  os << (index.IsUniqueIndex() ? " UNIQUE " : " NON-UNIQUE") << "\n";

  os << "\tValue schema : " << (*index.tuple_schema) ;
  os << "\tSize: " << index.GetSize();

  os << "\t-----------------------------------------------------------\n";

  return os;
}

void Index::PrintReport() {

  std::cout << identifier << ",";
  std::cout << GetTypeName() << ",";
  std::cout << lookup_counter << ",";
  std::cout << insert_counter << ",";
  std::cout << delete_counter << ",";
  std::cout << update_counter << std::endl;

}

} // End index namespace
} // End nstore namespace



