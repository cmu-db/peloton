/*-------------------------------------------------------------------------
 *
 * index_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index_factory.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "index/index_factory.h"

namespace nstore {
namespace index {

Index *IndexFactory::GetInstance(const IndexMetadata &metadata) {

  //bool unique = metadata.unique_keys;
  //IndexType type = metadata.type;

  catalog::Schema *key_schema = metadata.key_schema;
  //const id_t key_size = key_schema->GetLength();

  std::cout << "Creating index : "<< metadata.identifier << " " << (*key_schema);

  throw NotImplementedException("Unsupported Index Metadata" );

  return nullptr;
}

} // End index namespace
} // End nstore namespace

