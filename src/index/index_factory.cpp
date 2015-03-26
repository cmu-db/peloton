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
#include "array_unique_index.h"

namespace nstore {
namespace index {

Index *IndexFactory::GetInstance(const IndexMetadata &metadata) {

  bool unique = metadata.unique;
  bool ints_only = metadata.ints_only;
  IndexType type = metadata.type;

  catalog::Schema *key_schema = metadata.key_schema;
  const id_t key_size = key_schema->GetLength();

  std::cout << "Creating index : "<< metadata.identifier << " " << (*key_schema);

  // no int specialization beyond this point
  if (key_size > sizeof(int64_t) * 4) {
    ints_only = false;
  }

  // a bit of a hack, this should be improved later
  if ((ints_only) && (unique) && (type == INDEX_TYPE_ARRAY)) {
    return new ArrayUniqueIndex(metadata);
  }

  throw NotImplementedException("Unsupported Index Metadata" );

  return nullptr;
}

} // End index namespace
} // End nstore namespace

