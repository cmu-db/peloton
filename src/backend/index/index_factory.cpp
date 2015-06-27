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

#include "backend/index/index_factory.h"

#include "backend/index/btree_index.h"

namespace peloton {
namespace index {

Index *IndexFactory::GetInstance(IndexMetadata *metadata) {

  //catalog::Schema *key_schema = metadata->key_schema;
  //std::cout << "Creating index : "<< metadata->identifier << " " << (*key_schema);

  BtreeMultimapIndex *index = new BtreeMultimapIndex(metadata);

  return index;
}

} // End index namespace
} // End peloton namespace

