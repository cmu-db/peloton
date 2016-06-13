//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_factory.h
//
// Identification: src/backend/index/index_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/index/index.h"

namespace peloton {
namespace index {

//===--------------------------------------------------------------------===//
// IndexFactory
//===--------------------------------------------------------------------===//

class IndexFactory {
 public:
  // Get an index with required attributes
  static Index *GetInstance(IndexMetadata *metadata, const size_t &preallocate_size = 1);
};

}  // End index namespace
}  // End peloton namespace
