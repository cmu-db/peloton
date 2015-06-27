/*-------------------------------------------------------------------------
 *
 * index_factory.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index_factory.h
 *
 *-------------------------------------------------------------------------
 */

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
  static Index *GetInstance(IndexMetadata *metadata);

};


} // End index namespace
} // End peloton namespace
