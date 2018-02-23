//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_factory.h
//
// Identification: src/include/index/index_factory.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "index/index.h"

namespace peloton {
namespace index {

//===----------------------------------------------------------------------===//
//
// This is a factory for all index structures supported in Peloton
//
//===----------------------------------------------------------------------===//
class IndexFactory {
 public:
  /// Get an index with required attributes
  static Index *GetIndex(IndexMetadata *metadata);

 private:
  static std::string GetInfo(IndexMetadata *metadata,
                             const std::string &comparator_type);

  /// BwTree factory methods
  static Index *GetBwTreeIntsKeyIndex(IndexMetadata *metadata);
  static Index *GetBwTreeGenericKeyIndex(IndexMetadata *metadata);

  /// SkipList factory methods
  static Index *GetSkipListIntsKeyIndex(IndexMetadata *metadata);
  static Index *GetSkipListGenericKeyIndex(IndexMetadata *metadata);
};

}  // namespace index
}  // namespace peloton
