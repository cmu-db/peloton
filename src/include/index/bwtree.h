//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree.h
//
// Identification: src/backend/index/bwtree.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
class BWTree {
  // Add your declarations here
};

}  // End index namespace
}  // End peloton namespace