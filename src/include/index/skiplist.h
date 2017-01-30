//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// skiplist.h
//
// Identification: src/index/skiplist.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

# pragma once

namespace peloton {
namespace index {

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS template <typename KeyType, \
                                              typename ValueType, \
                                              typename KeyComparator, \
                                              typename KeyEqualityChecker, \
                                              typename KeyHashFunc, \
                                              typename ValueEqualityChecker, \
                                              typename ValueHashFunc>
template <typename KeyType,
          typename ValueType,
          typename KeyComparator,
          typename KeyEqualityChecker,
          typename KeyHashFunc,
          typename ValueEqualityChecker,
          typename ValueHashFunc>
class SkipList {

  // Add your declarations here

};

}  // End index namespace
}  // End peloton namespace
