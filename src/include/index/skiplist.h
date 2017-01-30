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

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <thread>
#include <unordered_set>
// offsetof() is defined here
#include <vector>
#include <cstddef>

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
          typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>,
          typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class SkipList {

  // Add your declarations here

};

}  // End index namespace
}  // End peloton namespace
