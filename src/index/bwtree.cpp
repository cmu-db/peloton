
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree.cpp
//
// Identification: src/index/bwtree.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/bwtree.h"

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#else
namespace wangziqi2013 {
namespace bwtree {
#endif

bool print_flag = true;

// This will be initialized when thread is initialized and in a per-thread
// basis, i.e. each thread will get the same initialization image and then
// is free to change them
thread_local int BwTreeBase::gc_id = -1;

std::atomic<size_t> BwTreeBase::total_thread_num{0UL};

}  // End index/bwtree namespace
}  // End peloton/wangziqi2013 namespace
