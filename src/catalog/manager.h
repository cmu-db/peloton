/*-------------------------------------------------------------------------
 *
 * catalog.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <atomic>

#include "tbb/concurrent_unordered_map.h"
#include "common/types.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

typedef tbb::concurrent_unordered_map<oid_t, void*> lookup_dir;

class Manager {
  Manager() = delete;
  Manager(Manager const&) = delete;

 public:

  static oid_t GetNextOid(){
    return ++oid;
  }

  static oid_t GetOid() {
    return oid;
  }

  static void SetLocation(const oid_t oid, void *location);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  static std::atomic<oid_t> oid;

  static lookup_dir locator;
};

} // End catalog namespace
} // End nstore namespace
