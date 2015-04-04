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
  Manager(Manager const&) = delete;

 public:

  Manager() : oid (ATOMIC_VAR_INIT(INVALID_OID)) {
  }

  oid_t GetNextOid(){
    return ++oid;
  }

  oid_t GetOid() const{
    return oid;
  }

  void SetLocation(const oid_t oid, void *location){
    locator.insert(std::pair<oid_t, void*>(oid, location));
  }

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> oid;

  lookup_dir locator;
};


} // End catalog namespace
} // End nstore namespace
