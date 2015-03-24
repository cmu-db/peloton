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
// ItemPointer
//===--------------------------------------------------------------------===//

struct ItemPointer {
  oid_t tile_id;
  oid_t offset;

  ItemPointer(oid_t tile_id, oid_t offset)
  : tile_id(tile_id), offset(offset){
  }

};


//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

typedef tbb::concurrent_unordered_map<oid_t, void*> locator_chm;

class Catalog {
  Catalog(Catalog const&) = delete;

 public:

  Catalog() : oid (ATOMIC_VAR_INIT(INVALID_OID)) {
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

  locator_chm locator;
};


} // End catalog namespace
} // End nstore namespace
