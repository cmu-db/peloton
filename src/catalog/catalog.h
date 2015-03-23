/*-------------------------------------------------------------------------
 *
 * storage_manager.h
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

#include "common/types.h"

#include "tbb/concurrent_hash_map.h"

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

class Catalog {
  Catalog() = delete;
  Catalog(Catalog const&) = delete;

 public:

  static oid_t GetNextOid(){
    return ++oid;
  }

  static std::atomic<oid_t> oid;

  tbb::concurrent_hash_map<oid_t, void *> locator;
};

std::atomic<oid_t> Catalog::oid = ATOMIC_VAR_INIT(1);


} // End catalog namespace
} // End nstore namespace
