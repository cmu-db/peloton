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
#include <map>
#include <thread>
#include <mutex>

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

  void SetLocation(oid_t oid, void *location){

    {
      std::lock_guard<std::mutex> lock(locator_mutex);

      locator[oid] = location;
    }

  }


 private:
  std::atomic<oid_t> oid;

  std::mutex locator_mutex;

  std::map<oid_t, void*> locator;
};


} // End catalog namespace
} // End nstore namespace
