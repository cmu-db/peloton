//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// off_gc_manager.h
//
// Identification: src/backend/gc/off_gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/gc/gc_manager.h"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class OffGCManager : public GCManager {
public:
  OffGCManager() {}

  virtual ~OffGCManager() {};

  static OffGCManager &GetInstance() {
    static OffGCManager gcManager;
    return gcManager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() {return false;};

  virtual void StartGC() {};

  virtual void StopGC() {};

  // recycle old version
  virtual void RecycleOldTupleSlot(const oid_t UNUSED_ATTRIBUTE &table_id,
                                   const oid_t UNUSED_ATTRIBUTE &tile_group_id,
                                   const oid_t UNUSED_ATTRIBUTE &tuple_id,
                                   const cid_t UNUSED_ATTRIBUTE &tuple_end_cid) {}

  // recycle invalid version
  virtual void RecycleInvalidTupleSlot(const oid_t UNUSED_ATTRIBUTE &table_id,
                                       const oid_t UNUSED_ATTRIBUTE &tile_group_id,
                                       const oid_t UNUSED_ATTRIBUTE &tuple_id) {}

  virtual ItemPointer ReturnFreeSlot(const oid_t UNUSED_ATTRIBUTE &table_id) {
    return INVALID_ITEMPOINTER;
  }

  virtual void RegisterTable(oid_t table_id UNUSED_ATTRIBUTE ){}
};

}  // namespace gc
}  // namespace peloton
