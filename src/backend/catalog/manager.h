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
#include "backend/common/types.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

typedef tbb::concurrent_unordered_map<oid_t, void*> lookup_dir;

class Manager {

public:

    // Singleton
    static Manager& GetInstance();

    oid_t GetNextOid() {
        return ++oid;
    }

    oid_t GetOid() {
        return oid;
    }

    void SetLocation(const oid_t oid, void *location);

    void *GetLocation(const oid_t oid) const;

    Manager() {}

    Manager(Manager const&) = delete;

    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//

    std::atomic<oid_t> oid = ATOMIC_VAR_INIT(START_OID);

    lookup_dir locator;
};

} // End catalog namespace
} // End nstore namespace
