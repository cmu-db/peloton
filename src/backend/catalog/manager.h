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
#include <utility>

#include "tbb/concurrent_unordered_map.h"
#include "backend/common/types.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

typedef tbb::concurrent_unordered_map<oid_t, void*> lookup_dir;
typedef tbb::concurrent_unordered_map<std::pair<oid_t, oid_t>, void*> global_lookup_dir; 


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

    // Store table location with two keys
    void SetLocation(const oid_t oid1, const oid_t oid2, void *location);


    void *GetLocation(const oid_t oid) const;

    // Look up the address with two keys
    void *GetLocation(const oid_t database_oid, const oid_t table_oid) const;


    Manager() {}

    Manager(Manager const&) = delete;

    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//

    std::atomic<oid_t> oid = ATOMIC_VAR_INIT(START_OID);

    lookup_dir locator;
    global_lookup_dir global_locator;

};

} // End catalog namespace
} // End peloton namespace
