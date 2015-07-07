/*-------------------------------------------------------------------------
 *
 * manager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/manager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/manager.h"

namespace peloton {
namespace catalog {


Manager& Manager::GetInstance() {
    static Manager manager;
    return manager;
}

void Manager::SetLocation(const oid_t oid, void *location) {
    locator.insert(std::pair<oid_t, void*>(oid, location));
}

void Manager::SetLocation(const oid_t db_oid, const oid_t table_oid, void *location) {
    std::pair<oid_t, oid_t> db_and_table_oid_pair; 
    db_and_table_oid_pair = std::make_pair(db_oid, table_oid);

    global_locator.insert(std::pair<std::pair<oid_t, oid_t>, void*>(db_and_table_oid_pair, location));
}

void *Manager::GetLocation(const oid_t oid) const {
    void *location = nullptr;
    try {
        location = catalog::Manager::GetInstance().locator.at(oid);
    }
    catch(std::exception& e) {
        // FIXME
    }
    return location;
}

void *Manager::GetLocation(const oid_t database_oid, const oid_t table_oid) const {
    void *location = nullptr;
    try {
        location = catalog::Manager::GetInstance().global_locator.at(std::make_pair(database_oid, table_oid));
    }
    catch(std::exception& e) {
        // FIXME
    }
    return location;
}

} // End catalog namespace
} // End peloton namespace



