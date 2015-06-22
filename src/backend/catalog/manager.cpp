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

namespace nstore {
namespace catalog {


Manager& Manager::GetInstance() {
    static Manager manager;
    return manager;
}

void Manager::SetLocation(const oid_t oid, void *location) {
    locator.insert(std::pair<oid_t, void*>(oid, location));
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

} // End catalog namespace
} // End nstore namespace



