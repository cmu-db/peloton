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

#include "catalog/manager.h"

namespace nstore {
namespace catalog {


Manager& Manager::GetInstance() {
  static Manager manager;
  return manager;
}

void Manager::SetLocation(const oid_t oid, void *location) {
  locator.insert(std::pair<oid_t, void*>(oid, location));
}

} // End catalog namespace
} // End nstore namespace



