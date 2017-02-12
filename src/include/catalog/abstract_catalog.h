//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.h
//
// Identification: src/include/catalog/abstract_catalog.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>

#include "common/item_pointer.h"
#include "common/printable.h"
#include "type/types.h"
#include "type/catalog_type.h"

namespace peloton {

namespace catalog {

class AbstractCatalogObject : public Printable {
protected:
  AbstractCatalogObject(oid_t oid) : oid_(oid) {};

public:
  inline oid_t GetOid() const { return oid_; }

private:
  oid_t oid_;
};
}
}