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
  AbstractCatalogObject(std::string name, oid_t oid) : name_(name), oid_(oid) {};

public:
  inline oid_t GetOid() const { return oid_; }

  const std::string &GetName() const { return name_; }

protected:
  std::string name_;
  oid_t oid_;
};
}
}