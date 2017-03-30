//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog_object.h
//
// Identification: src/include/catalog/abstract_catalog_object.h
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
#include "type/catalog_type.h"
#include "type/types.h"

namespace peloton {

namespace catalog {

class AbstractCatalogObject : public Printable {
 public:
  AbstractCatalogObject(const AbstractCatalogObject &) = delete;
  AbstractCatalogObject &operator=(const AbstractCatalogObject &) = delete;
  AbstractCatalogObject(AbstractCatalogObject &&) = delete;
  AbstractCatalogObject &operator=(AbstractCatalogObject &&) = delete;

  AbstractCatalogObject(std::string name, oid_t oid) : name_(name), oid_(oid) {};

  virtual ~AbstractCatalogObject() {}

 public:
  inline oid_t GetOid() const { return oid_; }

  const std::string &GetName() const { return name_; }

 protected:
  std::string name_;
  oid_t oid_;
};
}
}