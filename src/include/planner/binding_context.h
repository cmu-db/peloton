//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binding_context.h
//
// Identification: src/include/planner/binding_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "planner/attribute_info.h"

namespace peloton {
namespace planner {

class BindingContext {
 public:
  const AttributeInfo *Find(oid_t col_id) const {
    auto iter = mapping_.find(col_id);
    return (iter == mapping_.cend()) ? nullptr : iter->second;
  }

  bool Bind(oid_t col_id, const AttributeInfo *attribute_info) {
    mapping_[col_id] = attribute_info;
    return true;
  }

  bool BindNew(oid_t col_id, const AttributeInfo *attribute_info) {
    if (Find(col_id) != nullptr) {
      return false;
    } else {
      return Bind(col_id, attribute_info);
    }
  }

  void Rebind(oid_t old_col_id, oid_t new_col_id) {
    const auto *ai = Find(old_col_id);
    BindNew(new_col_id, ai);
    RemoveBinding(old_col_id);
  }

  void RemoveBinding(oid_t col_id) { mapping_.erase(col_id); }

 private:
  // The current mapping of column IDs to information about the column
  std::unordered_map<oid_t, const AttributeInfo *> mapping_;
};

}  // namespace planner
}  // namespace peloton