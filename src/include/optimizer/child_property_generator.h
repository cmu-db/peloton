//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.h
//
// Identification: src/include/optimizer/child_property_generator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/query_node_visitor.h"

namespace peloton {
namespace optimizer {

class ChildPropertyGenerator : public QueryNodeVisitor {
 public:
  ChildPropertyGenerator(ColumnManager &manager) : manager(manager) {}

  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> GetProperties(
      parser::SQLStatement *op) {
    op->Accept(this);

    return std::move(
        std::vector<std::pair<PropertySet, std::vector<PropertySet>>>());
  }

 private:
  ColumnManager &manager;
};

} /* namespace optimizer */
} /* namespace peloton */
