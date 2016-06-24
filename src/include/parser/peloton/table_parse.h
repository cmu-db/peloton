//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_parse.h
//
// Identification: src/include/parser/table_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/peloton/abstract_parse.h"

#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

class TableParse : public AbstractParse {
 public:
  TableParse() = delete;
  TableParse(const DropParse &) = delete;
  TableParse &operator=(const TableParse &) = delete;
  TableParse(DropParse &&) = delete;
  TableParse &operator=(TableParse &&) = delete;

  explicit TableParse(RangeVar *table_node) {
    entity_type_ = ENTITY_TYPE_TABLE;

    entity_name_ = std::string(table_node->relname);

    LOG_INFO("Transform table join_node: %s", entity_name_.c_str());
  }

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_DROP; }

  const std::string GetInfo() const { return "DropParse"; }

 private:
  // Type of entity
  EntityType entity_type_ = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name_;

  // TODO: Maybe we want to communicate with catalog and identify DataTable
  // here? I'm not sure about that...
  // storage::DataTable *data_table_;
};

}  // namespace parser
}  // namespace peloton
