//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_parse.h
//
// Identification: src/include/parser/peloton/table_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "parser/abstract_parse.h"

#include "type/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

class TableParse : public AbstractParse {
 public:
  TableParse() = delete;
  TableParse(const TableParse &) = delete;
  TableParse &operator=(const TableParse &) = delete;
  TableParse(TableParse &&) = delete;
  TableParse &operator=(TableParse &&) = delete;

  /*
  explicit TableParse(RangeVar *table_node) {
    entity_type_ = EntityType::TABLE;

    entity_name_ = std::string(table_node->relname);

    LOG_INFO("Transform table join_node: %s", entity_name_.c_str());
  }
  */

  inline ParseNodeType GetParseNodeType() const { return ParseNodeType::DROP; }

  const std::string GetInfo() const { return "DropParse"; }

 private:
  // Type of entity
  EntityType entity_type_ = EntityTypeId::INVALID;

  // Name of entity
  std::string entity_name_;

  // TODO: Maybe we want to communicate with catalog and identify DataTable
  // here? I'm not sure about that...
  // storage::DataTable *data_table_;
};

}  // namespace parser
}  // namespace peloton
