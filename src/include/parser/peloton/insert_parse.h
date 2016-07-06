//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_parse.h
//
// Identification: src/include/parser/peloton/insert_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "abstract_parse.h"

namespace peloton {
namespace parser {

class InsertParse : public AbstractParse {
 public:
  InsertParse() = delete;
  InsertParse(const InsertParse &) = delete;
  InsertParse &operator=(const InsertParse &) = delete;
  InsertParse(InsertParse &&) = delete;
  InsertParse &operator=(InsertParse &&) = delete;

  explicit InsertParse(InsertStmt *insert_node);

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_INSERT; }

  const std::string GetInfo() const { return "InsertParse"; }

  std::string GetTableName() { return entity_name; }

 private:

 // Table Name
 std::string entity_name;

 // if columns are specified in insert
 std::vector<std::string> columns;

 std::vector<Value> values;

};

}  // namespace parser
}  // namespace peloton
