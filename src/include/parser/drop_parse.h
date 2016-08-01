//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_parse.h
//
// Identification: src/include/parser/drop_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"

#include "common/logger.h"

#include "parser/abstract_parser.h"

namespace peloton {
namespace parser {
  class DropStatement;

class DropParse : public AbstractParse {
 public:
  DropParse() = delete;
  DropParse(const DropParse &) = delete;
  DropParse &operator=(const DropParse &) = delete;
  DropParse(DropParse &&) = delete;
  DropParse &operator=(DropParse &&) = delete;

  explicit DropParse(DropStatement *drop_node);

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_DROP; }

  const std::string GetEntityName() { return entity_name; }

  const std::string GetInfo() const { return "DropParse"; }

  std::string GetTableName() { return entity_name; }

  bool IsMissing() { return missing; }

 private:
  // Type of entity
  EntityType entity_type = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name;

  bool missing;
};

}  // namespace parser
}  // namespace peloton
