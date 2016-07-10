
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// update_parse.h
//
// Identification: /peloton/src/include/parser/peloton/update_parse.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/abstract_parse.h"

namespace peloton {
namespace parser {
  class UpdateStatement;

class UpdateParse : public AbstractParse {
 public:
	UpdateParse() = delete;
	UpdateParse(const UpdateParse &) = delete;
	UpdateParse &operator=(const UpdateParse &) = delete;
	UpdateParse(UpdateParse &&) = delete;
	UpdateParse &operator=(UpdateParse &&) = delete;

  explicit UpdateParse(UpdateStatement *update_node);

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_UPDATE; }

  const std::string GetEntityName() { return entity_name; }

  const std::string GetInfo() const { return "UpdateParse"; }

  std::string GetTableName() { return entity_name; }


 private:
  // Type of entity
  EntityType entity_type = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name;

};

}  // namespace parser
}  // namespace peloton
