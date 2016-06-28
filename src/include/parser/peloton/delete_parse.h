
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// delete_parse.h
//
// Identification: /peloton/src/include/parser/peloton/delete_parse.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/peloton/abstract_parse.h"

namespace peloton {
namespace parser {

class DeleteParse : public AbstractParse {
 public:
	DeleteParse() = delete;
	DeleteParse(const DeleteParse &) = delete;
	DeleteParse &operator=(const DeleteParse &) = delete;
	DeleteParse(DeleteParse &&) = delete;
	DeleteParse &operator=(DeleteParse &&) = delete;

  explicit DeleteParse(DeleteStmt *delete_node);

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_DELETE; }

  const std::string GetEntityName() { return entity_name; }

  const std::string GetInfo() const { return "DeleteParse"; }

  std::string GetTableName() { return entity_name; }


 private:
  // Type of entity
  EntityType entity_type = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name;

};

}  // namespace parser
}  // namespace peloton
