//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// create_parse.h
//
// Identification: /peloton/src/include/parser/peloton/create_parse.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/peloton/abstract_parse.h"

#include "common/types.h"

#include "common/logger.h"

namespace peloton {
namespace parser {

class CreateParse : public AbstractParse {
 public:
  CreateParse() = delete;
  CreateParse(const CreateParse &) = delete;
  CreateParse &operator=(const CreateParse &) = delete;
  CreateParse(CreateParse &&) = delete;
  CreateParse &operator=(CreateParse &&) = delete;

  explicit CreateParse(CreateStmt *create_node) {
    entity_type = ENTITY_TYPE_TABLE;

    ListCell *object_item;
    List *object_list = create_node->tableElts;
    ListCell *subobject_item;
    List *subobject_list;

    // Value
    foreach(object_item, object_list)
    {
      subobject_list = lfirst(object_item);

      foreach(subobject_item, subobject_list)
      {
        ::Value *value = (::Value *) lfirst(subobject_item);
        LOG_INFO("Column : %s ", strVal(value));
        columns.push_back(std::string(strVal(value)));
      }
    }

  }

  inline ParseNodeType GetParseNodeType() const {
    return PARSE_NODE_TYPE_CREATE;
  }

  const std::string GetInfo() const {
    return "CreateParse";
  }

  std::string GetTableName() { return entity_name; }

 private:

  // Type of entity
  EntityType entity_type = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name;

  // Table Columns (Column type?)
  std::vector<std::string> columns;

  // Table file path. (Since All data in memory , need path?)
  std::string filePath;
};

}  // namespace parser
}  // namespace peloton
