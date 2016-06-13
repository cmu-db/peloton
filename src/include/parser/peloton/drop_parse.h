//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_parse.h
//
// Identification: src/include/parser/insert_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/peloton/abstract_parse.h"

#include "common/types.h"

namespace peloton {
namespace parser {

class DropParse : public AbstractParse {
 public:
  DropParse() = delete;
  DropParse(const DropParse &) = delete;
  DropParse &operator=(const DropParse &) = delete;
  DropParse(DropParse &&) = delete;
  DropParse &operator=(DropParse &&) = delete;

  explicit DropParse(DropStmt *drop_node) {
    entity_type = ENTITY_TYPE_TABLE;

    ListCell *object_item;
    List *object_list = drop_node->objects;
    ListCell *subobject_item;
    List *subobject_list;

    // Value
    foreach(object_item, object_list){
      subobject_list = lfirst(object_item);

      foreach(subobject_item, subobject_list){
        ::Value *value = (::Value *) lfirst(subobject_item);
        LOG_INFO("Table : %s ", strVal(value));
        entity_name = std::string(strVal(value));
      }
    }

  }

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_DROP; }

  const std::string GetInfo() const { return "DropParse"; }

 private:

  // Type of entity
  EntityType entity_type = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name;
};

}  // namespace parser
}  // namespace peloton
