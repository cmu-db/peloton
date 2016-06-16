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

#include "catalog/column.h"

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

    RangeVar* relations = create_node->relation;

    schema = std::string(relations->schemaname);
    database_name = std::string(relations->catalogname);

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
        catalog::Column col = new catalog::Column(VALUE_TYPE_INTEGER, INVALID_OID,
                                                  std::string(strVal(value)),
                                                  false, INVALID_OID);
        columns_name.push_back(col);
      }
    }

  }

  inline ParseNodeType GetParseNodeType() const {
    return PARSE_NODE_TYPE_CREATE;
  }

  const std::string GetInfo() const {
    return "CreateParse";
  }

  std::string GetTableName() {
    return entity_name;
  }

  std::vector<catalog::Column> GetColumns(){
    return columns_name;
  }

  catalog::Schema* GetSchema(){
    catalog::Schema *schema = new catalog::Schema(columns_name);
    return schema;
  }

 private:

  // Entity Type
  EntityType entity_type;

  // Name of entity
  std::string entity_name;

  // Table Columns (Column type?)
  std::vector<catalog::Column> columns_name;

  // Schema Name
  std::string schema;

  // catalog name
  std::string database_name;

  // Table file path. (Since All data in memory , need path?)
  std::string filePath;
};

}  // namespace parser
}  // namespace peloton
