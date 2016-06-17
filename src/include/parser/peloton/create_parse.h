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

#include "catalog/schema.h"

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
	RangeVar* rv = create_node->relation;
	entity_name = std::string(rv->relname);

        auto col = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),"dept_id", true);
        
	columns_name.push_back(col);
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
  EntityType entity_type = ENTITY_TYPE_INVALID;

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
