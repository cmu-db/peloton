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
    
    // Get Table name
    RangeVar* rv = create_node->relation;
    entity_name = std::string(rv->relname);
    
    // Get Table Elements from Parse node
    List *object_list = create_node->tableElts;
    ListCell *object_item;

    foreach(object_item, object_list){
      
      LOG_INFO("Got Something from tableElts");
      ::Value *value = (::Value *)lfirst(object_item);

      LOG_INFO("Column Name: %s ", strVal(value));
      std::string column_name = std::string(strVal(value));

      // Cast to ColumnDef to access the Column type IDs
      ColumnDef *element = (ColumnDef *)lfirst(object_item);

      // Get the Name
      char* type_name =  strVal(linitial(element->typeName->names));

      // Flag to discard using type "pg_catalog"
      if(strcmp(type_name, "pg_catalog") == 0){
        LOG_INFO("Yes it was pg_catalog");

        type_name = strVal(llast(element->typeName->names));
      }

      LOG_INFO("Column type is: %s" , type_name);

      //TODO: Convert to ValueType

      }
    

    
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
