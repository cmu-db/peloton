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

#include "common/types.h"

#include "common/logger.h"

#include "common/value.h"

#include "catalog/column.h"
#include "catalog/schema.h"
#include "parser/abstract_parse.h"

namespace peloton {
namespace parser {
  class CreateStatement;

class CreateParse : public AbstractParse {
 public:
  CreateParse() = delete;
  CreateParse(const CreateParse &) = delete;
  CreateParse &operator=(const CreateParse &) = delete;
  CreateParse(CreateParse &&) = delete;
  CreateParse &operator=(CreateParse &&) = delete;

  explicit CreateParse(CreateStatement *create_node);
      inline ParseNodeType GetParseNodeType() const {
        return PARSE_NODE_TYPE_CREATE;
      }

      const std::string GetInfo() const {
        return "CreateParse";
      }

      std::string GetTableName() {
        return entity_name;
      }

      std::vector<catalog::Column> GetColumns() {
        return columns_name;
      }

      catalog::Schema* GetSchema() {
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
    };

  } // namespace parser
  } // namespace peloton
