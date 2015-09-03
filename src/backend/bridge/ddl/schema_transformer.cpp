//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// schema_transformer.cpp
//
// Identification: src/backend/bridge/ddl/schema_transformer.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/bridge/ddl/format_transformer.h"

#include <vector>
#include <iostream>

#include "backend/catalog/constraint.h"
#include "backend/catalog/column.h"
#include "backend/common/types.h"
#include "backend/common/logger.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Schema Transformer
//===--------------------------------------------------------------------===//

catalog::Schema *SchemaTransformer::GetSchemaFromTupleDesc(
    TupleDesc tupleDesc) {
  catalog::Schema *schema = nullptr;

  std::vector<catalog::Column> columns;
  int natts = tupleDesc->natts;

  // construct column
  for (int column_itr = 0; column_itr < natts; column_itr++) {
    std::vector<catalog::Constraint> constraint_infos;

    PostgresValueFormat postgresValueFormat( tupleDesc->attrs[column_itr]->atttypid, 
                                             tupleDesc->attrs[column_itr]->attlen,
                                             tupleDesc->attrs[column_itr]->atttypmod);

    PelotonValueFormat pelotonValueFormat = FormatTransformer::TransformValueFormat(postgresValueFormat);

    ValueType value_type = pelotonValueFormat.GetType();
    int column_length = pelotonValueFormat.GetLength();
    bool is_inlined = pelotonValueFormat.IsInlined();

    // Skip invalid attributes (e.g., ctid)
    if(VALUE_TYPE_INVALID == value_type){
      continue;
    }

    // NOT NULL constraint
    if (tupleDesc->attrs[column_itr]->attnotnull) {
      std::string constraint_name = "not_null";
      catalog::Constraint constraint(CONSTRAINT_TYPE_NOTNULL, constraint_name);
      constraint_infos.push_back(constraint);
    }

    // DEFAULT value constraint
    if (tupleDesc->attrs[column_itr]->atthasdef) {
      std::string constraint_name = "default";
      catalog::Constraint constraint(CONSTRAINT_TYPE_DEFAULT, constraint_name);
      constraint_infos.push_back(constraint);
    }

    LOG_TRACE("Column length: %d/%d, is inlined: %d", tupleDesc->attrs[column_itr]->attlen, column_length, is_inlined);
    catalog::Column column(value_type, column_length,
                           std::string(NameStr(tupleDesc->attrs[column_itr]->attname), NAMEDATALEN),
                           is_inlined);

    for (auto constraint : constraint_infos)
      column.AddConstraint(constraint);

    columns.push_back(column);
  }

  schema = new catalog::Schema(columns);

  return schema;
}

}  // End bridge namespace
}  // End peloton namespace
