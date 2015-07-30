/*-------------------------------------------------------------------------
 *
 * schema_transformaer.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/tuple_schema.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/ddl/schema_transformer.h"

#include <vector>
#include <iostream>

#include "backend/common/types.h"
#include "backend/catalog/constraint.h"
#include "backend/catalog/column.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Schema Transformer
//===--------------------------------------------------------------------===//


catalog::Schema* SchemaTransformer::GetSchemaFromTupleDesc(TupleDesc tupleDesc){
  catalog::Schema* schema = nullptr;

  std::vector<catalog::Column> columns;
  int natts = tupleDesc->natts;

  // construct column
  for(int column_itr=0; column_itr<natts; column_itr++){
    std::vector<catalog::Constraint> constraint_infos;

    // value
    PostgresValueType postgresValueType = (PostgresValueType)tupleDesc->attrs[column_itr]->atttypid;
    ValueType value_type = PostgresValueTypeToPelotonValueType(postgresValueType);

    // length
    int column_length = tupleDesc->attrs[column_itr]->attlen;

    // inlined
    bool is_inlined = true;
    if (tupleDesc->attrs[column_itr]->attlen == -1) {
      column_length = tupleDesc->attrs[column_itr]->atttypmod;
      is_inlined = false;
    }

    // NOT NULL constraint
    if (tupleDesc->attrs[column_itr]->attnotnull) {
      catalog::Constraint constraint(CONSTRAINT_TYPE_NOTNULL);
      constraint_infos.push_back(constraint);
    }

    // DEFAULT value constraint
    if (tupleDesc->attrs[column_itr]->atthasdef) {
      catalog::Constraint constraint(CONSTRAINT_TYPE_DEFAULT);
      constraint_infos.push_back(constraint);
    }

    catalog::Column column(value_type, column_length, NameStr(tupleDesc->attrs[column_itr]->attname),
        is_inlined);
    columns.push_back(column);
  }

  schema = new catalog::Schema(columns);

  std::cout << *schema << std::endl;

  return schema;
}

}  // End bridge namespace
}  // End peloton namespace

