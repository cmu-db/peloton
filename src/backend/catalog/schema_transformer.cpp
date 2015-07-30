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

#include <vector>
#include <iostream>

#include "schema_transformer.h"
#include "backend/common/types.h"
#include "backend/catalog/constraint.h"
#include "backend/catalog/column.h"

namespace peloton {
namespace catalog {

//===--------------------------------------------------------------------===//
// Schema Transformer
//===--------------------------------------------------------------------===//


Schema* SchemaTransformer::GetSchemaFromTupleDesc(TupleDesc tupleDesc){
  Schema* schema = nullptr;

  std::vector<Column> columns;
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
      Constraint constraint(CONSTRAINT_TYPE_NOTNULL);
      constraint_infos.push_back(constraint);
    }

    // DEFAULT value constraint
    if (tupleDesc->attrs[column_itr]->atthasdef) {
      Constraint constraint(CONSTRAINT_TYPE_DEFAULT);
      constraint_infos.push_back(constraint);
    }

    Column column(value_type, column_length, NameStr(tupleDesc->attrs[column_itr]->attname), 
        is_inlined);
    columns.push_back(column);
  }

  schema = new Schema(columns);

  std::cout << *schema << std::endl;

  return schema;
}

}  // End catalog namespace
}  // End peloton namespace

