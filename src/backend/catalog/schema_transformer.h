/*-------------------------------------------------------------------------
 *
 * schema_transformaer.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/tuple_schema.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/schema.h"

#include "c.h"
#include "access/tupdesc.h"

namespace peloton {
namespace catalog {

class Schema;

//===--------------------------------------------------------------------===//
// Schema Transformer
//===--------------------------------------------------------------------===//

class SchemaTransformer {
public:
  SchemaTransformer(const SchemaTransformer &) = delete;
  SchemaTransformer &operator=(const SchemaTransformer &) = delete;
  SchemaTransformer(SchemaTransformer &&) = delete;
  SchemaTransformer &operator=(SchemaTransformer &&) = delete;

  static Schema* GetSchemaFromTupleDesc(TupleDesc tupleDesc);
};

}  // End catalog namespace
}  // End peloton namespace

