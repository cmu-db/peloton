//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// schema_transformer.h
//
// Identification: src/backend/bridge/ddl/schema_transformer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/schema.h"

#include "c.h"
#include "access/tupdesc.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Schema Transformer
//===--------------------------------------------------------------------===//

class SchemaTransformer {
 public:
  SchemaTransformer(const SchemaTransformer &) = delete;
  SchemaTransformer &operator=(const SchemaTransformer &) = delete;
  SchemaTransformer(SchemaTransformer &&) = delete;
  SchemaTransformer &operator=(SchemaTransformer &&) = delete;

  static catalog::Schema *GetSchemaFromTupleDesc(TupleDesc tupleDesc);
};

}  // End bridge namespace
}  // End peloton namespace
