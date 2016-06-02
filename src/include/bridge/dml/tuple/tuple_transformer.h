//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_transformer.h
//
// Identification: src/bridge/dml/tuple/tuple_transformer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "postgres.h"
#include "executor/tuptable.h"

#include "common/value.h"
#include "common/value_factory.h"
#include "storage/data_table.h"
#include "common/abstract_tuple.h"

namespace peloton {

class VarlenPool;

namespace bridge {

//===--------------------------------------------------------------------===//
// Tuple Transformer
//===--------------------------------------------------------------------===//

class TupleTransformer {
 public:
  TupleTransformer(const TupleTransformer &) = delete;
  TupleTransformer &operator=(const TupleTransformer &) = delete;
  TupleTransformer(TupleTransformer &&) = delete;
  TupleTransformer &operator=(TupleTransformer &&) = delete;

  TupleTransformer(){};

  static Value GetValue(Datum datum, Oid atttypid);

  static Datum GetDatum(peloton::Value value);

  static storage::Tuple *GetPelotonTuple(TupleTableSlot *slot,
                                         const catalog::Schema *schema,
                                         VarlenPool *pool);

  static TupleTableSlot *GetPostgresTuple(AbstractTuple *tuple,
                                          TupleDesc tuple_desc);
};

}  // namespace bridge
}  // namespace peloton
