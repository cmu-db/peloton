//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.h
//
// Identification: src/include/codegen/tuple.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/row_batch.h"
#include "codegen/tile_group.h"
#include "type/abstract_pool.h"

namespace peloton {

class ItemPointer;

namespace catalog {
class Schema;
}  // namespace storage

namespace type {
class AbstractPool;
}  // namespace type

namespace codegen {

//===----------------------------------------------------------------------===//
// This class the main entry point for any code generation that requires
// operating on tuples. It generates the serialized form of the tuple data
//===----------------------------------------------------------------------===//
class Tuple {
 public:
  // Constructor
  Tuple(catalog::Schema &schema);

  void Generate(CodeGen &codegen, RowBatch::Row &row,
      const std::vector<const planner::AttributeInfo *> &ais,
      llvm::Value *tuple_storage, llvm::Value *pool) const;

 private:
  // The table associated with this generator
  catalog::Schema &schema_;
};

}  // namespace codegen
}  // namespace peloton
