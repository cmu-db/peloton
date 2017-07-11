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

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// This class the main entry point for any code generation that requires
// operating on tuples. It only maintains the serialized form of the tuple
// data, and it can be considered to extend to hold the whole tuple
//===----------------------------------------------------------------------===//
class Tuple {
 public:
  // Constructor
  Tuple(storage::DataTable &table);

  // Generate code to build into the tuple data area
  void GenerateTupleData(CodeGen &codegen, RowBatch::Row &row,
                         const std::vector<const planner::AttributeInfo *> &ais,
                         llvm::Value *tuple_data, llvm::Value *pool) const;

 private:
  // The table associated with this generator
  storage::DataTable &table_;
};

}  // namespace codegen
}  // namespace peloton
