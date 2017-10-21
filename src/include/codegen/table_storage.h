//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_storage.h
//
// Identification: src/include/codegen/table_storage.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {

namespace catalog {
class Schema;
}  // namespace catalog

namespace codegen {
class Value;
//===----------------------------------------------------------------------===//
// This class the main entry point for any code generation that requires
// operating on tuples. It generates the serialized form of the tuple data
//===----------------------------------------------------------------------===//
class TableStorage {
 public:
  // Constructor
  TableStorage(catalog::Schema &schema): schema_(schema) {}

  void StoreValues(CodeGen &codegen, llvm::Value *tuple_ptr,
      const std::vector<codegen::Value> &values, llvm::Value *pool) const;

 private:
  // The table associated with this generator
  catalog::Schema &schema_;
};

}  // namespace codegen
}  // namespace peloton
