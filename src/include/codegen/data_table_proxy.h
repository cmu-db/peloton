//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.h
//
// Identification: src/include/codegen/data_table_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A utility class that serves as a helper to proxy the precompiled DataTable
// class. It significantly eases the pain in invoking DataTable methods from
// LLVM code.
//===----------------------------------------------------------------------===//
class DataTableProxy {
 public:
  // Return the LLVM type that matches the memory layout of our Table class
  static llvm::Type *GetType(CodeGen &codegen);

  //===--------------------------------------------------------------------===//
  // The proxy for DataTable::GetTileGroupCount()
  //===--------------------------------------------------------------------===//
  struct _GetTileGroupCount {
    // Return the symbol for the DataTable.GetTileGroupCount() function
    static const std::string &GetFunctionName();
    // Return the LLVM-typed function definition for
    // DataTable.GetTileGroupCount()
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
