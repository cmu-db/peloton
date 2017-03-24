//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager_proxy.h
//
// Identification: src/include/codegen/manager_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A proxy to some of the methods in catalog::Catalog
//===----------------------------------------------------------------------===//
class CatalogProxy {
 public:
  // Return the LLVM type that matches the memory layout of our Manager class
  static llvm::Type *GetType(CodeGen &codegen);

  //===--------------------------------------------------------------------===//
  // A structure that proxies Catalog::GetTableWithOid()
  //===--------------------------------------------------------------------===//
  struct _GetTableWithOid {
    // Return the symbol for the Manager.GetTableWithOid() function
    static const std::string &GetFunctionName();
    // Return the LLVM-typed function definition for Manager.GetTableWithOid()
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton