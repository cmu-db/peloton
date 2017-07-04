//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table_proxy.h
//
// Identification: src/include/codegen/oa_hash_table_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A utility class that serves as a helper to proxy most of the methods in
// peloton::codegen::util::OAHashTable. It significantly reduces the pain in
// calling methods on OAHashTable instances from LLVM code.
//===----------------------------------------------------------------------===//
class OAHashTableProxy {
 public:
  // Return the LLVM type that matches the memory layout of our HashTable
  static llvm::Type *GetType(CodeGen &codegen);

  //===--------------------------------------------------------------------===//
  // The proxy for HashTable::Init()
  //===--------------------------------------------------------------------===//
  struct _Init {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for HashTable::Destroy()
  //===--------------------------------------------------------------------===//
  struct _Destroy {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for HashTable::StoreTuple()
  //===--------------------------------------------------------------------===//
  struct _StoreTuple {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

//===----------------------------------------------------------------------===//
// A utility class that serves as a helper to proxy the precompiled
// HashTable::HashEntry methods and types.
//===----------------------------------------------------------------------===//
class OAHashEntryProxy {
 public:
  // Return the LLVM type that matches the memory layout of our HashEntry
  static llvm::Type *GetType(CodeGen &codegen);

  // Return the type of kv list to hold values for duplicated keys
  static llvm::Type *GetKeyValueListType(CodeGen &codegen);
};

}  // namespace codegen
}  // namespace peloton