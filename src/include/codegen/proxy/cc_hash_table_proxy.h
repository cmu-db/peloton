//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table_proxy.h
//
// Identification: src/include/codegen/cc_hash_table_proxy.h
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
// peloton::util::CCHashTable. It significantly reduces the pain in calling
// methods on HashTable instances from LLVM code.
//===----------------------------------------------------------------------===//
class CCHashTableProxy {
 public:
  // Return the LLVM type that matches the memory layout of our HashTable
  static llvm::Type *GetType(CodeGen &codegen);

  //===--------------------------------------------------------------------===//
  // The proxy for CCHashTable::Init()
  //===--------------------------------------------------------------------===//
  struct _Init {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for CCHashTable::Destroy()
  //===--------------------------------------------------------------------===//
  struct _Destroy {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for CCHashTable::StoreTuple()
  //===--------------------------------------------------------------------===//
  struct _StoreTuple {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

//===----------------------------------------------------------------------===//
// A utility class that serves as a helper to proxy the precompiled
// CCHashTable::HashEntry methods and types.
//===----------------------------------------------------------------------===//
class HashEntryProxy {
 public:
  // Return the LLVM type that matches the memory layout of our HashEntry
  static llvm::Type *GetType(CodeGen &codegen);
};

}  // namespace codegen
}  // namespace peloton