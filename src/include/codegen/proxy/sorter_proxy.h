//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.h
//
// Identification: src/include/codegen/sorter_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class SorterProxy {
 public:
  // Get the LLVM type for peloton::codegen::util::Sorter
  static llvm::Type *GetType(CodeGen &codegen);

  //===--------------------------------------------------------------------===//
  // The proxy for codegen::util::Sorter::Init()
  //===--------------------------------------------------------------------===//
  struct _Init {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for codegen::util::Sorter::StoreInputTuple()
  //===--------------------------------------------------------------------===//
  struct _StoreInputTuple {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for codegen::util::Sorter::Sort()
  //===--------------------------------------------------------------------===//
  struct _Sort {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // The proxy for codegen::util::Sorter::Destroy()
  //===--------------------------------------------------------------------===//
  struct _Destroy {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton