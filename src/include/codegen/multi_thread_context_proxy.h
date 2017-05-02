//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_thread_context_proxy.h
//
// Identification: src/include/codegen/multi_thread_context_proxy.h
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
// peloton::codegen::MultiThreadContext. It significantly reduces the pain in
// calling methods on MultiThreadContext instances from LLVM code.
//===----------------------------------------------------------------------===//
class MultiThreadContextProxy {
 public:
  // Return the LLVM type that matches the memory layout of our HashTable
  static llvm::Type *GetType(CodeGen &codegen);

  static llvm::Function *InitInstanceFunction(CodeGen &codegen);
  static llvm::Function *GetRangeStartFunction(CodeGen &codegen);
  static llvm::Function *GetRangeEndFunction(CodeGen &codegen);
  static llvm::Function *GetThreadIdFunction(CodeGen &codegen);
  static llvm::Function *GetGetBarrierFunction(CodeGen &codegen);
};


}  // namespace codegen
}  // namespace peloton
