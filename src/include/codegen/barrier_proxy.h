#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A utility class that serves as a helper to proxy most of the methods in
// peloton::codegen::MultiThreadContext. It significantly reduces the pain in
// calling methods on MultiThreadContext instances from LLVM code.
//===----------------------------------------------------------------------===//
class BarrierProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen);
  static llvm::Function *GetInitInstanceFunction(CodeGen &codegen);
  static llvm::Function *GetMasterWaitFunction(CodeGen &codegen);
};

}
}
