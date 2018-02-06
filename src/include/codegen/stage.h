//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stage.h
//
// Identification: src/include/codegen/stage.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

enum class StageKind {
  SINGLE_THREADED,
};

struct CodeGenStage {
  StageKind kind_;
  llvm::Function *llvm_func_;
};

struct Stage {
  StageKind kind_;
  void (*func_ptr_)(char *);
};

}  // namespace codegen
}  // namespace peloton
