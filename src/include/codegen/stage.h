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

#include "storage/data_table.h"
#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

enum class StageKind { SINGLETHREADED, MULTITHREADED_SEQSCAN };

struct CodeGenStage {
  StageKind kind_;
  union {
    struct {
      llvm::Function *singlethreaded_func_;
    };
    struct {
      llvm::Function *multithreaded_seqscan_init_;
      llvm::Function *multithreaded_seqscan_func_;
      const storage::DataTable *table_;
    };
  };
};

struct Stage {
  StageKind kind_;
  union {
    struct {
      void (*singlethreaded_func_)(char *);
    };
    struct {
      void (*multithreaded_seqscan_init_)(char *, size_t);
      void (*multithreaded_seqscan_func_)(char *, size_t, size_t, size_t);
      const storage::DataTable *table_;
    };
  };
};

inline CodeGenStage SingleThreadedCodeGenStage(llvm::Function *func) {
  CodeGenStage stage;
  stage.kind_ = StageKind::SINGLETHREADED;
  stage.singlethreaded_func_ = func;
  return stage;
}

inline CodeGenStage MultiThreadedSeqScanCodeGen(
    llvm::Function *init, llvm::Function *func,
    const storage::DataTable *table) {
  CodeGenStage stage;
  stage.kind_ = StageKind::MULTITHREADED_SEQSCAN;
  stage.multithreaded_seqscan_init_ = init;
  stage.multithreaded_seqscan_func_ = func;
  stage.table_ = table;
  return stage;
}

}  // namespace codegen
}  // namespace peloton
