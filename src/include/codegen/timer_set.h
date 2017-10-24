//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timer_set.h
//
// Identification: src/codegen/timer_set.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/timer_set_proxy.h"

// Accessor class to TimerSet
namespace peloton {
namespace codegen {

class TimerSet {
 public:
  void Init(CodeGen &codegen, llvm::Value *timer_set_ptr) const {
    codegen.Call(TimerSetProxy::Init, {timer_set_ptr});
  }

  void Destroy(CodeGen &codegen, llvm::Value *timer_set_ptr) const {
    codegen.Call(TimerSetProxy::Destroy, {timer_set_ptr});
  }

  void StartTimer(int timer_id, CodeGen &codegen,
                  llvm::Value *timer_set_ptr) const {
    codegen.Call(TimerSetProxy::Start,
                 {timer_set_ptr, codegen.Const32(timer_id)});
  }

  void StopTimer(int timer_id, CodeGen &codegen,
                 llvm::Value *timer_set_ptr) const {
    codegen.Call(TimerSetProxy::Stop,
                 {timer_set_ptr, codegen.Const32(timer_id)});
  }

  llvm::Value *GetDuration(int timer_id, CodeGen &codegen,
                           llvm::Value *timer_set_ptr) const {
    return codegen.Call(TimerSetProxy::GetDuration,
                        {timer_set_ptr, codegen.Const32(timer_id)});
  }

  void PrintDuration(int timer_id, const std::string &name, CodeGen &codegen,
                     llvm::Value *timer_set_ptr) const {
    llvm::Value *duration = codegen.Call(
        TimerSetProxy::GetDuration, {timer_set_ptr, codegen.Const32(timer_id)});
    codegen.CallPrintf(name + ": %0.0f ms\n", {duration});
  }
};

}  // namespace codegen
}  // namespace peloton